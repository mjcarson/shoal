//! A basic key/value database example

use shoal_core::server::Conf;
use shoal_core::storage::FileSystem;
use shoal_core::tables::{EphemeralTable, PersistentTable};
use shoal_core::{rkyv, FromShoal};

use shoal_core::server::ring::Ring;
use shoal_core::shared::queries::{Get, Queries, Query};
use shoal_core::shared::responses::Response;
use shoal_core::shared::traits::{
    Archivable, ShoalDatabase, ShoalQuery, ShoalResponse, ShoalTable,
};

use glommio::LocalExecutorBuilder;
use gxhash::GxHasher;
use rkyv::{Archive, Deserialize, Serialize};
use std::hash::Hasher;
use uuid::Uuid;

// A basic key/value table in Shoal
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
#[derive(shoal_derive::ShoalTable)]
#[shoal_table(db = "Basic")]
pub struct KeyValueRow {
    /// A partition key
    key: String,
    /// A value
    value: String,
}

impl KeyValueRow {
    /// Create a new key/value row
    fn new<T: Into<String>>(key: T, value: T) -> Self {
        KeyValueRow {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl From<KeyValueRow> for BasicQueryKinds {
    fn from(row: KeyValueRow) -> Self {
        // get our rows partition key
        let key = KeyValueRow::partition_key(&row.key);
        // build our query kind
        BasicQueryKinds::KeyValueQuery(Query::Insert { key, row })
    }
}

impl ShoalTable for KeyValueRow {
    /// The sort type for this data
    type Sort = String;

    /// Build the sort tuple for this row
    fn get_sort(&self) -> &Self::Sort {
        &self.key
    }

    /// Calculate the partition key for this row
    fn partition_key(sort: &Self::Sort) -> u64 {
        // create a new hasher
        let mut hasher = GxHasher::default();
        // hash the first key
        hasher.write(sort.as_bytes());
        // get our hash
        hasher.finish()
    }

    /// Any filters to apply when listing/crawling rows
    type Filters = String;

    /// Determine if a row should be filtered
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters to apply
    /// * `row` - The row to filter
    fn is_filtered(filter: &Self::Filters, row: &Self) -> bool {
        &row.value == filter
    }
}

impl Archivable for KeyValueRow {
    fn deserialize(archived: &<Self as Archive>::Archived) -> Self {
        // deserialize it
        archived.deserialize(&mut rkyv::Infallible).unwrap()
    }
}

#[derive(Debug, Archive, Serialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct KeyValueGet {
    /// The key to get rows with
    pub key: String,
    /// Any filters to apply to rows
    pub filters: Option<String>,
    /// The number of rows to get at most
    pub limit: Option<usize>,
}

impl KeyValueGet {
    /// Create a new get query for a [`KeyValueRow`]
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key string
    pub fn new<T: Into<String>>(key: T) -> Self {
        KeyValueGet {
            key: key.into(),
            filters: None,
            limit: None,
        }
    }

    /// Set a filter for getting rows
    ///
    /// # Arguments
    ///
    /// * `filter` - The value to filter on
    pub fn filter<T: Into<String>>(mut self, filter: T) -> Self {
        // set our filter
        self.filters = Some(filter.into());
        self
    }
}

impl From<KeyValueGet> for BasicQueryKinds {
    /// Build our a `QueryKind` for getting `KeyValueRows`
    fn from(specific: KeyValueGet) -> Self {
        // build our sort key
        let key = <KeyValueRow as ShoalTable>::Sort::from(specific.key);
        // build our partition key
        let partition_key = <KeyValueRow as ShoalTable>::partition_key(&key);
        // build the general query
        let general = Get {
            partition_keys: vec![partition_key],
            partitions: vec![key],
            filters: specific.filters,
            limit: specific.limit,
        };
        // build our query kind
        Self::KeyValueQuery(Query::Get(general))
    }
}

/// The different tables we can query
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
#[archive(check_bytes)]
pub enum BasicQueryKinds {
    /// A query for the key/value table
    KeyValueQuery(Query<KeyValueRow>),
}

impl ShoalQuery for BasicQueryKinds {
    /// Deserialize our response types
    ///
    /// # Arguments
    ///
    /// * `buff` - The buffer to deserialize into a response
    fn response_query_id(buff: &[u8]) -> &Uuid {
        // try to cast this query
        let kinds = shoal_core::rkyv::check_archived_root::<BasicResponseKinds>(buff).unwrap();
        // get our response query id
        match kinds {
            ArchivedBasicResponseKinds::KeyValueRow(resp) => &resp.id,
        }
    }

    /// find the shard for this query
    ///
    /// # Arguments
    ///
    /// * `ring` - The shard ring to check against
    /// * `found` - The shards we found for this query
    fn find_shard<'a>(
        &self,
        ring: &'a Ring,
        found: &mut Vec<&'a shoal_core::server::shard::ShardInfo>,
    ) {
        match &self {
            BasicQueryKinds::KeyValueQuery(query) => {
                // get our shards info
                query.find_shard(ring, found);
            }
        };
    }
}

/// The different tables we can get responses from
#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum BasicResponseKinds {
    KeyValueRow(Response<KeyValueRow>),
}

impl ShoalResponse for BasicResponseKinds {
    /// Get the index of a single [`Self::ResponseKinds`]
    fn get_index(&self) -> usize {
        // get our response index
        match self {
            BasicResponseKinds::KeyValueRow(resp) => resp.index,
        }
    }

    /// Get whether this is the last response in a response stream
    fn is_end_of_stream(&self) -> bool {
        // check if this is the end of the stream
        match self {
            BasicResponseKinds::KeyValueRow(resp) => resp.end,
        }
    }
}

/// The tables we are adding to to shoal
//#[shoal_db(name = "Basic")]
pub struct Basic {
    /// A basic key value table
    pub key_value: PersistentTable<KeyValueRow, FileSystem<KeyValueRow>>,
}

impl ShoalDatabase for Basic {
    /// The different tables or types of queries we will handle
    type QueryKinds = BasicQueryKinds;

    /// The different tables we can get responses from
    type ResponseKinds = BasicResponseKinds;

    /// Create a new shoal db instance
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - A shoal config
    async fn new(shard_name: &str, conf: &Conf) -> Self {
        Basic {
            key_value: PersistentTable::new(shard_name, conf).await,
        }
    }

    /// Deserialize our query types
    fn unarchive(buff: &[u8]) -> Queries<Self> {
        // try to cast this query
        let query = shoal_core::rkyv::check_archived_root::<Queries<Self>>(buff).unwrap();
        // deserialize it
        query.deserialize(&mut rkyv::Infallible).unwrap()
    }

    // Deserialize our response types
    fn unarchive_response(buff: &[u8]) -> Self::ResponseKinds {
        // try to cast this query
        let query = shoal_core::rkyv::check_archived_root::<Self::ResponseKinds>(buff).unwrap();
        // deserialize it
        query.deserialize(&mut rkyv::Infallible).unwrap()
    }

    /// Handle messages for different table types
    async fn handle(
        &mut self,
        id: Uuid,
        index: usize,
        typed_query: Self::QueryKinds,
        end: bool,
    ) -> Self::ResponseKinds {
        // match on the right query and execute it
        match typed_query {
            BasicQueryKinds::KeyValueQuery(query) => {
                // handle these queries
                BasicResponseKinds::KeyValueRow(self.key_value.handle(id, index, query, end).await)
            }
        }
    }
}

#[tokio::main]
async fn test_queries() {
    // build a client for Shoal
    let shoal = shoal_core::client::Shoal::<Basic>::new("0.0.0.0:0").await;
    // build a query
    let query = shoal
        .query()
        .add(KeyValueRow::new("hello", "world1"))
        .add(KeyValueRow::new("hello2", "world3"))
        .add(KeyValueRow::new("hello3", "nope"))
        .add(KeyValueGet::new("hello"))
        .add(KeyValueGet::new("hello2"))
        .add(KeyValueGet::new("hello3").filter("nope"))
        .add(KeyValueGet::new("missing?"));

    // send our query
    let mut stream = shoal.send(query).await;
    // skip the next 3 responses since they are just inserts
    stream.skip(3).await;
    // try to cast the next response
    while let Some(key_value) = stream.next_typed_first::<KeyValueRow>().await.unwrap() {
        println!("{:?}", key_value);
    }
}

fn main() {
    // start our shards
    let db = LocalExecutorBuilder::default()
        .spawn(|| async move { shoal_core::server::start::<Basic>().unwrap() })
        .unwrap();
    // sleep for 1s
    std::thread::sleep(std::time::Duration::from_secs(1));
    // test our queries
    test_queries();
    std::thread::sleep(std::time::Duration::from_secs(20));
    // wait for our db to exit
    db.join().unwrap();
}
