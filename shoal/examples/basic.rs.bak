//! A basic key/value database example

use shoal::bencher::Bencher;
use shoal_core::server::messages::QueryMetadata;
use shoal_core::server::ring::Ring;
use shoal_core::server::{Conf, ServerError};
use shoal_core::shared::queries::{SortedGet, SortedQuery, SortedUpdate};
use shoal_core::shared::responses::Response;
use shoal_core::shared::traits::{
    PartitionKeySupport, QuerySupport, RkyvSupport, ShoalDatabase, ShoalQuery, ShoalResponse,
    ShoalSortedTable,
};
use shoal_core::storage::FileSystem;
use shoal_core::tables::PersistentSortedTable;
use shoal_core::{rkyv, ShoalPool};
use shoal_derive::ShoalSortedTable;

use glommio::TaskQueueHandle;
use gxhash::GxHasher;
use rkyv::{Archive, Deserialize, Serialize};
use std::hash::Hasher;
use std::net::SocketAddr;
use uuid::Uuid;

// A basic key/value table in Shoal
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable)]
#[shoal_table(db = "Basic")]
pub struct KeyValue {
    /// A partition key
    key: String,
    /// A sort key
    sort: String,
    /// The data for this sorted row
    data: String,
}

impl KeyValue {
    /// Create a new key/value row
    fn new<T: Into<String>>(key: T, sort: T, data: T) -> Self {
        KeyValue {
            key: key.into(),
            sort: sort.into(),
            data: data.into(),
        }
    }
}

impl PartitionKeySupport for KeyValue {
    /// The partition key type for this data
    type PartitionKey = String;

    /// The name of this table
    fn name() -> &'static str {
        "KeyValue"
    }

    /// Calculate the partition key for this row
    fn get_partition_key(&self) -> u64 {
        Self::get_partition_key_from_values(&self.key)
    }

    /// Calculate the partition key for this row
    fn get_partition_key_from_values(values: &Self::PartitionKey) -> u64 {
        // create a new hasher
        let mut hasher = GxHasher::default();
        // hash the first key
        hasher.write(values.as_bytes());
        // get our hash
        hasher.finish()
    }

    /// Get the partition key for this row from an archived value
    fn get_partition_key_from_archived_insert(intent: &<Self as Archive>::Archived) -> u64 {
        // create a new hasher
        let mut hasher = GxHasher::default();
        // hash the first key
        hasher.write(intent.key.as_bytes());
        // get our hash
        hasher.finish()
    }
}

impl ShoalSortedTable for KeyValue {
    /// The updates that can be applied to this table
    type Update = String;

    /// The sort type for this data
    type Sort = String;

    /// Build the sort tuple for this row
    fn get_sort(&self) -> &Self::Sort {
        &self.sort
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
        &row.data == filter
    }

    /// Apply an update to a single row
    ///
    /// # Arguments
    ///
    /// * `update` - The update to apply to a specific row
    fn update(&mut self, update: &SortedUpdate<Self>) {
        self.data = update.update.clone();
    }
}

#[derive(Debug, Archive, Serialize)]
#[rkyv(derive(Debug))]
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
        let key = <KeyValue as ShoalSortedTable>::Sort::from(specific.key);
        // build our partition key
        let partition_key = <KeyValue as PartitionKeySupport>::get_partition_key_from_values(&key);
        // build the general query
        let general = SortedGet {
            partition_keys: vec![partition_key],
            sort_keys: vec![key],
            filters: specific.filters,
            limit: specific.limit,
        };
        // build our query kind
        Self::KeyValue(SortedQuery::Get(general))
    }
}

/// Delete a key value row
#[derive(Debug, Archive, Serialize)]
#[rkyv(derive(Debug))]
pub struct KeyValueDelete {
    /// Tkey key to the partition to delete a row from
    pub partition_key: u64,
    /// The sort key to delete
    pub sort_key: String,
}

impl KeyValueDelete {
    /// Create a new key value delete
    ///
    /// # Arguments
    ///
    /// * `key` - The key for determining the parititon
    /// * `sort_key` - The values used to sort data
    pub fn new<S: Into<String>>(key: &String, sort_key: S) -> Self {
        // calculate our partition key
        let partition_key = KeyValue::get_partition_key_from_values(key);
        // build a key value delete object
        KeyValueDelete {
            partition_key,
            sort_key: sort_key.into(),
        }
    }
}

impl From<KeyValueDelete> for BasicQueryKinds {
    /// Build our `QueryKind` for getting `KeyValueDelete`
    ///
    /// # Arguments
    ///
    /// * `delete` - The delete query
    fn from(delete: KeyValueDelete) -> Self {
        // build our delete query
        let query = SortedQuery::Delete {
            key: delete.partition_key,
            sort_key: delete.sort_key,
        };
        BasicQueryKinds::KeyValue(query)
    }
}

/// An update to apply to a row in this table
pub struct KeyValueUpdate {
    /// The partition key to update data in
    partition_key: u64,
    /// The sort key for a specific row in a partition
    sort_key: String,
    /// The data to update
    data: String,
}

impl KeyValueUpdate {
    /// Create an update for a specific row
    ///
    /// # Arguments
    ///
    /// * `key` - The key to the partition to upate data in
    /// * `sort_key` - The sort key used to identify the specific row to update in a partition
    /// * `data` - The new data to set
    pub fn new<S: Into<String>, D: Into<String>>(key: &String, sort_key: S, data: D) -> Self {
        // calculate our partition key
        let partition_key = KeyValue::get_partition_key_from_values(key);
        // build a new upidate object
        KeyValueUpdate {
            partition_key,
            sort_key: sort_key.into(),
            data: data.into(),
        }
    }
}

impl From<KeyValueUpdate> for BasicQueryKinds {
    /// Build our `QueryKind` for getting `KeyValueUpdate`
    ///
    /// # Arguments
    ///
    /// * `specific` - The update query to this table/data type
    fn from(specific: KeyValueUpdate) -> Self {
        // cast this update to a generalized update
        let general = SortedUpdate {
            partition_key: specific.partition_key,
            sort_key: specific.sort_key,
            update: specific.data,
        };
        // wrap our general update in a query
        let query = SortedQuery::Update(general);
        // wrap our general update in a table specific query
        BasicQueryKinds::KeyValue(query)
    }
}

/// The different tables we can query
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub enum BasicQueryKinds {
    /// A query for the key/value table
    KeyValue(SortedQuery<KeyValue>),
}

impl RkyvSupport for BasicQueryKinds {}

impl ShoalQuery for BasicQueryKinds {
    /// Deserialize our response types
    ///
    /// # Arguments
    ///
    /// * `buff` - The buffer to deserialize into a response
    fn response_query_id(buff: &[u8]) -> &Uuid {
        // try to cast this query
        let archive = BasicResponseKinds::load(buff);
        //let kinds = shoal_core::rkyv::check_archived_root::<BasicResponseKinds>(buff).unwrap();
        // get our response query id
        match archive {
            ArchivedBasicResponseKinds::KeyValue(resp) => &resp.id,
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
            BasicQueryKinds::KeyValue(query) => {
                // get our shards info
                query.find_shard(ring, found);
            }
        };
    }
}

/// The different tables we can get responses from
#[derive(Debug, Archive, Serialize, Deserialize)]
pub enum BasicResponseKinds {
    KeyValue(Response<KeyValue>),
}

impl RkyvSupport for BasicResponseKinds {}

impl ShoalResponse for BasicResponseKinds {
    /// Get the index of a single [`Self::ResponseKinds`]
    fn get_index(&self) -> usize {
        // get our response index
        match self {
            BasicResponseKinds::KeyValue(resp) => resp.index,
        }
    }

    /// Get whether this is the last response in a response stream
    fn is_end_of_stream(&self) -> bool {
        // check if this is the end of the stream
        match self {
            BasicResponseKinds::KeyValue(resp) => resp.end,
        }
    }
}

/// The tables we are adding to to shoal
//#[derive(ShoalDB)]
//#[shoal_db(name = "Basic")]
pub struct Basic {
    /// A basic key value table
    pub key_value: PersistentSortedTable<KeyValue, FileSystem>,
}

impl QuerySupport for Basic {
    /// The different tables or types of queries we will handle
    type QueryKinds = BasicQueryKinds;

    /// The different tables we can get responses from
    type ResponseKinds = BasicResponseKinds;
}

impl ShoalDatabase for Basic {
    /// Create a new shoal db instance
    ///
    /// # Arguments
    ///
    /// * `shard_name` - The id of the shard that owns this table
    /// * `conf` - A shoal config
    async fn new(
        shard_name: &str,
        conf: &Conf,
        medium_priority: TaskQueueHandle,
    ) -> Result<Self, ServerError> {
        let db = Basic {
            key_value: PersistentSortedTable::new(shard_name, conf, medium_priority).await?,
        };
        Ok(db)
    }

    /// Handle messages for different table types
    async fn handle(
        &mut self,
        meta: QueryMetadata,
        typed_query: Self::QueryKinds,
    ) -> Option<(SocketAddr, Self::ResponseKinds)> {
        // match on the right query and execute it
        match typed_query {
            BasicQueryKinds::KeyValue(query) => {
                // handle these queries
                match self.key_value.handle(meta, query).await {
                    Some((addr, response)) => {
                        // wrap our response with the right table kind
                        let wrapped = BasicResponseKinds::KeyValue(response);
                        Some((addr, wrapped))
                    }
                    None => None,
                }
            }
        }
    }

    /// Flush any in flight writes to disk
    async fn flush(&mut self) -> Result<(), ServerError> {
        self.key_value.flush().await
    }

    /// Get all flushed messages and send their response back
    ///
    /// # Arguments
    ///
    /// * `flushed` - The flushed response to send back
    async fn handle_flushed(
        &mut self,
        flushed: &mut Vec<(SocketAddr, Self::ResponseKinds)>,
    ) -> Result<(), ServerError> {
        // get all flushed queries in their specific format
        let specific = self.key_value.get_flushed().await?;
        // wrap and add our specific queries
        let wrapped = specific
            .drain(..)
            .map(|(addr, resp)| (addr, BasicResponseKinds::KeyValue(resp)));
        // extend our response list with our wrapped queries
        flushed.extend(wrapped);
        Ok(())
    }

    /// Shutdown this table and flush any data to disk if needed
    async fn shutdown(&mut self) -> Result<(), ServerError> {
        // shutdown the key value table
        self.key_value.shutdown().await
    }
}

#[tokio::main]
async fn test_queries() {
    // build a client for Shoal
    let shoal = shoal_core::client::Shoal::<Basic>::new("0.0.0.0:0")
        .await
        .unwrap();
    // create a new bencher
    let mut bencher = Bencher::new(".benchmark", 1000);
    for _ in 0..1000 {
        // start this instance timer
        bencher.instance_start();
        // build a query
        let query = shoal
            .query()
            .add(KeyValue::new("hello", "world1", "1"))
            .add(KeyValue::new("hello", "world2", "2"))
            .add(KeyValue::new("hello", "world1", "3"))
            .add(KeyValueUpdate::new(&"hello3".to_owned(), "world3", "3-"))
            .add(KeyValue::new("RemoveMe", "Please", "1"))
            .add(KeyValueDelete::new(&"RemoveMe".to_owned(), "Please"))
            .add(KeyValueGet::new("hello"))
            .add(KeyValueGet::new("hello2"))
            .add(KeyValueGet::new("hello3"))
            .add(KeyValueGet::new("RemoveMe"));
        // send our query
        let mut stream = shoal.send(query).await.unwrap();
        // skip the next 3 responses since they are just inserts
        stream.skip(10).await.unwrap();
        //// try to cast the next response
        //while let Some(key_value) = stream.next_typed_first::<KeyValueRow>().await.unwrap() {
        //    println!("{:?}", key_value);
        //}
        // stop this instance timer
        bencher.instance_stop();
    }
    // finish this benchmark
    bencher.finish(false);
}

fn main() {
    // start Shoal
    let pool = ShoalPool::<Basic>::start().unwrap();
    // sleep for 1s
    std::thread::sleep(std::time::Duration::from_secs(1));
    // test our queries
    test_queries();
    // wait for our db to exit
    pool.exit().unwrap();
}
