use glommio::channels::channel_mesh::Senders;
use glommio::{executor, net::UdpSocket, LocalExecutorBuilder};
use gxhash::GxHasher;
use rkyv::{Archive, Deserialize, Serialize};
use std::hash::Hasher;
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

mod client;
mod server;
pub mod shared;

use server::messages::MeshMsg;
use server::ring::Ring;
use server::shard::ShardContact;
use shared::queries::{Get, Queries, Query, TaggedQuery};
use shared::responses::Response;
use shared::traits::{ShoalDatabase, ShoalTable};

use server::{Conf, ServerError, Table};

// A dummy row
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct DummyRow {
    /// A partition key
    key: String,
    /// A value
    value: String,
}

impl DummyRow {
    /// Create a new dummy row
    fn new<T: Into<String>>(key: T, value: T) -> Self {
        DummyRow {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl From<DummyRow> for QueryKinds {
    fn from(row: DummyRow) -> QueryKinds {
        // get our rows partition key
        let key = DummyRow::partition_key(&row.key);
        // build our query kind
        QueryKinds::DummyQuery(Query::Insert { key, row })
    }
}

impl ShoalTable for DummyRow {
    /// The sort type for this dadta
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

#[derive(Debug, Archive, Serialize)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
pub struct DummyGet {
    /// The key to get rows with
    pub key: String,
    /// Any filters to apply to rows
    pub filters: Option<String>,
    /// The number of rows to get at most
    pub limit: Option<usize>,
}

impl DummyGet {
    /// Create a new get query for a [`DummyRow`]
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key string
    pub fn new<T: Into<String>>(key: T) -> Self {
        DummyGet {
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

impl From<DummyGet> for QueryKinds {
    /// Build our a `QueryKind` for getting `DummyRows`
    fn from(specific: DummyGet) -> Self {
        // build our sort key
        let key = <DummyRow as ShoalTable>::Sort::from(specific.key);
        // build our partition key
        let partition_key = <DummyRow as ShoalTable>::partition_key(&key);
        // build the general query
        let general = Get {
            partition_keys: vec![partition_key],
            partitions: vec![key],
            filters: specific.filters,
            limit: specific.limit,
        };
        // build our query kind
        Self::DummyQuery(Query::Get(general))
    }
}

/// The different tables we can query
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
#[archive(check_bytes)]
pub enum QueryKinds {
    /// A query for the dummy table
    DummyQuery(Query<DummyRow>),
}

/// The different tables we can get responses from
#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum ResponseKinds {
    Dummy(Response<DummyRow>),
}

/// The tables we are adding to to shoal
#[derive(Default, Debug)]
pub struct Tables {
    /// A dummy table
    pub dummy: Table<DummyRow>,
}

impl ShoalDatabase for Tables {
    /// The different tables or types of queries we will handle
    type QueryKinds = QueryKinds;

    /// The different tables we can get responses from
    type ResponseKinds = ResponseKinds;

    /// Deserialize our query types
    fn unarchive(buff: &[u8]) -> Queries<Self> {
        // try to cast this query
        let query = rkyv::check_archived_root::<Queries<Self>>(buff).unwrap();
        // deserialize it
        query.deserialize(&mut rkyv::Infallible).unwrap()
    }

    // Deserialize our response types
    fn unarchive_response(buff: &[u8]) -> Self::ResponseKinds {
        // try to cast this query
        let query = rkyv::check_archived_root::<Self::ResponseKinds>(buff).unwrap();
        // deserialize it
        query.deserialize(&mut rkyv::Infallible).unwrap()
    }

    // Deserialize our response types
    fn response_query_id(buff: &[u8]) -> &Uuid {
        // try to cast this query
        let kinds = rkyv::check_archived_root::<Self::ResponseKinds>(buff).unwrap();
        // get our query id
        match kinds {
            ArchivedResponseKinds::Dummy(resp) => &resp.id,
        }
    }

    /// Get the index of a single [`Self::ResponseKinds`]
    ///
    /// # Arguments
    ///
    /// * `resp` - The resp to to get the order index for
    fn response_index(resp: &ResponseKinds) -> usize {
        match resp {
            ResponseKinds::Dummy(resp) => resp.index,
        }
    }

    /// Get whether this is the last response in a response stream
    ///
    /// # Arguments
    ///
    /// * `resp` - The response to check
    fn is_end_of_stream(resp: &Self::ResponseKinds) -> bool {
        match resp {
            ResponseKinds::Dummy(resp) => resp.end,
        }
    }

    /// Forward our queries to the correct shards
    async fn send_to_shard(
        ring: &Ring,
        mesh_tx: &mut Senders<MeshMsg<Self>>,
        addr: SocketAddr,
        queries: Queries<Self>,
    ) -> Result<(), ServerError> {
        let mut tmp = Vec::with_capacity(1);
        // get the index for the last query in this bundle
        let end_index = queries.queries.len() - 1;
        // crawl over our queries
        for (index, kind) in queries.queries.into_iter().enumerate() {
            // get our target shards info
            match &kind {
                QueryKinds::DummyQuery(query) => {
                    // get our shards info
                    query.find_shard(ring, &mut tmp);
                }
            };
            // send this query to the right shards
            for shard_info in tmp.drain(..) {
                match &shard_info.contact {
                    ShardContact::Local(id) => {
                        //println!("coord - sending query to shard: {id}");
                        mesh_tx
                            .send_to(
                                *id,
                                MeshMsg::Query {
                                    addr,
                                    id: queries.id,
                                    index,
                                    query: kind.clone(),
                                    end: index == end_index,
                                },
                            )
                            .await
                            .unwrap();
                    }
                };
            }
        }
        Ok(())
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
            QueryKinds::DummyQuery(query) => {
                // handle these queries
                ResponseKinds::Dummy(self.dummy.handle(id, index, query, end))
            }
        }
    }
}

async fn test_queries_helper() {
    // build a client for Shoal
    let shoal = client::Shoal::<Tables>::new("0.0.0.0:0").await;
    // track the number of queries we have done
    let mut cnt = 0;
    loop {
        println!("cnt -> {cnt} / chans -> {}", shoal.channel_map.len());
        // build a query
        let query = shoal
            .query()
            .add(DummyRow::new("hello", "world1"))
            .add(DummyRow::new("hello2", "world2"))
            .add(DummyRow::new("hello3", "nope"))
            .add(DummyGet::new("hello"))
            .add(DummyGet::new("hello2"))
            .add(DummyGet::new("hello3").filter("nope"))
            .add(DummyGet::new("missing?"));
        // send our query
        let mut stream = shoal.send(query).await;
        // read our responses
        while let Some(msg) = stream.next().await {
            match msg {
                ResponseKinds::Dummy(resp) => println!("main - {resp:?}"),
            }
        }
        cnt += 1;
        if cnt == 10 {
            break;
        }
    }
    //// sleep for 5 seconds
    //tokio::time::sleep(Duration::from_secs(10)).await;
}

#[tokio::main]
async fn test_queries_scale() {
    // execute out test queries
    let handle_1 = tokio::task::spawn(async move { test_queries_helper().await });
    let handle_2 = tokio::task::spawn(async move { test_queries_helper().await });
    let handle_3 = tokio::task::spawn(async move { test_queries_helper().await });
    let handle_4 = tokio::task::spawn(async move { test_queries_helper().await });
    let handle_5 = tokio::task::spawn(async move { test_queries_helper().await });
    tokio::try_join!(handle_1, handle_2, handle_3, handle_4, handle_5).unwrap();
    // sleep for 5 seconds
    tokio::time::sleep(Duration::from_secs(10)).await;
}

#[tokio::main]
async fn test_queries() {
    // build a client for Shoal
    let shoal = client::Shoal::<Tables>::new("0.0.0.0:0").await;
    // build a query
    let query = shoal
        .query()
        .add(DummyRow::new("hello", "world1"))
        .add(DummyRow::new("hello2", "world2"))
        .add(DummyRow::new("hello3", "nope"))
        .add(DummyGet::new("hello"))
        .add(DummyGet::new("hello2"))
        .add(DummyGet::new("hello3").filter("nope"))
        .add(DummyGet::new("missing?"));
    // send our query
    let mut stream = shoal.send(query).await;
    // read our responses
    while let Some(msg) = stream.next().await {
        match msg {
            ResponseKinds::Dummy(resp) => println!("main - {resp:?}"),
        }
    }
}

fn main() {
    // start our shards
    let db = LocalExecutorBuilder::default()
        .spawn(|| async move { server::start::<Tables>().unwrap() })
        .unwrap();
    // sleep for 1s
    std::thread::sleep(std::time::Duration::from_secs(1));
    // test our queries
    test_queries();
    std::thread::sleep(Duration::from_secs(20));
    // wait for our db to exit
    db.join().unwrap();
}
