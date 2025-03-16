//! A shoal example on TMDB data

use kanal::{AsyncReceiver, AsyncSender};
use owo_colors::OwoColorize;
use shoal_core::client::Shoal;
use shoal_core::server::messages::QueryMetadata;
use shoal_core::server::ring::Ring;
use shoal_core::server::{Conf, ServerError};
use shoal_core::shared::queries::{UnsortedGet, UnsortedQuery, UnsortedUpdate};
use shoal_core::shared::responses::Response;
use shoal_core::shared::traits::{
    PartitionKeySupport, QuerySupport, RkyvSupport, ShoalQuery, ShoalResponse, ShoalUnsortedTable,
};
use shoal_core::shared::traits::{ShoalDatabase, ShoalSortedTable};
use shoal_core::storage::FileSystem;
use shoal_core::tables::PersistentUnsortedTable;
use shoal_core::ShoalPool;
use shoal_derive::ShoalUnsortedTable;

use futures::stream::{FuturesUnordered, StreamExt};
use glommio::TaskQueueHandle;
use gxhash::GxHasher;
use rkyv::{Archive, Deserialize, Serialize};
use std::hash::Hasher;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::net::ToSocketAddrs;
use tokio::task::{JoinHandle, JoinSet};
use uuid::Uuid;

#[derive(
    Debug,
    Archive,
    Serialize,
    Deserialize,
    Clone,
    ShoalUnsortedTable,
    serde::Deserialize,
    serde::Serialize,
    PartialEq,
)]
#[shoal_table(db = "Tmdb")]
pub struct Movie {
    /// The id for this movie
    pub id: u64,
    /// The name of this move
    pub title: String,
    /// The vote average
    pub vote_average: f64,
    /// The total number of votes
    pub vote_count: u64,
    /// The status of this movie
    pub status: String,
    /// The Date this movie was release
    pub release_date: String,
    /// The total revenue this movie made
    pub revenue: u64,
    /// The runtime for this movie in minutes
    pub runtime: u64,
    /// Whether this is an adult movie
    pub adult: bool,
    /// The path to this movies backdrop on tmdb
    pub backdrop_path: String,
    /// The budget for this movie
    pub budget: u64,
    /// The url to this movies homepage
    pub homepage: String,
    /// The imdb id for this movie
    pub imdb_id: String,
    /// The original language for this movie
    pub original_language: String,
    /// The original title for this movie
    pub original_title: String,
    /// The overview for this movie
    pub overview: String,
    /// The popularity of this movie
    pub popularity: f64,
    /// The path to this movies poster on tmdb
    pub poster_path: String,
    /// The tagline for this movie
    pub tagline: String,
    /// The genres for this movie
    pub genres: Vec<String>,
    /// The production companies for this movie
    pub production_companies: Vec<String>,
    /// The countries this movie was produced in
    pub production_countries: Vec<String>,
    /// The languages spoken in this movie
    pub spoken_languages: Vec<String>,
    /// The keywords for this movie
    pub keywords: Vec<String>,
}

//impl Movie {
//    /// Create a new movie row
//    fn new<T: Into<String>>(title: T, overview: T) -> Self {
//        Movie {
//            title: title.into(),
//            overview: overview.into(),
//        }
//    }
//}

impl PartitionKeySupport for Movie {
    /// The partition key type for this data
    type PartitionKey = u64;

    /// The name of this table
    fn name() -> &'static str {
        "Movies"
    }

    /// Calculate the partition key for this row
    fn get_partition_key(&self) -> u64 {
        Self::get_partition_key_from_values(&self.id)
    }

    /// Calculate the partition key for this row
    fn get_partition_key_from_values(values: &Self::PartitionKey) -> u64 {
        // create a new hasher
        let mut hasher = GxHasher::default();
        // hash the first key
        hasher.write_u64(*values);
        // get our hash
        hasher.finish()
    }

    /// Get the partition key for this row from an archived value
    fn get_partition_key_from_archived_insert(intent: &<Self as Archive>::Archived) -> u64 {
        // create a new hasher
        let mut hasher = GxHasher::default();
        // hash the first key
        hasher.write_u64(intent.id.to_native());
        // get our hash
        hasher.finish()
    }
}

impl ShoalUnsortedTable for Movie {
    /// The updates that can be applied to this table
    type Update = String;

    /// Any filters to apply when listing/crawling rows
    type Filters = String;

    /// Determine if a row should be filtered
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters to apply
    /// * `row` - The row to filter
    fn is_filtered(filter: &Self::Filters, row: &Self) -> bool {
        &row.title == filter
    }

    /// Apply an update to a single row
    ///
    /// # Arguments
    ///
    /// * `update` - The update to apply to a specific row
    fn update(&mut self, update: &UnsortedUpdate<Self>) {
        self.overview = update.update.clone();
    }
}

#[derive(Debug, Archive, Serialize)]
#[rkyv(derive(Debug))]
pub struct MovieGet {
    /// The id of the movie to get
    pub id: u64,
    /// Any filters to apply to rows
    pub filters: Option<String>,
    /// The number of rows to get at most
    pub limit: Option<usize>,
}

impl MovieGet {
    /// Create a new get query for a [`MovieRow`]
    ///
    /// # Arguments
    ///
    /// * `key` - The partition key string
    pub fn new(id: u64) -> Self {
        MovieGet {
            id,
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

impl From<MovieGet> for TmdbQueryKinds {
    /// Build our a `QueryKind` for getting `MovieRows`
    fn from(specific: MovieGet) -> Self {
        // build our partition key
        let partition_key =
            <Movie as PartitionKeySupport>::get_partition_key_from_values(&specific.id);
        // build the general query
        let general = UnsortedGet {
            partition_keys: vec![partition_key],
            filters: specific.filters,
            limit: specific.limit,
        };
        // build our query kind
        Self::Movie(UnsortedQuery::Get(general))
    }
}

/// Delete a key value row
#[derive(Debug, Archive, Serialize)]
#[rkyv(derive(Debug))]
pub struct MovieDelete {
    /// Tkey key to the partition to delete a row from
    pub partition_key: u64,
}

impl MovieDelete {
    /// Create a new key value delete
    ///
    /// # Arguments
    ///
    /// * `key` - The key for determining the parititon
    pub fn new(key: u64) -> Self {
        // calculate our partition key
        let partition_key = Movie::get_partition_key_from_values(&key);
        // build a key value delete object
        MovieDelete { partition_key }
    }
}

impl From<MovieDelete> for TmdbQueryKinds {
    /// Build our `QueryKind` for getting `MovieDelete`
    ///
    /// # Arguments
    ///
    /// * `delete` - The delete query
    fn from(delete: MovieDelete) -> Self {
        // build our delete query
        let query = UnsortedQuery::Delete {
            key: delete.partition_key,
        };
        TmdbQueryKinds::Movie(query)
    }
}

/// An update to apply to a row in this table
pub struct MovieUpdate {
    /// The partition key to update data in
    partition_key: u64,
    /// The data to update
    data: String,
}

impl MovieUpdate {
    /// Create an update for a specific row
    ///
    /// # Arguments
    ///
    /// * `key` - The key to the partition to upate data in
    /// * `data` - The new data to set
    pub fn new<D: Into<String>>(key: u64, data: D) -> Self {
        // calculate our partition key
        let partition_key = Movie::get_partition_key_from_values(&key);
        // build a new upidate object
        MovieUpdate {
            partition_key,
            data: data.into(),
        }
    }
}

impl From<MovieUpdate> for TmdbQueryKinds {
    /// Build our `QueryKind` for getting `MovieUpdate`
    ///
    /// # Arguments
    ///
    /// * `specific` - The update query to this table/data type
    fn from(specific: MovieUpdate) -> Self {
        // cast this update to a generalized update
        let general = UnsortedUpdate {
            partition_key: specific.partition_key,
            update: specific.data,
        };
        // wrap our general update in a query
        let query = UnsortedQuery::Update(general);
        // wrap our general update in a table specific query
        TmdbQueryKinds::Movie(query)
    }
}

/// The different tables we can query
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub enum TmdbQueryKinds {
    /// A query for the key/value table
    Movie(UnsortedQuery<Movie>),
}

impl RkyvSupport for TmdbQueryKinds {}

impl ShoalQuery for TmdbQueryKinds {
    /// Deserialize our response types
    ///
    /// # Arguments
    ///
    /// * `buff` - The buffer to deserialize into a response
    fn response_query_id(buff: &[u8]) -> &Uuid {
        // try to cast this query
        let archive = TmdbResponseKinds::load(buff);
        // get our response query id
        match archive {
            ArchivedTmdbResponseKinds::Movie(resp) => &resp.id,
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
            TmdbQueryKinds::Movie(query) => {
                // get our shards info
                query.find_shard(ring, found);
            }
        };
    }
}

/// The different tables we can get responses from
#[derive(Debug, Archive, Serialize, Deserialize)]
pub enum TmdbResponseKinds {
    Movie(Response<Movie>),
}

impl RkyvSupport for TmdbResponseKinds {}

impl ShoalResponse for TmdbResponseKinds {
    /// Get the index of a single [`Self::ResponseKinds`]
    fn get_index(&self) -> usize {
        // get our response index
        match self {
            TmdbResponseKinds::Movie(resp) => resp.index,
        }
    }

    /// Get whether this is the last response in a response stream
    fn is_end_of_stream(&self) -> bool {
        // check if this is the end of the stream
        match self {
            TmdbResponseKinds::Movie(resp) => resp.end,
        }
    }
}

pub struct TmdbClient {}

impl QuerySupport for TmdbClient {
    /// The different tables or types of queries we will handle
    type QueryKinds = TmdbQueryKinds;

    /// The different tables we can get responses from
    type ResponseKinds = TmdbResponseKinds;
}

/// The tables we are adding to to shoal
//#[derive(ShoalDB)]
//#[shoal_db(name = "Basic")]
pub struct Tmdb {
    /// A basic key value table
    pub movies: PersistentUnsortedTable<Movie, FileSystem>,
}

impl ShoalDatabase for Tmdb {
    /// This databases external client type
    type ClientType = TmdbClient;

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
        let db = Tmdb {
            movies: PersistentUnsortedTable::new(shard_name, conf, medium_priority).await?,
        };
        Ok(db)
    }

    /// Handle messages for different table types
    async fn handle(
        &mut self,
        meta: QueryMetadata,
        typed_query: <Self::ClientType as QuerySupport>::QueryKinds,
    ) -> Option<(
        SocketAddr,
        <Self::ClientType as QuerySupport>::ResponseKinds,
    )> {
        // match on the right query and execute it
        match typed_query {
            TmdbQueryKinds::Movie(query) => {
                // handle these queries
                match self.movies.handle(meta, query).await {
                    Some((addr, response)) => {
                        // wrap our response with the right table kind
                        let wrapped = TmdbResponseKinds::Movie(response);
                        Some((addr, wrapped))
                    }
                    None => None,
                }
            }
        }
    }

    /// Flush any in flight writes to disk
    async fn flush(&mut self) -> Result<(), ServerError> {
        self.movies.flush().await
    }

    /// Get all flushed messages and send their response back
    ///
    /// # Arguments
    ///
    /// * `flushed` - The flushed response to send back
    async fn handle_flushed(
        &mut self,
        flushed: &mut Vec<(
            SocketAddr,
            <Self::ClientType as QuerySupport>::ResponseKinds,
        )>,
    ) -> Result<(), ServerError> {
        // get all flushed queries in their specific format
        let specific = self.movies.get_flushed().await?;
        // wrap and add our specific queries
        let wrapped = specific
            .drain(..)
            .map(|(addr, resp)| (addr, TmdbResponseKinds::Movie(resp)));
        // extend our response list with our wrapped queries
        flushed.extend(wrapped);
        Ok(())
    }

    /// Shutdown this table and flush any data to disk if needed
    async fn shutdown(&mut self) -> Result<(), ServerError> {
        // shutdown the key value table
        self.movies.shutdown().await
    }
}

pub enum MovieMsg {
    /// Insert a movie into shoal
    Insert(Movie),
    /// Verify a movies data in shoal
    Verify(Movie),
    /// Shutdown this worker
    Shutdown,
}

pub struct MovieWorker {
    /// The client for shoal
    shoal: Arc<Shoal<TmdbClient>>,
    /// The channel to add movies too
    movies_tx: AsyncSender<MovieMsg>,
    /// The channel to receive movies on
    movies_rx: AsyncReceiver<MovieMsg>,
    // keep a list so we can send our movies in 10 at a time
    insert_buffer: Vec<Movie>,
}

impl MovieWorker {
    /// Create a new Movie worker
    ///
    /// # Arguments
    ///
    /// * `shoal` - A client for shoal
    /// * `movies_rx` - A channel to receive movies on
    pub fn new(
        shoal: &Arc<Shoal<TmdbClient>>,
        movies_tx: &AsyncSender<MovieMsg>,
        movies_rx: &AsyncReceiver<MovieMsg>,
    ) -> Self {
        MovieWorker {
            shoal: shoal.clone(),
            movies_tx: movies_tx.clone(),
            movies_rx: movies_rx.clone(),
            insert_buffer: Vec::with_capacity(10),
        }
    }

    /// Send our currently buffered movies to shoal
    async fn send(&mut self) {
        // Start building a query to shoal
        let mut query = self.shoal.query();
        // get the number of movies in our buffer
        let buffered = self.insert_buffer.len();
        // add each movie to this query
        for movie in self.insert_buffer.drain(..) {
            // add this movie to our query
            query.add_mut(movie);
        }
        // send this query to shoal
        let mut stream = self.shoal.send(query).await.unwrap();
        // skip all responses
        stream.skip(buffered).await.unwrap();
    }

    /// Start streaming movies into Shoal
    pub async fn start(mut self) {
        // keep looping until we have no more movies to send
        loop {
            // wait for a intent log compaction job
            let job = self.movies_rx.recv().await.unwrap();
            // handle this job
            match job {
                MovieMsg::Insert(movie) => {
                    // add this movie to  our buffer
                    self.insert_buffer.push(movie);
                    // check if we have 10 movies to send
                    if self.insert_buffer.len() > 10 {
                        // send all of our buffered movies
                        self.send().await;
                    }
                }
                MovieMsg::Verify(movie) => {
                    // build the query to get this movies info
                    let query = self.shoal.query().add(MovieGet::new(movie.id));
                    // start this query
                    let mut stream = self.shoal.send(query).await.unwrap();
                    // this query will only ever return a single row
                    match stream.next_typed_first::<Movie>().await.unwrap() {
                        Some(Some(movie_data)) => {
                            // make sure this movies data matches
                            if movie != movie_data {
                                panic!("{} has invalid data - {movie_data:#?}", movie.title);
                            }
                        }
                        _ => panic!("{} is missing", movie.title),
                    }
                }
                MovieMsg::Shutdown => {
                    // reemit the shutdown order
                    self.movies_tx.send(MovieMsg::Shutdown).await.unwrap();
                    // there is no more movies to send so shutdown this worker
                    break;
                }
            }
        }
        // check if we have any remaining buffered movies
        if !self.insert_buffer.is_empty() {
            // send all of our remaining buffered movies
            self.send().await;
        }
    }
}

pub struct MovieController {
    /// A shoal client
    shoal: Arc<Shoal<TmdbClient>>,
    /// The channel to add movies too
    movies_tx: AsyncSender<MovieMsg>,
    /// The channel to receive movies on
    movies_rx: AsyncReceiver<MovieMsg>,
    /// The tasks for this controllers workers
    tasks: JoinSet<()>,
}

impl MovieController {
    /// Create a default movie controller
    async fn new<A: ToSocketAddrs>(addr: A) -> Self {
        // build a client for Shoal
        let shoal = Shoal::<TmdbClient>::new(addr).await.unwrap();
        // instance a large but bounded channel
        let (movies_tx, movies_rx) = kanal::bounded_async(1);
        // create our controller
        MovieController {
            shoal: Arc::new(shoal),
            movies_tx,
            movies_rx,
            tasks: JoinSet::default(),
        }
    }
}

impl MovieController {
    /// Spawn workers for this controller
    fn spawn(&mut self, count: usize) {
        for _ in 0..count {
            // create a new worker
            let worker = MovieWorker::new(&self.shoal, &self.movies_tx, &self.movies_rx);
            // spawn this worker
            self.tasks.spawn(worker.start());
        }
    }

    /// Upload data to shoal
    async fn upload<P: AsRef<Path>>(&mut self, path: P) {
        // open a handle to our tmdb dataset
        let file = File::open(path).await.unwrap();
        // wrap our file in a csv reader
        let mut reader = csv_async::AsyncDeserializer::from_reader(file);
        // set the type we are going to deserialize
        let mut typed_reader = reader.deserialize::<Movie>();
        // skip any movies we fail to deserialize
        while let Some(Ok(movie)) = typed_reader.next().await {
            // add our movie to our channel
            self.movies_tx.send(MovieMsg::Insert(movie)).await.unwrap();
        }
    }

    /// verify data in shoal
    async fn verify<P: AsRef<Path>>(&mut self, path: P) {
        // open a handle to our tmdb dataset
        let file = File::open(path).await.unwrap();
        // wrap our file in a csv reader
        let mut reader = csv_async::AsyncDeserializer::from_reader(file);
        // set the type we are going to deserialize
        let mut typed_reader = reader.deserialize::<Movie>();
        // skip any movies we fail to deserialize
        while let Some(Ok(movie)) = typed_reader.next().await {
            // add our movie to our channel to be verified
            self.movies_tx.send(MovieMsg::Verify(movie)).await.unwrap();
        }
    }

    /// Start streaming jobs to our workers
    pub async fn start<P: AsRef<Path>>(&mut self, path: P) {
        // spawn 5 workers
        self.spawn(5);
        // upload our tmdb data
        self.upload(&path).await;
        // emit that workers should shutdown once all movie info has been streamed to shoal
        self.movies_tx.send(MovieMsg::Shutdown).await.unwrap();
        // swap our task with with a default one
        let tasks = std::mem::take(&mut self.tasks);
        // wait for all workers to complete
        tasks.join_all().await;
        // pop the last shutdown message
        self.movies_rx.recv().await.unwrap();
        // spawn 5 workers
        self.spawn(5);
        tokio::time::sleep(Duration::from_secs(3)).await;
        // verify our tmdb data
        self.verify(&path).await;
        // emit that workers should shutdown once all movie info has been streamed to shoal
        self.movies_tx.send(MovieMsg::Shutdown).await.unwrap();
        // swap our task with with a default one
        let tasks = std::mem::take(&mut self.tasks);
        // wait for all workers to complete
        tasks.join_all().await;
    }
}

#[tokio::main]
async fn read_csv() {
    // start ou controller
    let mut controller = MovieController::new("0.0.0.0:0").await;
    // start streaming movies to shoal with multiple workers
    controller
        .start("/home/mcarson/datasets/TMDB_movie_dataset_v11.csv")
        .await;
}

fn main() {
    // start Shoal
    let pool = ShoalPool::<Tmdb>::start().unwrap();
    // sleep for 1s
    std::thread::sleep(std::time::Duration::from_secs(1));
    // read and insert our csv
    read_csv();
    // wait for our db to exit
    pool.exit().unwrap();
}
//
