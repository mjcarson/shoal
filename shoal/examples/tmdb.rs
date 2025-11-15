//! A shoal example on TMDB data

use shoal::bencher::{BenchWorker, Bencher};
use shoal_core::client::{Shoal, ShoalQueryStream, ShoalResponse, ShoalResultStream};
use shoal_core::server::messages::{QueryMetadata, ShardMsg};
use shoal_core::server::ring::Ring;
use shoal_core::server::{Conf, ServerError};
use shoal_core::shared::queries::{Queries, UnsortedGet, UnsortedQuery, UnsortedUpdate};
use shoal_core::shared::responses::{ArchivedResponseAction, Response};
use shoal_core::shared::traits::ShoalDatabase;
use shoal_core::shared::traits::{
    PartitionKeySupport, QuerySupport, RkyvSupport, ShoalQuerySupport, ShoalResponseSupport,
    ShoalUnsortedTable,
};
use shoal_core::storage::{FileSystem, FullArchiveMap, LoaderMsg, Loaders};
use shoal_core::tables::PersistentUnsortedTable;
use shoal_core::ShoalPool;
use shoal_derive::{ShoalDB, ShoalUnsortedTable};

use deepsize2::DeepSizeOf;
use futures::stream::StreamExt;
use glommio::TaskQueueHandle;
use gxhash::GxHasher;
use kanal::{AsyncReceiver, AsyncSender};
use mimalloc::MiMalloc;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::hash::Hasher;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::net::ToSocketAddrs;
use tokio::task::JoinSet;
use tokio::time::Instant;
use uuid::Uuid;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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
    DeepSizeOf,
)]
#[rkyv(derive(Debug))]
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

    /// Determine if a row should be filtered against an archived row
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters to apply
    /// * `row` - The row to filter
    fn is_filtered_archived(
        filter: &Self::Filters,
        row: &<Self as rkyv::Archive>::Archived,
    ) -> bool {
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
            partition_key,
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

impl ShoalQuerySupport for TmdbQueryKinds {
    /// Deserialize our response types
    ///
    /// # Arguments
    ///
    /// * `buff` - The buffer to deserialize into a response
    fn response_query_id(buff: &[u8]) -> Result<&Uuid, rkyv::rancor::Error> {
        // try to cast this query
        let archive = TmdbResponseKinds::access(buff)?;
        // get our response query id
        match archive {
            ArchivedTmdbResponseKinds::Movie(resp) => Ok(&resp.id),
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

impl ShoalResponseSupport for TmdbResponseKinds {
    /// Get the index of a single [`Self::ResponseKinds`]
    fn get_index_archived(archived: &<Self as Archive>::Archived) -> usize {
        // get our response index
        match archived {
            // TODO fix this usize conversion
            ArchivedTmdbResponseKinds::Movie(resp) => resp.index.to_native() as usize,
        }
    }

    /// Get whether this is the last response in a response stream
    fn is_end_of_stream(archived: &<Self as Archive>::Archived) -> bool {
        // check if this is the end of the stream
        match archived {
            ArchivedTmdbResponseKinds::Movie(resp) => resp.end,
        }
    }

    /// Get the query id from the response
    fn get_query_id(archived: &<Self as Archive>::Archived) -> Uuid {
        // get our response query id
        match archived {
            ArchivedTmdbResponseKinds::Movie(resp) => resp.id.to_owned(),
        }
    }
}

/// The tables we are adding to to shoal
#[derive(ShoalDB)]
//#[shoal_db(name = "Basic")]
pub struct Tmdb {
    /// A basic key value table
    pub movie: PersistentUnsortedTable<Movie, FileSystem, TmdbTableNames>,
}

pub enum MovieMsg<Q: QuerySupport> {
    /// Insert a movie into shoal
    Insert(Movie),
    /// Verify a movies data in shoal
    Verify(Movie),
    /// A Response from a shoal query
    Response(ShoalResponse<Q>),
    /// Shutdown this worker
    Shutdown(Vec<u8>),
}

pub async fn response_streamer(
    movies_tx: AsyncSender<MovieMsg<TmdbClient>>,
    mut response_stream: ShoalResultStream<TmdbClient>,
) {
    // keep getting responses until this stream closes
    while let Some(response) = response_stream.next().await.unwrap() {
        // wrap our response in a MovieMsg
        let wrapped = MovieMsg::Response(response);
        // send this response to our main worker
        movies_tx.send(wrapped).await.unwrap();
    }
}

pub struct MovieWorker {
    /// The id for this worker
    id: u8,
    /// A shoal client
    shoal: Arc<Shoal<TmdbClient>>,
    /// The channel to add movies too
    movies_tx: AsyncSender<MovieMsg<TmdbClient>>,
    /// The channel to receive movies on
    movies_rx: AsyncReceiver<MovieMsg<TmdbClient>>,
    /// Buffered queries to send to shoal
    buffer: Queries<TmdbClient>,
    /// The benchmark worker for this worker
    bencher: BenchWorker,
    /// A map of timers for benchmarking
    timers: HashMap<u64, Instant>,
    /// Whether this worker has already reemitted the shutdown order or not
    reemitted_shutdown: bool,
}

impl MovieWorker {
    /// Create a new Movie worker
    ///
    /// # Arguments
    ///
    /// * `shoal` - A client for shoal
    /// * `movies_rx` - A channel to receive movies on
    /// * `bencher` - The benchmark worker to use
    pub async fn new(
        id: u8,
        shoal: Arc<Shoal<TmdbClient>>,
        movies_tx: &AsyncSender<MovieMsg<TmdbClient>>,
        movies_rx: &AsyncReceiver<MovieMsg<TmdbClient>>,
        bencher: BenchWorker,
    ) -> Self {
        // get a default query object
        let buffer = shoal.query();
        // create our movie worker
        MovieWorker {
            id,
            shoal,
            movies_tx: movies_tx.clone(),
            movies_rx: movies_rx.clone(),
            buffer,
            bencher,
            timers: HashMap::with_capacity(10000),
            reemitted_shutdown: false,
        }
    }

    ///// Send our currently buffered movies to shoal
    //async fn send(&mut self) {
    //    // Start building a query to shoal
    //    // let mut query = self
    //    //     .query_stream
    //    //     .query_with_capacity(self.insert_buffer.len());
    //    let mut query = self.shoal.query();
    //    // add each movie to this query
    //    for movie in self.insert_buffer.drain(..) {
    //        //println!("sending {} / {}", movie.id, movie.title);
    //        // add this movie to our query
    //        query.add_mut(movie);
    //    }
    //    // get the number of queries we are sending
    //    let skip = query.queries.len();
    //    //self.count += 1;
    //    // send this query to shoal
    //    let mut stream = self.shoal.send(query).await.unwrap();
    //    // wait for these results to complete
    //    stream.skip(skip).await.unwrap();
    //    //self.query_stream.add(query).await.unwrap();
    //}

    ///// Start streaming movies into Shoal
    //pub async fn start(mut self) -> BenchWorker {
    //    // keep looping until we have no more movies to send
    //    loop {
    //        // wait for a intent log compaction job
    //        let job = self.movies_rx.recv().await.unwrap();
    //        // handle this job
    //        match job {
    //            MovieMsg::Insert(movie) => {
    //                // add this movie to  our buffer
    //                self.insert_buffer.push(movie.clone());
    //                // check if we have 10 movies to send
    //                if self.insert_buffer.len() > 10 {
    //                    // benchmark only commands to the db when it comes to inserts
    //                    self.bencher.instance_start();
    //                    // send all of our buffered movies
    //                    self.send().await;
    //                    // stop our command benchmark for inserts
    //                    self.bencher.instance_stop();
    //                }
    //            }
    //            MovieMsg::Verify(movie) => {
    //                //// add a get query for this movie
    //                //self.get_buffer.push(MovieGet::new(movie.id));
    //                //// check if we have 10 movies to send
    //                //if self.get_buffer.len() > 10 {
    //                //    //// benchmark only commands to the db when it comes to inserts
    //                //    //self.bencher.instance_start();
    //                //    // send all of our buffered movies
    //                //    self.send().await;
    //                //    //// stop our command benchmark for inserts
    //                //    //self.bencher.instance_stop();
    //                //}
    //                // benchmark the entire verify operation
    //                self.bencher.instance_start();
    //                // build the query to get this movies info
    //                let query = self.shoal.query().add(MovieGet::new(movie.id));
    //                // start this query
    //                let mut stream = self.shoal.send(query).await.unwrap();
    //                // get our results
    //                while let Some(response) = stream.next().await.unwrap() {
    //                    // access our responses data
    //                    // there is a double Option because a stream can end or it a get can return nothing
    //                    if let Some(archived) = response.access::<Movie>().unwrap() {
    //                        // make sure our movie data matches
    //                        if movie.title != archived[0].title {
    //                            panic!(
    //                                "{} has invalid data - {:#?}",
    //                                movie.title, archived[0].title
    //                            );
    //                        }
    //                        //println!("FOUND {}", archived[0].title)
    //                    }
    //                    //let archived = response.access();
    //                    //if let ArchivedTmdbResponseKinds::Movie(action) = archived {
    //                    //    if let shoal_core::shared::responses::ArchivedResponseAction::Get(rows) = &action.data {
    //                    //
    //                    //    }
    //                    //}
    //                    //// access our response
    //                    //let opt = response.access::<Movie>().unwrap();
    //                    //let vec = opt.as_ref().unwrap();
    //                    //let archived = vec.first().unwrap();
    //                    //// make sure our movie data matches
    //                    //if movie.title == archived.title {
    //                    //    panic!("{} has invalid data - {:#?}", movie.title, archived.title);
    //                    //}
    //                }
    //                //// this query will only ever return a single row
    //                //match stream.next_typed_first::<Movie>().await.unwrap() {
    //                //    Some(Some(movie_data)) => {
    //                //        // make sure this movies data matches
    //                //        if movie != movie_data {
    //                //            panic!("{} has invalid data - {movie_data:#?}", movie.title);
    //                //        }
    //                //        //println!("verified - {}", movie.title);
    //                //    }
    //                //    _ => println!("{} is missing", movie.title),
    //                //}
    //                // stop our command benchmark for verifying
    //                self.bencher.instance_stop();
    //            }
    //            //MovieMsg::Response(response) => {
    //            //    if let Ok(Some(movie)) = response.access::<Movie>() {
    //            //        println!("got: {}", movie[0].title);
    //            //    }
    //            //}
    //            //    // this query will only ever return a single row
    //            //    match stream.next_typed_first::<Movie>().await.unwrap() {
    //            //        Some(Some(movie_data)) => {
    //            //            // make sure this movies data matches
    //            //            if movie != movie_data {
    //            //                panic!("{} has invalid data - {movie_data:#?}", movie.title);
    //            //            }
    //            //            //println!("verified - {}", movie.title);
    //            //        }
    //            //        _ => println!("{} is missing", movie.title),
    //            //    }
    //            //    // stop our command benchmark for verifying
    //            //    self.bencher.instance_stop();
    //            //}
    //            MovieMsg::Response(_) => unimplemented!("NOT NEEDED?"),
    //            MovieMsg::Shutdown => {
    //                // reemit the shutdown order
    //                self.movies_tx.send(MovieMsg::Shutdown).await.unwrap();
    //                // there is no more movies to send so shutdown this worker
    //                break;
    //            }
    //        }
    //    }
    //    //// check if we have any remaining buffered movies
    //    //if !self.buffer.is_empty() {
    //    //    // benchmark any remaining sends
    //    //    self.bencher.instance_start();
    //    //    //// send all of our remaining buffered movies
    //    //    //self.send().await;
    //    //    // stop this instance benchmark
    //    //    self.bencher.instance_stop();
    //    //}
    //    //        // check if we have any remaining buffered movies
    //    //        if !self.insert_buffer.is_empty() {
    //    //            // benchmark any remaining sends
    //    //            self.bencher.instance_start();
    //    //            // send all of our remaining buffered movies
    //    //            self.send().await;
    //    //            // stop this instance benchmark
    //    //            self.bencher.instance_stop();
    //    //        }
    //    // return our benchmark worker
    //    self.bencher
    //}

    pub async fn stream_start(mut self) -> BenchWorker {
        println!("STARTING: {}", self.id);
        // get a new stream to send results over
        let (mut stream_tx, stream_rx) = self.shoal.stream().unwrap();
        // stream any results to our workers message queue
        let handle = tokio::spawn(response_streamer(self.movies_tx.clone(), stream_rx));
        // keep looping until we have no more movies to send
        loop {
            // wait for a intent log compaction job
            let job = self.movies_rx.recv().await.unwrap();
            // handle this movie
            match job {
                MovieMsg::Insert(movie) => {
                    //// add a timer for this movie
                    //self.timers.insert(movie.id, Instant::now());
                    // insert this movie into shoal into our buffer
                    self.buffer.add_mut(movie);
                }
                MovieMsg::Verify(movie) => {
                    // add a timer for this movie
                    self.timers.insert(movie.id, Instant::now());
                    // add the query to get this movie to our query buffer
                    self.buffer.add_mut(MovieGet::new(movie.id))
                }
                MovieMsg::Response(response) => {
                    //match response
                    if let Ok(Some(movie)) = response.access::<Movie>() {
                        // get and add our get timer
                        if let Some(timer) = self.timers.remove(&movie[0].id.to_native()) {
                            // add this timer to our benchmark
                            self.bencher.add_timer(timer);
                        }
                        //println!("got: {}", movie[0].title);
                    }
                    //    // this query will only ever return a single row
                    //    match stream.next_typed_first::<Movie>().await.unwrap() {
                    //        Some(Some(movie_data)) => {
                    //            // make sure this movies data matches
                    //            if movie != movie_data {
                    //                panic!("{} has invalid data - {movie_data:#?}", movie.title);
                    //            }
                    //            //println!("verified - {}", movie.title);
                    //        }
                    //        _ => println!("{} is missing", movie.title),
                    //    }
                    //    // stop our command benchmark for verifying
                    //    self.bencher.instance_stop();
                }
                // all responses should have been processed so break
                MovieMsg::AllResponsesReceived => break,
                // all commands have been sent so this worker can shutdown once everything
                // has been processed
                MovieMsg::Shutdown(mut received) => {
                    println!("SHUTDOWN {} -> {received:?}", self.id);
                    // check if we still have any buffered queries
                    if !self.buffer.is_empty() {
                        // swap our full query buffer with a new one
                        let queries = std::mem::take(&mut self.buffer);
                        // send any remaining buffered queries
                        stream_tx.send(queries).await.unwrap();
                    }
                    // add ourselves to the shutdown received vec if we aren't already in it
                    if !received.contains(&self.id) {
                        // add ourselves to shutdown recieved list
                        received.push(self.id);
                    }
                    // if the shutdown recieve list is full then close our query stream and stop reemitting the shutdown command
                    if received.len() == 5 {
                        println!(
                            "SHUTDOWN {} -> break {received:?} @ {}",
                            self.id,
                            received.len()
                        );
                        //stream_tx.close().await.unwrap();
                        break;
                    }
                    println!("SHUTDOWN {} -> reemit {received:?}", self.id);
                    // reemit our shutdown command until all workers have recieved it
                    self.movies_tx
                        .send(MovieMsg::Shutdown(received))
                        .await
                        .unwrap()
                }
            }
            // if we have 10 movies to insert then send them to shoal
            if self.buffer.len() > 10 {
                // swap our full query buffer with a new one
                let queries = std::mem::take(&mut self.buffer);
                // send our buffered queries
                stream_tx.send(queries).await.unwrap();
            }
        }
        // we should only have responses to handle now so panic if we get any others
        loop {
            println!("PRE: {}", self.id);
            // wait for a intent log compaction job
            let job = self.movies_rx.recv().await.unwrap();
            println!("POST: {}", self.id);
            // handle this movie
            match job {
                MovieMsg::Insert(_) => panic!("insert?"),
                MovieMsg::Verify(_) => panic!("verify?"),
                MovieMsg::Response(response) => {
                    //match response
                    if let Ok(Some(movie)) = response.access::<Movie>() {
                        // get and add our get timer
                        if let Some(timer) = self.timers.remove(&movie[0].id.to_native()) {
                            // add this timer to our benchmark
                            self.bencher.add_timer(timer);
                        }
                        //println!("got: {}", movie[0].title);
                    }
                }
                // all responses should have been processed so break
                MovieMsg::AllResponsesReceived => break,
                // all commands have been sent so this worker can shutdown once everything
                // has been processed
                MovieMsg::Shutdown(mut received) => {
                    // check if we still have any buffered queries
                    if !self.buffer.is_empty() {
                        // swap our full query buffer with a new one
                        let queries = std::mem::take(&mut self.buffer);
                        // send any remaining buffered queries
                        stream_tx.send(queries).await.unwrap();
                    }
                    // add ourselves to the shutdown received vec if we aren't already in it
                    if !received.contains(&self.id) {
                        // add ourselves to shutdown recieved list
                        received.push(self.id);
                    }
                    // if the shutdown recieve list is full then close our query stream and stop reemitting the shutdown command
                    if received.len() == 5 {
                        //stream_tx.close().await.unwrap();
                        continue;
                    }
                    // reemit our shutdown command until all workers have recieved it
                    self.movies_tx
                        .send(MovieMsg::Shutdown(received))
                        .await
                        .unwrap()
                }
            }
        }
        println!("WAITING FOR RESP: {}", self.id);
        // loop and just handle responses since our
        // wait for our response streamer to exit
        handle.await.unwrap();
        // return our bench worker
        self.bencher
    }

    ///// Start streaming movies into Shoal
    //pub async fn start(mut self) -> BenchWorker {
    //    // track if we have already propagated a shutdown order
    //    let mut shutdown = false;
    //    // keep a list of timers to check
    //    let mut timers = VecDeque::with_capacity(1000);
    //    // keep looping until we have no more movies to send
    //    loop {
    //        // wait for a intent log compaction job
    //        let job = self.movies_rx.recv().await.unwrap();
    //        // handle this job
    //        match job {
    //            MovieMsg::Insert(movie) => {
    //                // add this movie to  our buffer
    //                self.insert_buffer.push(movie.clone());
    //                // check if we have 10 movies to send
    //                if self.insert_buffer.len() > 10 {
    //                    // benchmark only commands to the db
    //                    timers.push_front(self.bencher.get_timer());
    //                    // send all of our buffered movies
    //                    self.send().await;
    //                    //// stop our command benchmark for inserts
    //                    //self.bencher.instance_stop();
    //                }
    //            }
    //            MovieMsg::Verify(movie) => {
    //                // add a get query for this movie
    //                self.get_buffer.push(MovieGet::new(movie.id));
    //                // check if we have 10 movies to send
    //                if self.get_buffer.len() > 10 {
    //                    // benchmark only commands to the db
    //                    timers.push_front(self.bencher.get_timer());
    //                    // send all of our buffered movies
    //                    self.send().await;
    //                    //// stop our command benchmark for inserts
    //                    //self.bencher.instance_stop();
    //                }
    //                //// benchmark the entire verify operation
    //                //self.bencher.instance_start();
    //                //// build the query to get this movies info
    //                //let query = self.shoal.query().add(MovieGet::new(movie.id));
    //                //// start this query
    //                //let mut stream = self.shoal.send(query).await.unwrap();
    //                //// get our results
    //                //while let Some(response) = stream.next().await.unwrap() {
    //                //    // access our responses data
    //                //    // there is a double Option because a stream can end or it a get can return nothing
    //                //    if let Some(archived) = response.access::<Movie>().unwrap() {
    //                //        // make sure our movie data matches
    //                //        if movie.title != archived[0].title {
    //                //            panic!(
    //                //                "{} has invalid data - {:#?}",
    //                //                movie.title, archived[0].title
    //                //            );
    //                //        }
    //                //    }
    //                //    //let archived = response.access();
    //                //    //if let ArchivedTmdbResponseKinds::Movie(action) = archived {
    //                //    //    if let shoal_core::shared::responses::ArchivedResponseAction::Get(rows) = &action.data {
    //                //    //
    //                //    //    }
    //                //    //}
    //                //    //// access our response
    //                //    //let opt = response.access::<Movie>().unwrap();
    //                //    //let vec = opt.as_ref().unwrap();
    //                //    //let archived = vec.first().unwrap();
    //                //    //// make sure our movie data matches
    //                //    //if movie.title == archived.title {
    //                //    //    panic!("{} has invalid data - {:#?}", movie.title, archived.title);
    //                //    //}
    //                //}
    //                //// this query will only ever return a single row
    //                //match stream.next_typed_first::<Movie>().await.unwrap() {
    //                //    Some(Some(movie_data)) => {
    //                //        // make sure this movies data matches
    //                //        if movie != movie_data {
    //                //            panic!("{} has invalid data - {movie_data:#?}", movie.title);
    //                //        }
    //                //        //println!("verified - {}", movie.title);
    //                //    }
    //                //    _ => println!("{} is missing", movie.title),
    //                //}
    //                // stop our command benchmark for verifying
    //                //self.bencher.instance_stop();
    //            }
    //            // TODO this is not safe for multiple threads as responses/timers are not sticky to workers
    //            MovieMsg::Response(response) => {
    //                self.resp += 1;
    //                // pop this responses timer
    //                // THIS IS NOT SOUND
    //                if let Some(time) = timers.pop_back() {
    //                    println!(
    //                        "{} / {} GOT RESPONSE {:?}",
    //                        self.resp,
    //                        self.count,
    //                        time.elapsed()
    //                    );
    //                    // add our time
    //                    self.bencher.add_timer(time);
    //                }
    //                if let Ok(Some(archived)) = response.access::<Movie>() {
    //                    println!("FOUND? {}", archived[0].title);
    //                }
    //                // only exit if our count/responses received are the same
    //                if self.count == self.resp {
    //                    println!("BREAK0");
    //                    break;
    //                }
    //            }
    //            MovieMsg::Shutdown => {
    //                // reemit the shutdown order if we haven't already
    //                if !shutdown {
    //                    // propagate this shutdown order
    //                    self.movies_tx.send(MovieMsg::Shutdown).await.unwrap();
    //                    // set that this worker has already reemitted a shutdown order
    //                    shutdown = true;
    //                }
    //                // only exit if our count/responses received are the same
    //                if self.count == self.resp {
    //                    // there is no more movies to send or receive so shutdown this worker
    //                    println!("BREAK1");
    //                    break;
    //                }
    //            }
    //        }
    //    }
    //    //// check if we have any remaining buffered movies
    //    //if !self.buffer.is_empty() {
    //    //    // benchmark any remaining sends
    //    //    self.bencher.instance_start();
    //    //    //// send all of our remaining buffered movies
    //    //    //self.send().await;
    //    //    // stop this instance benchmark
    //    //    self.bencher.instance_stop();
    //    //}
    //    println!("EXIT LOOP: {} == {}", self.count, self.resp);
    //    // return our benchmark worker
    //    self.bencher
    //}
}

pub struct MovieController {
    /// A shoal client
    shoal: Arc<Shoal<TmdbClient>>,
    /// The channel to add movies too
    movies_tx: AsyncSender<MovieMsg<TmdbClient>>,
    /// The channel to receive movies on
    movies_rx: AsyncReceiver<MovieMsg<TmdbClient>>,
    /// Our respons streamer tasks
    response_streamers: JoinSet<()>,
    /// The tasks for this controllers workers
    tasks: JoinSet<BenchWorker>,
}

impl MovieController {
    /// Create a default movie controller
    async fn new<A: ToSocketAddrs>(addr: A) -> Self {
        // build a client for Shoal
        let shoal = Shoal::<TmdbClient>::new(addr).await.unwrap();
        // instance a large but bounded channel
        let (movies_tx, movies_rx) = kanal::unbounded_async();
        // create our controller
        MovieController {
            shoal: Arc::new(shoal),
            movies_tx,
            movies_rx,
            response_streamers: JoinSet::default(),
            tasks: JoinSet::default(),
        }
    }
}

impl MovieController {
    /// Spawn workers for this controller
    async fn spawn(&mut self, count: u8, bencher: &Bencher) {
        for i in 0..count {
            // get a new bench worker
            let bench_worker = bencher.worker(10000);
            //// make a shoal query/result stream
            //let (query_stream, result_stream) = self.shoal.stream().await.unwrap();
            // create a new worker
            let worker = MovieWorker::new(
                i,
                self.shoal.clone(),
                &self.movies_tx,
                &self.movies_rx,
                bench_worker,
            )
            .await;
            // spawn this worker
            self.tasks.spawn(worker.stream_start());
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
        ////// only upload a limited number of movies
        //let mut cap = 50;
        // skip any movies we fail to deserialize
        while let Some(Ok(movie)) = typed_reader.next().await {
            // add our movie to our channel
            self.movies_tx.send(MovieMsg::Insert(movie)).await.unwrap();
            //cap -= 1;
            //if cap == 0 {
            //    break;
            //}
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
        ////// only upload a limited number of movies
        //let mut cap = 50;
        //let mut orig_total = 0;
        // skip any movies we fail to deserialize
        while let Some(Ok(movie)) = typed_reader.next().await {
            //println!("ORIG: {} -> {}", movie.title, movie.deep_size_of());
            //orig_total += movie.deep_size_of();
            // add our movie to our channel to be verified
            self.movies_tx.send(MovieMsg::Verify(movie)).await.unwrap();
            //cap -= 1;
            //if cap == 0 {
            //    break;
            //}
        }
        //println!("ORIG TOTAL -> {}", orig_total);
    }

    /// Start streaming jobs to our workers
    pub async fn start<P: AsRef<Path>>(&mut self, path: P) {
        // loop over our reads/writes 500 times
        for i in 0..1 {
            println!("\n\n $$$$ {i} $$$$");
            // create a new bencher
            let mut bencher = Bencher::new(".benchmark", 10000);
            // spawn 5 workers
            self.spawn(5, &bencher).await;
            // upload our tmdb data
            self.upload(&path).await;
            //// emit that workers should shutdown once all movie info has been streamed to shoal
            //self.movies_tx.send(MovieMsg::Shutdown).await.unwrap();
            ///// swap our task with with a default one
            //let tasks = std::mem::take(&mut self.tasks);
            //// wait for all workers to complete
            //let insert_bench_workers = tasks.join_all().await;
            //// pop the last shutdown message
            //self.movies_rx.recv().await.unwrap();
            println!("--------------");
            //// spawn 5 workers
            //self.spawn(5, &bencher).await;
            // verify our tmdb data
            self.verify(&path).await;
            // emit that workers should shutdown once all movie info has been streamed to shoal
            self.movies_tx
                .send(MovieMsg::Shutdown(Vec::with_capacity(5)))
                .await
                .unwrap();
            // swap our task with with a default one
            let tasks = std::mem::take(&mut self.tasks);
            // wait for all workers to complete
            let verify_bench_workers = tasks.join_all().await;
            // merge our workers back into our main bencher
            //bencher.merge_workers(insert_bench_workers);
            bencher.merge_workers(verify_bench_workers);
            // log our benchmark results
            bencher.finish(false);
            // pop the last shutdown message
            self.movies_rx.recv().await.unwrap();
        }
    }
}

#[tokio::main]
async fn read_csv() {
    // start ou controller
    let mut controller = MovieController::new("127.0.0.1:12000").await;
    // sleep for 5s
    std::thread::sleep(std::time::Duration::from_secs(5));
    // start streaming movies to shoal with multiple workers
    controller
        .start("/home/mcarson/datasets/TMDB_movie_dataset_v11.csv")
        .await;
}

fn main() {
    // start Shoal
    let pool = ShoalPool::<Tmdb>::start().unwrap();
    // sleep for 5s
    std::thread::sleep(std::time::Duration::from_secs(5));
    // read and insert our csv
    read_csv();
    // wait for our db to exit
    pool.exit().unwrap();
}
