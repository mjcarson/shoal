//! A shoal example on TMDB data

use core_affinity::{set_for_current, CoreId};
use shoal::bencher::{BenchWorker, Bencher};
use shoal_core::client::{QuerySuceededOpts, Shoal, ShoalResponse, ShoalUnorderedResultStream};
use shoal_core::server::messages::QueryMetadata;
use shoal_core::server::ring::Ring;
use shoal_core::server::{Conf, ServerError};
use shoal_core::shared::queries::{
    Queries, SortedQuery, SortedUpdate, UnsortedGet, UnsortedQuery, UnsortedUpdate,
};
use shoal_core::shared::responses::{Response, ResponseActionNames};
use shoal_core::shared::traits::{
    PartitionKeySupport, QuerySupport, RkyvSupport, ShoalQuerySupport, ShoalResponseSupport,
    ShoalUnsortedTable,
};
use shoal_core::shared::traits::{ShoalDatabase, ShoalSortedTable};
use shoal_core::storage::{FileSystem, FullArchiveMap, LoaderMsg, Loaders};
use shoal_core::tables::{PersistentSortedTable, PersistentUnsortedTable};
use shoal_core::ShoalPool;
use shoal_derive::{ShoalDB, ShoalSortedTable, ShoalUnsortedTable};

use deepsize2::DeepSizeOf;
use futures::stream::StreamExt;
use glommio::TaskQueueHandle;
use gxhash::GxHasher;
use kanal::{AsyncReceiver, AsyncSender};
use mimalloc::MiMalloc;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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
    #[shoal(partition)]
    pub id: u64,
    /// The name of this move
    #[shoal(filter)]
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
    #[shoal(update)]
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

//impl PartitionKeySupport for Movie {
//    /// The partition key type for this data
//    type PartitionKey = u64;
//
//    /// The name of this table
//    fn name() -> &'static str {
//        "Movies"
//    }
//
//    /// Calculate the partition key for this row
//    fn get_partition_key(&self) -> u64 {
//        Self::get_partition_key_from_values(&self.id)
//    }
//
//    /// Calculate the partition key for this row
//    fn get_partition_key_from_values(values: &Self::PartitionKey) -> u64 {
//        // create a new hasher
//        let mut hasher = GxHasher::default();
//        // hash the first key
//        hasher.write_u64(*values);
//        // get our hash
//        hasher.finish()
//    }
//
//    /// Get the partition key for this row from an archived value
//    fn get_partition_key_from_archived_insert(intent: &<Self as Archive>::Archived) -> u64 {
//        // create a new hasher
//        let mut hasher = GxHasher::default();
//        // hash the first key
//        hasher.write_u64(intent.id.to_native());
//        // get our hash
//        hasher.finish()
//    }
//}

// ShoalUnsortedTable is now automatically derived via #[derive(ShoalUnsortedTable)]
// with #[shoal(filter)] on title and #[shoal(update)] on overview

//#[derive(Debug, Archive, Serialize)]
//#[rkyv(derive(Debug))]
//pub struct MovieGet {
//    /// The id of the movie to get
//    pub id: u64,
//    /// Any filters to apply to rows
//    pub filters: Option<String>,
//    /// The number of rows to get at most
//    pub limit: Option<usize>,
//}

//impl MovieGet {
//    ///// Create a new get query for a [`MovieRow`]
//    /////
//    ///// # Arguments
//    /////
//    ///// * `key` - The partition key string
//    //pub fn new(id: u64) -> Self {
//    //    MovieGet {
//    //        id,
//    //        filters: None,
//    //        limit: None,
//    //    }
//    //}
//
//    /// Set a filter for getting rows
//    ///
//    /// # Arguments
//    ///
//    /// * `filter` - The value to filter on
//    pub fn filter<T: Into<String>>(mut self, filter: T) -> Self {
//        // set our filter
//        self.filters = Some(filter.into());
//        self
//    }
//}

impl From<MovieGet> for TmdbQueryKinds {
    /// Build our a `QueryKind` for getting `MovieRows`
    fn from(specific: MovieGet) -> Self {
        // build our partition key
        let partition_key =
            <Movie as PartitionKeySupport>::get_partition_key_from_values(&specific.partition_key);
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

///// Delete a key value row
//#[derive(Debug, Archive, Serialize)]
//#[rkyv(derive(Debug))]
//pub struct MovieDelete {
//    /// Tkey key to the partition to delete a row from
//    pub partition_key: u64,
//}

//impl MovieDelete {
//    /// Create a new key value delete
//    ///
//    /// # Arguments
//    ///
//    /// * `key` - The key for determining the parititon
//    pub fn new(key: u64) -> Self {
//        // calculate our partition key
//        let partition_key = Movie::get_partition_key_from_values(&key);
//        // build a key value delete object
//        MovieDelete { partition_key }
//    }
//}

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

///// An update to apply to a row in this table
//pub struct MovieUpdate {
//    /// The partition key to update data in
//    partition_key: u64,
//    /// The data to update
//    data: String,
//}
//
//impl MovieUpdate {
//    /// Create an update for a specific row
//    ///
//    /// # Arguments
//    ///
//    /// * `key` - The key to the partition to upate data in
//    /// * `data` - The new data to set
//    pub fn new<D: Into<String>>(key: u64, data: D) -> Self {
//        // calculate our partition key
//        let partition_key = Movie::get_partition_key_from_values(&key);
//        // build a new upidate object
//        MovieUpdate {
//            partition_key,
//            data: data.into(),
//        }
//    }
//}

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
            update: specific,
        };
        // wrap our general update in a query
        let query = UnsortedQuery::Update(general);
        // wrap our general update in a table specific query
        TmdbQueryKinds::Movie(query)
    }
}

#[derive(
    Debug,
    Archive,
    Serialize,
    Deserialize,
    Clone,
    ShoalSortedTable,
    serde::Deserialize,
    serde::Serialize,
    PartialEq,
    DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Tmdb")]
pub struct MovieByKeyword {
    /// The keyword for this movie
    pub keyword: String,
    /// The name of this movie
    pub title: String,
}

impl PartitionKeySupport for MovieByKeyword {
    /// The partition key type for this data
    type PartitionKey = String;

    /// The name of this table
    fn name() -> &'static str {
        "MovieByKeyword"
    }

    /// Calculate the partition key for this row
    fn get_partition_key(&self) -> u64 {
        Self::get_partition_key_from_values(&self.keyword)
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
        hasher.write(intent.keyword.as_bytes());
        // get our hash
        hasher.finish()
    }
}

impl ShoalSortedTable for MovieByKeyword {
    /// The updates that can be applied to this table
    type Update = String;

    /// The sort type for this data
    type Sort = String;

    /// Build the sort tuple for this row
    fn get_sort(&self) -> &Self::Sort {
        &self.title
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
    fn update(&mut self, update: &SortedUpdate<Self>) {
        ()
    }
}

/// The different tables we can query
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub enum TmdbQueryKinds {
    /// A query for the key/value table
    Movie(UnsortedQuery<Movie>),
    /// A query for the movie by keyword table
    MovieByKeyword(SortedQuery<MovieByKeyword>),
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
            ArchivedTmdbResponseKinds::MovieByKeyword(resp) => Ok(&resp.id),
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
            TmdbQueryKinds::MovieByKeyword(query) => {
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
    MovieByKeyword(Response<MovieByKeyword>),
}

impl RkyvSupport for TmdbResponseKinds {}

impl ShoalResponseSupport for TmdbResponseKinds {
    /// Get the index of a single [`Self::ResponseKinds`]
    fn get_index_archived(archived: &<Self as Archive>::Archived) -> usize {
        // get our response index
        match archived {
            // TODO fix this usize conversion
            ArchivedTmdbResponseKinds::Movie(resp) => resp.index.to_native() as usize,
            ArchivedTmdbResponseKinds::MovieByKeyword(resp) => resp.index.to_native() as usize,
        }
    }

    /// Get whether this is the last response in a response stream
    fn is_end_of_stream(archived: &<Self as Archive>::Archived) -> bool {
        // check if this is the end of the stream
        match archived {
            ArchivedTmdbResponseKinds::Movie(resp) => resp.end,
            ArchivedTmdbResponseKinds::MovieByKeyword(resp) => resp.end,
        }
    }

    /// Get the query id from the response
    fn get_query_id(archived: &<Self as Archive>::Archived) -> Uuid {
        // get our response query id
        match archived {
            ArchivedTmdbResponseKinds::Movie(resp) => resp.id.to_owned(),
            ArchivedTmdbResponseKinds::MovieByKeyword(resp) => resp.id.to_owned(),
        }
    }
}

/// The tables we are adding to to shoal
#[derive(ShoalDB)]
//#[shoal_db(name = "Basic")]
pub struct Tmdb {
    /// A basic key value table
    pub movie: PersistentUnsortedTable<Movie, FileSystem, TmdbTableNames>,
    /// A sorted table of movies by keywords
    pub movie_by_keyword: PersistentSortedTable<MovieByKeyword, FileSystem, TmdbTableNames>,
}

pub enum MovieMsg {
    /// Insert a movie into shoal
    Insert(Movie),
    /// Verify a movies data in shoal
    Verify(Movie),
    /// Shutdown this worker
    Shutdown,
}

/// The messages from a response streamer to a worker
pub enum WorkerMsg<Q: QuerySupport> {
    /// A Response from a shoal query
    Response(ShoalResponse<Q>),
    /// All responses have been received for this worker
    AllResponsesReceived,
}

pub async fn response_streamer(
    movies_tx: AsyncSender<WorkerMsg<TmdbClient>>,
    mut response_stream: ShoalUnorderedResultStream<TmdbClient>,
) {
    // keep getting responses until this stream closes
    while let Some(response) = response_stream.next().await.unwrap() {
        // wrap our response in a MovieMsg
        let wrapped = WorkerMsg::Response(response);
        // send this response to our main worker
        movies_tx.send(wrapped).await.unwrap();
    }
    // All responses have been recieved for our worker
    movies_tx
        .send(WorkerMsg::AllResponsesReceived)
        .await
        .unwrap();
}

pub struct MovieWorker {
    /// The id for this worker
    id: u8,
    /// A shoal client
    shoal: Arc<Shoal<TmdbClient>>,
    /// The channel to add movies too
    movies_tx: AsyncSender<MovieMsg>,
    /// The channel to receive movies on
    movies_rx: AsyncReceiver<MovieMsg>,
    /// Buffered queries to send to shoal
    buffer: Queries<TmdbClient>,
    /// The benchmark worker for this worker
    bencher: BenchWorker,
    /// A map of timers for benchmarking
    timers: HashMap<usize, Instant>,
    /// Count the number of rows inserted
    inserted: Arc<AtomicUsize>,
    /// Count the number of rows retrieved
    retrieved: Arc<AtomicUsize>,
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
        movies_tx: &AsyncSender<MovieMsg>,
        movies_rx: &AsyncReceiver<MovieMsg>,
        bencher: BenchWorker,
        inserted: &Arc<AtomicUsize>,
        retrieved: &Arc<AtomicUsize>,
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
            inserted: inserted.clone(),
            retrieved: retrieved.clone(),
        }
    }

    fn verify_response(&mut self, response: ShoalResponse<TmdbClient>) {
        // get this responses index
        let index = response.get_index();
        // get and add our get timer
        if let Some(timer) = self.timers.remove(&index) {
            // add this timer to our benchmark
            self.bencher.add_timer(timer);
        }
        // get the kind of query that we are verifying
        let kind = response.kind();
        // check if this query failed or not
        match response.suceeded(QuerySuceededOpts::default()) {
            Ok(()) => match kind {
                ResponseActionNames::Insert => {
                    self.inserted.fetch_add(1, Ordering::SeqCst);
                }
                ResponseActionNames::Get => {
                    // get the movie info from this query
                    match response.access::<Movie>().unwrap() {
                        // increment our movie count
                        Some(movies) => {
                            self.retrieved.fetch_add(movies.len(), Ordering::SeqCst);
                        }
                        None => println!("Missing movie!"),
                    }
                }
                _ => (),
            },
            Err(error) => panic!("Error: {error:#?}"),
        }
    }

    pub async fn stream_start(mut self) -> BenchWorker {
        // get a new stream to send results over
        let (mut stream_tx, stream_rx) = self.shoal.stream_unordered().unwrap();
        // create a queue just for this worker
        let (worker_tx, worker_rx) = kanal::unbounded_async();
        // stream any results to our workers message queue
        let handle = tokio::spawn(response_streamer(worker_tx, stream_rx));
        // track how many requests are currently in flight
        let mut in_flight = 0;
        // keep looping until we have no more movies to send
        'outer: loop {
            // first check for any messages from our response streamer
            loop {
                match worker_rx.try_recv().unwrap() {
                    Some(WorkerMsg::Response(response)) => {
                        // decrement our in_flight count
                        in_flight -= 1;
                        self.verify_response(response)
                    }
                    // all responses should have been processed so break
                    Some(WorkerMsg::AllResponsesReceived) => break 'outer,
                    // nothing from our response streamer yet
                    None => break,
                }
            }
            // if we have more then 10 in flight requests then wait for one to complete
            if in_flight >= 10 {
                // wait for a response from any currently in flight_queries
                match worker_rx.recv().await.unwrap() {
                    WorkerMsg::Response(response) => {
                        // decrement our in_flight count
                        in_flight -= 1;
                        // verify this movie
                        self.verify_response(response);
                        // restart our loop from the top
                        continue;
                    }
                    // all responses should have been processed so break
                    WorkerMsg::AllResponsesReceived => break 'outer,
                }
            }
            // wait for a intent log compaction job
            let job = self.movies_rx.recv().await.unwrap();
            // handle this movie
            match job {
                // insert this movie into shoal into our buffer
                MovieMsg::Insert(movie) => self.buffer.add_mut(movie),
                // add the query to get this movie to our query buffer
                MovieMsg::Verify(movie) => self.buffer.add_mut(MovieGet::new(movie.id)),
                // all commands have been sent so this worker can shutdown once everything
                // has been processed
                MovieMsg::Shutdown => {
                    // check if we still have any buffered queries
                    if !self.buffer.is_empty() {
                        // swap our full query buffer with a new one
                        let queries = std::mem::take(&mut self.buffer);
                        // send any remaining buffered queries
                        stream_tx.send(queries).await.unwrap();
                    }
                    // emit this shutdown order for our other workers
                    self.movies_tx.send(MovieMsg::Shutdown).await.unwrap();
                    // shutdown our query stream
                    stream_tx.close().await.unwrap();
                    // we only need to process worker responses now
                    break;
                }
            }
            // if we have 10 movies to insert or get then send them to shoal
            if self.buffer.len() > 10 {
                // swap our full query buffer with a new one
                let queries = std::mem::take(&mut self.buffer);
                // get the current time
                let timer = Instant::now();
                // get our current query index
                let mut index = stream_tx.base_index;
                // setup timers for all of our movies
                for _ in &queries.queries {
                    // add a timer for this movie
                    self.timers.insert(index, timer);
                    index += 1;
                }
                // increment our in flight query count
                in_flight += queries.queries.len();
                // send our buffered queries
                stream_tx.send(queries).await.unwrap();
            }
        }
        // keep processing our worker responses until there are no more
        loop {
            // first check for any messages from our response streamer
            match worker_rx.recv().await.unwrap() {
                // handle this response
                WorkerMsg::Response(response) => self.verify_response(response),
                // all responses should have been processed so break
                WorkerMsg::AllResponsesReceived => break,
            }
        }
        // loop and just handle responses since our
        // wait for our response streamer to exit
        handle.await.unwrap();
        // return our bench worker
        self.bencher
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
    tasks: JoinSet<BenchWorker>,
    /// Count the number of rows inserted
    inserted: Arc<AtomicUsize>,
    /// Count the number of rows retrieved
    retrieved: Arc<AtomicUsize>,
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
            tasks: JoinSet::default(),
            inserted: Arc::new(AtomicUsize::new(0)),
            retrieved: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl MovieController {
    /// Spawn workers for this controller
    async fn spawn(&mut self, count: u8, bencher: &Bencher) {
        for i in 0..count {
            // get a new bench worker
            let bench_worker = bencher.worker(10000);
            // create a new worker
            let worker = MovieWorker::new(
                i,
                self.shoal.clone(),
                &self.movies_tx,
                &self.movies_rx,
                bench_worker,
                &self.inserted,
                &self.retrieved,
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
        // only upload a limited number of movies
        //let mut cap = 100;
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
        // only upload a limited number of movies
        //let mut cap = 100;
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
            println!("--------------");
            // verify our tmdb data
            self.verify(&path).await;
            println!("DONE?");
            // emit that workers should shutdown once all movie info has been streamed to shoal
            self.movies_tx.send(MovieMsg::Shutdown).await.unwrap();
            // swap our task with with a default one
            let tasks = std::mem::take(&mut self.tasks);
            // wait for all workers to complete
            let bench_workers = tasks.join_all().await;
            // merge our workers back into our main bencher
            bencher.merge_workers(bench_workers);
            // log our benchmark results
            bencher.finish(false);
            // pop the last shutdown message
            self.movies_rx.recv().await.unwrap();
            // print how many movies were inserted/retrieved
            println!("Inserted: {}", self.inserted.load(Ordering::Relaxed));
            println!("Retrieved: {}", self.retrieved.load(Ordering::Relaxed));
        }
    }

    /// Shutdown our controller and its workers
    async fn close(mut self) {
        println!("CLOSING {} tasks", self.tasks.len());
        while let Some(Err(error)) = self.tasks.join_next().await {
            println!("ERROR: {error:#?}");
        }
    }
}

async fn read_csv() {
    // start ou controller
    let mut controller = MovieController::new("127.0.0.1:12000").await;
    // sleep for 5s
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    // start streaming movies to shoal with multiple workers
    controller
        .start("/home/mcarson/datasets/TMDB_movie_dataset_v11_first_100k.csv")
        .await;
    // shutdown our controller
    controller.close().await;
}

#[hotpath::main]
fn main() {
    // load our config
    let conf = Conf::new("shoal.yml").expect("Failed to load config");
    // setup tracing/telemetry
    let provider = shoal_core::server::trace::setup(&conf);
    // start Shoal
    let pool = ShoalPool::<Tmdb>::start(conf).unwrap();
    // sleep for 5s
    std::thread::sleep(std::time::Duration::from_secs(5));
    // Reserve specific cores for tokio (e.g., cores 28-32)
    // Make sure these don't overlap with Shoal's cores (which uses 1-N)
    let tokio_cores: Vec<CoreId> = vec![
        CoreId { id: 28 },
        CoreId { id: 29 },
        CoreId { id: 30 },
        CoreId { id: 31 },
    ];
    // build a runtime that is pinned to specific cores
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("tokio-worker")
        .enable_all()
        .on_thread_start(move || {
            static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

            let idx = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let core = tokio_cores[idx % tokio_cores.len()];

            if !set_for_current(core) {
                eprintln!("Failed to set affinity for tokio worker {}", idx);
            } else {
                println!("Tokio worker {} pinned to core {}", idx, core.id);
            }
        })
        .build()
        .unwrap();
    // read and insert our csv
    runtime.block_on(read_csv());
    // wait for our db to exit
    pool.exit().unwrap();
    // shutdown our tracer
    shoal_core::server::trace::shutdown(provider);
}
