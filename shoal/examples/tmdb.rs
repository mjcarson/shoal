//! A shoal example on TMDB data

use clap::Parser;
use core_affinity::{set_for_current, CoreId};
use shoal::bencher::{BenchOp, BenchWorker, Bencher};
use shoal::client::{QuerySuceededOpts, ShoalQueryStream, ShoalUnorderedResultStream};
use shoal::shared::queries::Queries;
use shoal::shared::responses::ResponseActionNames;
use shoal::shared::traits::QuerySupport;
use shoal::{
    Conf, FileSystem, PersistentSortedTable, PersistentUnsortedTable, Shoal, ShoalPool,
    ShoalResponse, ShoalSortedTable, ShoalUnsortedTable,
};

use deepsize2::DeepSizeOf;
use futures::stream::StreamExt;
use kanal::{AsyncReceiver, AsyncSender};
use mimalloc::MiMalloc;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::fs::File;
use tokio::net::ToSocketAddrs;
use tokio::task::JoinSet;
use tokio::time::Instant;

/// Which phase of the benchmark we are streaming rows for
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Insert every row of the dataset
    Insert,
    /// Read back every row of the dataset
    Verify,
}

/// Benchmark Shoal against the TMDB dataset
///
/// See `docs/src/operations/benchmarking.md` for how to get a comparable result.
#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
pub struct Args {
    /// The number of client workers to run
    #[clap(long, default_value_t = 5)]
    pub workers: u8,
    /// The number of queries to buffer before sending a batch
    #[clap(long, default_value_t = 100)]
    pub batch: usize,
    /// The maximum queries each worker may have in flight at once
    ///
    /// This must be well above `--batch` or the worker drains its pipeline between
    /// batches and the measurement reports that stall as server latency.
    #[clap(long, default_value_t = 4096)]
    pub in_flight: usize,
    /// The number of times to repeat the upload and verify cycle
    #[clap(long, default_value_t = 1)]
    pub iterations: usize,
    /// The TMDB csv dataset to load
    #[clap(
        long,
        default_value = "/home/mcarson/datasets/TMDB_movie_dataset_v11_first_100k.csv"
    )]
    pub dataset: PathBuf,
    /// Only load this many rows from the dataset
    #[clap(long)]
    pub limit: Option<usize>,
    /// The baseline file to compare this run against
    #[clap(long, default_value = ".benchmark")]
    pub baseline: PathBuf,
    /// Record this run as the new baseline
    #[clap(long, default_value_t = false)]
    pub write_baseline: bool,
    /// The shoal config to start the server with
    #[clap(long, default_value = "shoal.yml")]
    pub conf: PathBuf,
    /// The address to connect to
    #[clap(long, default_value = "127.0.0.1:12000")]
    pub addr: String,
    /// The cores to pin the client's tokio workers to
    ///
    /// These must not share a physical core with any shard. On an SMT part the
    /// sibling of a busy core is not a free core.
    #[clap(long, value_delimiter = ',', default_values_t = [28usize, 29, 30, 31])]
    pub client_cores: Vec<usize>,
    /// Exit when the benchmark finishes instead of waiting for a newline
    #[clap(long, default_value_t = false)]
    pub no_wait: bool,
}

impl Args {
    /// Check that these arguments can produce a meaningful measurement
    fn validate(&self) -> Result<(), String> {
        // a worker blocks once it hits its in flight limit and cannot buffer the next
        // batch until it drops back under, so a limit near the batch size empties the
        // pipeline every cycle and we end up timing our own stalls
        if self.in_flight <= self.batch * 4 {
            return Err(format!(
                "--in-flight ({}) must be more than 4x --batch ({}), otherwise the client \
                 drains its pipeline between batches and measures its own stalls",
                self.in_flight, self.batch
            ));
        }
        // we need at least one worker to send anything
        if self.workers == 0 {
            return Err("--workers must be at least 1".to_string());
        }
        // a batch of nothing never gets sent
        if self.batch == 0 {
            return Err("--batch must be at least 1".to_string());
        }
        Ok(())
    }
}

/// Deserialize a comma-space separated string into a Vec<String>
fn deserialize_comma_separated<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    if s.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(s.split(", ").map(|s| s.to_string()).collect())
    }
}

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
    #[serde(deserialize_with = "deserialize_comma_separated")]
    pub genres: Vec<String>,
    /// The production companies for this movie
    #[serde(deserialize_with = "deserialize_comma_separated")]
    pub production_companies: Vec<String>,
    /// The countries this movie was produced in
    #[serde(deserialize_with = "deserialize_comma_separated")]
    pub production_countries: Vec<String>,
    /// The languages spoken in this movie
    #[serde(deserialize_with = "deserialize_comma_separated")]
    pub spoken_languages: Vec<String>,
    /// The keywords for this movie
    #[serde(deserialize_with = "deserialize_comma_separated")]
    pub keywords: Vec<String>,
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
    #[shoal(partition)]
    pub keyword: String,
    /// The name of this movie
    #[shoal(sort)]
    pub title: String,
}

/// The tables we are adding to to shoal
#[shoal::db]
pub struct Tmdb {
    /// A basic key value table
    pub movie: PersistentUnsortedTable<Movie, FileSystem>,
    /// A sorted table of movies by keywords
    pub movie_by_keyword: PersistentSortedTable<MovieByKeyword, FileSystem>,
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
    /// The number of queries to buffer before sending a batch
    batch: usize,
    /// The maximum queries this worker may have in flight at once
    max_in_flight: usize,
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
    /// * `args` - The benchmark settings for this run
    /// * `inserted` - The shared count of inserted rows
    /// * `retrieved` - The shared count of retrieved rows
    pub async fn new(
        id: u8,
        shoal: Arc<Shoal<TmdbClient>>,
        movies_tx: &AsyncSender<MovieMsg>,
        movies_rx: &AsyncReceiver<MovieMsg>,
        bencher: BenchWorker,
        args: &Args,
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
            // size this to hold every query we can have outstanding at once
            timers: HashMap::with_capacity(args.in_flight),
            batch: args.batch,
            max_in_flight: args.in_flight,
            inserted: inserted.clone(),
            retrieved: retrieved.clone(),
        }
    }

    fn verify_response(&mut self, response: ShoalResponse<TmdbClient>) {
        // get this responses index
        let index = response.get_index();
        // get the kind of query that we are verifying
        let kind = response.kind();
        // record how long this query took against the operation it was
        //
        // inserts and gets are tracked separately because they are different
        // operations, and a percentile over both pooled together just reports
        // where the boundary between the two distributions falls
        if let Some(timer) = self.timers.remove(&index) {
            // work out which distribution this sample belongs in
            let op = match kind {
                ResponseActionNames::Insert => Some(BenchOp::Insert),
                ResponseActionNames::Get => Some(BenchOp::Get),
                // we only time the two operations this benchmark drives
                _ => None,
            };
            // add this timer to our benchmark
            if let Some(op) = op {
                self.bencher.add_timer(op, timer);
            }
        }
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

    /// Send our buffered queries to shoal and start timing them
    ///
    /// Returns how many queries were sent so the caller can track them as in flight.
    /// Every batch goes through here, including the last one, so the tail of a run
    /// is measured like the rest of it.
    ///
    /// # Arguments
    ///
    /// * `stream_tx` - The query stream to send our batch on
    async fn send_batch(&mut self, stream_tx: &mut ShoalQueryStream<TmdbClient>) -> usize {
        // swap our full query buffer with a new one
        let queries = std::mem::take(&mut self.buffer);
        // get how many queries we are about to send
        let sent = queries.queries.len();
        // get the current time
        //
        // this is one timestamp for the whole batch, so every query in it is charged
        // for the ones ahead of it. see the benchmarking docs before reading too much
        // into an individual percentile
        let timer = Instant::now();
        // get our current query index
        let mut index = stream_tx.base_index;
        // setup timers for all of our movies
        for _ in &queries.queries {
            // add a timer for this movie
            self.timers.insert(index, timer);
            index += 1;
        }
        // send our buffered queries
        stream_tx.send(queries).await.unwrap();
        sent
    }

    pub async fn stream_start(mut self) -> BenchWorker {
        // get a new stream to send results over
        let (mut stream_tx, stream_rx) = self.shoal.stream_unordered().unwrap();
        // create a queue just for this worker
        let (worker_tx, worker_rx) = kanal::unbounded_async();
        // stream any results to our workers message queue
        let handle = tokio::spawn(response_streamer(worker_tx, stream_rx));
        // track how many queries are currently in flight
        let mut in_flight: usize = 0;
        // keep looping until we have no more movies to send
        'outer: loop {
            // first check for any messages from our response streamer
            loop {
                match worker_rx.try_recv().unwrap() {
                    Some(WorkerMsg::Response(response)) => {
                        // decrement our in_flight count
                        // saturate rather than wrap: a query that yields more than
                        // one response would otherwise underflow this to usize::MAX
                        // and permanently satisfy the gate below, deadlocking us
                        in_flight = in_flight.saturating_sub(1);
                        self.verify_response(response)
                    }
                    // all responses should have been processed so break
                    Some(WorkerMsg::AllResponsesReceived) => break 'outer,
                    // nothing from our response streamer yet
                    None => break,
                }
            }
            // if we are at our in flight limit then wait for a query to complete
            //
            // this is a high water mark, not a drain: we resume as soon as we are
            // one query under the limit, so the pipeline stays full. gating near the
            // batch size instead would empty it every cycle and the idle time would
            // show up as server latency
            if in_flight >= self.max_in_flight {
                // wait for a response from any currently in flight_queries
                match worker_rx.recv().await.unwrap() {
                    WorkerMsg::Response(response) => {
                        // decrement our in_flight count
                        // saturate rather than wrap: a query that yields more than
                        // one response would otherwise underflow this to usize::MAX
                        // and permanently satisfy the gate below, deadlocking us
                        in_flight = in_flight.saturating_sub(1);
                        // verify this movie
                        self.verify_response(response);
                        // restart our loop from the top
                        continue;
                    }
                    // all responses should have been processed so break
                    WorkerMsg::AllResponsesReceived => break 'outer,
                }
            }
            // try to claim the next job without blocking
            //
            // parking on this channel would stop us draining responses entirely, so
            // when the producer runs dry we fall back to whichever channel can still
            // make progress instead
            let job = match self.movies_rx.try_recv().unwrap() {
                // we claimed a job so handle it below
                Some(job) => job,
                // no work is queued for us right now
                None => {
                    // flush any partial batch rather than stranding it until shutdown
                    if !self.buffer.is_empty() {
                        in_flight += self.send_batch(&mut stream_tx).await;
                        continue;
                    }
                    // nothing is buffered, so drain responses while we have any outstanding
                    if in_flight > 0 {
                        match worker_rx.recv().await.unwrap() {
                            WorkerMsg::Response(response) => {
                                // decrement our in_flight count
                                in_flight = in_flight.saturating_sub(1);
                                // verify this response
                                self.verify_response(response);
                                // restart our loop from the top
                                continue;
                            }
                            // all responses should have been processed so break
                            WorkerMsg::AllResponsesReceived => break 'outer,
                        }
                    }
                    // nothing buffered and nothing outstanding so its safe to park here
                    self.movies_rx.recv().await.unwrap()
                }
            };
            // handle this movie
            match job {
                // insert this movie into shoal into our buffer
                MovieMsg::Insert(movie) => {
                    // add the keyword inserts to our query buffer
                    for keyword in &movie.keywords {
                        // build the movie by keyword row to inserts
                        let by_keyword = MovieByKeyword {
                            title: movie.title.clone(),
                            keyword: keyword.clone(),
                        };
                        // add this insert to our buffer
                        self.buffer.add_mut(by_keyword);
                    }
                    // add the full movie to our query buffer
                    self.buffer.add_mut(movie)
                }
                // add the query to get this movie to our query buffer
                MovieMsg::Verify(movie) => self.buffer.add_mut(MovieGet::new(movie.id)),
                // all commands have been sent so this worker can shutdown once everything
                // has been processed
                MovieMsg::Shutdown => {
                    // send any remaining buffered queries
                    //
                    // this goes through the same helper as every other batch so the
                    // tail is timed like the rest of the run. we don't track these as
                    // in flight because we stop gating on that the moment we break out
                    // and just drain whatever is left
                    if !self.buffer.is_empty() {
                        self.send_batch(&mut stream_tx).await;
                    }
                    // emit this shutdown order for our other workers
                    self.movies_tx.send(MovieMsg::Shutdown).await.unwrap();
                    // shutdown our query stream
                    stream_tx.close().await.unwrap();
                    // we only need to process worker responses now
                    break;
                }
            }
            // once we have buffered a full batch send it to shoal
            //
            // one movie can add several queries because of MovieByKeyword fan out,
            // so a batch is at least this many queries and usually more
            if self.buffer.len() >= self.batch {
                in_flight += self.send_batch(&mut stream_tx).await;
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
    ///
    /// # Arguments
    ///
    /// * `bencher` - The bencher to build worker collectors from
    /// * `args` - The benchmark settings for this run
    async fn spawn(&mut self, bencher: &Bencher, args: &Args) {
        for i in 0..args.workers {
            // get a new bench worker
            let bench_worker = bencher.worker(10000);
            // create a new worker
            let worker = MovieWorker::new(
                i,
                self.shoal.clone(),
                &self.movies_tx,
                &self.movies_rx,
                bench_worker,
                args,
                &self.inserted,
                &self.retrieved,
            )
            .await;
            // spawn this worker
            self.tasks.spawn(worker.stream_start());
        }
    }

    /// Upload data to shoal
    ///
    /// # Arguments
    ///
    /// * `path` - The dataset to read movies from
    /// * `limit` - Only upload this many movies if set
    async fn upload<P: AsRef<Path>>(&mut self, path: P, limit: Option<usize>) -> usize {
        // stream our dataset into our workers as insert jobs
        self.stream_dataset(path, limit, Phase::Insert).await
    }

    /// verify data in shoal
    ///
    /// # Arguments
    ///
    /// * `path` - The dataset to read movies from
    /// * `limit` - Only verify this many movies if set
    async fn verify<P: AsRef<Path>>(&mut self, path: P, limit: Option<usize>) -> usize {
        // stream our dataset into our workers as verify jobs
        self.stream_dataset(path, limit, Phase::Verify).await
    }

    /// Stream our dataset into our workers as jobs
    ///
    /// # Arguments
    ///
    /// * `path` - The dataset to read movies from
    /// * `limit` - Only stream this many movies if set
    /// * `phase` - Which phase of the benchmark we are streaming for
    async fn stream_dataset<P: AsRef<Path>>(
        &mut self,
        path: P,
        limit: Option<usize>,
        phase: Phase,
    ) -> usize {
        // open a handle to our tmdb dataset
        let file = File::open(path).await.unwrap();
        // wrap our file in a csv reader
        let mut reader = csv_async::AsyncDeserializer::from_reader(file);
        // set the type we are going to deserialize
        let mut typed_reader = reader.deserialize::<Movie>();
        // track how many movies we have streamed so we can honor our limit
        let mut streamed = 0;
        // track how many responses these jobs should produce, so our caller can wait
        // for the phase to finish before starting the next one
        let mut expected = 0;
        // read movies until we run out or hit our limit
        while let Some(row) = typed_reader.next().await {
            // stop at the first row we cannot deserialize
            //
            // this truncates the run rather than skipping the row, so a dataset with
            // a bad row part way through silently benchmarks only its clean prefix
            let movie = match row {
                Ok(movie) => movie,
                Err(error) => {
                    eprintln!("warning: stopping at unreadable csv row {streamed}: {error}");
                    break;
                }
            };
            // work out how many responses this movie will produce and wrap it as a job
            let job = match phase {
                // an insert fans out into one row per keyword plus the movie itself
                Phase::Insert => {
                    expected += movie.keywords.len() + 1;
                    MovieMsg::Insert(movie)
                }
                // a verify is a single get for the movie
                Phase::Verify => {
                    expected += 1;
                    MovieMsg::Verify(movie)
                }
            };
            // add our movie to our channel
            self.movies_tx.send(job).await.unwrap();
            streamed += 1;
            // stop once we have streamed as many movies as we were asked for
            if Some(streamed) == limit {
                break;
            }
        }
        expected
    }

    /// Wait for a counter to reach a target before moving on to the next phase
    ///
    /// `upload` only queues work, it does not wait for it. Without this barrier a
    /// worker can send a verify for a movie that another worker has not inserted
    /// yet, and that get correctly reports no match. The old in flight limit hid
    /// this by draining the pipeline after every batch, which is not something a
    /// benchmark should rely on for correctness.
    ///
    /// # Arguments
    ///
    /// * `counter` - The counter to watch
    /// * `target` - The value to wait for
    /// * `label` - What we are waiting on, for the timeout message
    async fn await_phase(counter: &AtomicUsize, target: usize, label: &str) {
        // remember where we started so we can tell whether we are still making progress
        let mut last = counter.load(Ordering::SeqCst);
        let mut stalled = 0;
        // wait for our counter to catch up to our target
        while counter.load(Ordering::SeqCst) < target {
            // give our workers a moment to make progress
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            // check whether anything moved
            let current = counter.load(Ordering::SeqCst);
            if current == last {
                stalled += 1;
                // bail out rather than hanging forever if we stop making progress
                if stalled > 30_000 {
                    panic!(
                        "timed out waiting for {label}: stuck at {current}/{target}, \
                         a response was probably dropped"
                    );
                }
            } else {
                last = current;
                stalled = 0;
            }
        }
    }

    /// Start streaming jobs to our workers
    /// Start streaming jobs to our workers
    ///
    /// # Arguments
    ///
    /// * `args` - The benchmark settings for this run
    pub async fn start(&mut self, args: &Args) {
        // loop over our reads/writes as many times as we were asked to
        for i in 0..args.iterations {
            println!("\n\n $$$$ {i} $$$$");
            // create a new bencher
            let mut bencher = Bencher::new(&args.baseline, 10000);
            // spawn our workers
            self.spawn(&bencher, args).await;
            // note where our counters are so we can wait for just this iteration
            let inserted_before = self.inserted.load(Ordering::SeqCst);
            // upload our tmdb data
            let expected_inserts = self.upload(&args.dataset, args.limit).await;
            // wait for every insert to be acknowledged before we read anything back
            Self::await_phase(
                &self.inserted,
                inserted_before + expected_inserts,
                "inserts",
            )
            .await;
            println!("--------------");
            // verify our tmdb data
            self.verify(&args.dataset, args.limit).await;
            println!("DONE?");
            // emit that workers should shutdown once all movie info has been streamed to shoal
            self.movies_tx.send(MovieMsg::Shutdown).await.unwrap();
            // swap our task with with a default one
            let tasks = std::mem::take(&mut self.tasks);
            // wait for all workers to complete
            let bench_workers = tasks.join_all().await;
            // merge our workers back into our main bencher
            bencher.merge_workers(bench_workers);
            // log our benchmark results, recording a new baseline if asked to
            bencher.finish(args.write_baseline);
            // pop the last shutdown message
            self.movies_rx.recv().await.unwrap();
            // print how many movies were inserted/retrieved
            println!("Inserted: {}", self.inserted.load(Ordering::Relaxed));
            println!("Retrieved: {}", self.retrieved.load(Ordering::Relaxed));
        }
        // query this db manually
        let query = self
            .shoal
            .query()
            .parse("select * from MovieByKeyword where keyword = 'alien'")
            .unwrap();
        // try to execute this query
        let mut response = self.shoal.send(query).await.unwrap();
        // keep getting rows in response
        while let Some(row) = response.next().await.unwrap() {
            // access this rows data
            match row.access::<MovieByKeyword>().unwrap() {
                Some(row) => (), //println!("row: {row:#?}"),
                None => println!("missing row?"),
            }
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

async fn read_csv(args: Args) {
    // start ou controller
    let mut controller = MovieController::new(args.addr.as_str()).await;
    // sleep for 5s
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    // start streaming movies to shoal with multiple workers
    controller.start(&args).await;
    // shutdown our controller
    controller.close().await;
}

#[hotpath::main]
fn main() {
    // parse our benchmark settings
    let args = Args::parse();
    // make sure these settings can produce a meaningful measurement
    if let Err(error) = args.validate() {
        eprintln!("error: {error}");
        std::process::exit(2);
    }
    // load our config
    let conf = Conf::from_file(args.conf.to_str().expect("config path is not valid utf8"))
        .expect("Failed to load config");
    println!("conf -> {conf:#?}");
    // setup tracing/telemetry
    let provider = shoal_core::server::trace::setup(&conf);
    // start Shoal
    let pool = ShoalPool::<Tmdb>::start(conf).unwrap();
    // sleep for 5s
    std::thread::sleep(std::time::Duration::from_secs(5));
    // Reserve specific cores for the client's tokio runtime
    //
    // These must not share a *physical* core with any shard. On an SMT part the
    // sibling of a busy core is not a free core, and shoal's `exclude_cores` filters
    // on the physical core id, so excluding one there frees both of its threads.
    // See docs/src/operations/benchmarking.md.
    let tokio_cores: Vec<CoreId> = args
        .client_cores
        .iter()
        .map(|id| CoreId { id: *id })
        .collect();
    // build a runtime that is pinned to specific cores
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(tokio_cores.len())
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
    runtime.block_on(read_csv(args.clone()));
    // wait for input before exiting unless we were told not to
    //
    // looping the benchmark from a script needs --no-wait, otherwise the first run
    // blocks here forever waiting on a newline
    if !args.no_wait {
        let mut input_text = String::new();
        std::io::stdin()
            .read_line(&mut input_text) // `read_line` returns a `Result` which needs handling
            .expect("Failed to read line"); // Handle potential errors
    }
    // wait for our db to exit
    pool.exit().unwrap();
    // shutdown our tracer
    shoal_core::server::trace::shutdown(provider);
}
