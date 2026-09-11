//! The same tour as the `tmdb` example, at the scale of a real dataset
//!
//! ```sh
//! cargo run --release --example tmdb_dataset
//! ```
//!
//! Unlike the `tmdb` example beside it, which is self contained and runs with nothing set up,
//! this one needs two things off disk.
//!
//! # The dataset
//!
//! `TMDB_movie_dataset_v11.csv`, about 538 MB and 1.19 million movies, from
//! <https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies>. Put it at
//! `~/datasets/TMDB_movie_dataset_v11.csv` or point `--dataset` at wherever it landed. Nothing in
//! this repository fetches it, which is the whole reason this example is separate from the one
//! that needs no setup.
//!
//! # The config
//!
//! `./shoal.yml` if there is one, and the built in defaults if there is not. The defaults put
//! storage at `/opt/shoal`, which has to exist and be writable, and a full load writes several
//! gigabytes into it.
//!
//! # What it shows
//!
//! 1. **A wide row.** [`Movie`] is the full 24 column TMDB record, deserialized straight out of
//!    the csv into the table struct.
//! 2. **Fan out into a second table.** Each movie also writes one [`MovieByKeyword`] row per
//!    keyword it carries, which is what makes "every movie tagged `alien`, in title order" a range
//!    of one partition rather than a scan.
//! 3. **A pipelined client.** Several workers each drive their own unordered stream behind an in
//!    flight gate, which is what a client that wants throughput out of Shoal actually looks like.
//! 4. **Reading it back**, three ways: a sampled re-read of ids through the same pipeline, one
//!    keyed get printed in full, and the same question asked in SHQL.
//!
//! # This is not a benchmark
//!
//! It prints an elapsed time because a run that takes minutes should say how many, not because the
//! number means anything. There is no warmup, no percentile, no baseline and no core pinning here
//! on purpose: measuring Shoal is `shoal-bench`'s job, and a second thing that printed latencies
//! would only end up disagreeing with it. See `docs/src/performance/benchmarking.md`.

use clap::Parser;
use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::ShoalQueryStream;
use shoal::server::conf::Resources;
use shoal::shared::queries::Queries;
use shoal::shared::responses::ResponseActionNames;
use shoal::{
    Conf, Errors, FileSystem, PersistentSortedTable, PersistentUnsortedTable, QuerySuceededOpts,
    Shoal, ShoalPool, ShoalSortedTable, ShoalUnsortedTable,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender};

/// Load the TMDB dataset into Shoal and read it back
#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
pub struct Args {
    /// The TMDB csv to load
    ///
    /// Download it from
    /// <https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies>.
    #[clap(long, default_value_os_t = default_dataset())]
    pub dataset: PathBuf,
    /// The shoal config to start the server with, if it exists
    ///
    /// A path that is not there is not an error: the server falls back to its built in defaults,
    /// which store data under `/opt/shoal`.
    #[clap(long, default_value = "shoal.yml")]
    pub conf: PathBuf,
    /// Only load this many movies instead of the whole file
    #[clap(long)]
    pub limit: Option<usize>,
    /// The number of client workers to run, each with its own stream
    #[clap(long, default_value_t = 4)]
    pub workers: usize,
    /// The number of queries to buffer before sending a batch
    #[clap(long, default_value_t = 100)]
    pub batch: usize,
    /// The maximum queries each worker may have in flight at once
    ///
    /// This must be well above `--batch` or the worker drains its pipeline between batches and
    /// spends the run waiting on itself.
    #[clap(long, default_value_t = 4096)]
    pub in_flight: usize,
    /// How many of the loaded movies to read back again
    #[clap(long, default_value_t = 10_000)]
    pub verify: usize,
}

impl Args {
    /// Check that these arguments describe a run that can make progress
    ///
    /// # Errors
    ///
    /// Returns what is wrong with the arguments, for the caller to print.
    fn validate(&self) -> Result<(), String> {
        // a worker stops buffering once it hits its in flight limit, so a limit near the batch
        // size empties the pipeline every cycle and the run spends its time idle
        if self.in_flight <= self.batch * 4 {
            return Err(format!(
                "--in-flight ({}) must be more than 4x --batch ({}), otherwise a worker drains \
                 its pipeline between batches",
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

/// Where the dataset is looked for when `--dataset` was not given
///
/// Built from `$HOME` at runtime rather than baked in as an absolute path, because the version of
/// this example that carried one could only ever have run on the machine it was written on.
fn default_dataset() -> PathBuf {
    // fall back to the current directory when there is no home to hang this off
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    Path::new(&home).join("datasets/TMDB_movie_dataset_v11.csv")
}

/// Deserialize a comma-space separated column into a list
///
/// The dataset joins its list valued columns with `", "` and quotes the whole thing, so what
/// arrives here is one string per row rather than a sequence.
///
/// # Arguments
///
/// * `deserializer` - The deserializer to read this column from
fn deserialize_comma_separated<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // read the whole column as one string
    let raw: String = serde::Deserialize::deserialize(deserializer)?;
    // an empty cell is an empty list rather than a list holding one empty string
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    Ok(raw.split(", ").map(ToString::to_string).collect())
}

/// Deserialize a column that should hold an integer, tolerating one that does not
///
/// A cell that is empty or unparseable becomes zero. This is deliberate and only safe because none
/// of the fields it is used on identify a row: `id` is left strict below, so a row whose key
/// cannot be read is skipped rather than being filed under partition zero along with every other
/// unreadable row.
///
/// # Arguments
///
/// * `deserializer` - The deserializer to read this column from
fn deserialize_lenient_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // csv hands every column over as text, so asking for a string always succeeds
    let raw: String = serde::Deserialize::deserialize(deserializer)?;
    Ok(raw.trim().parse().unwrap_or_default())
}

/// Deserialize a column that should hold a float, tolerating one that does not
///
/// The same bargain as [`deserialize_lenient_u64`], for the two rating columns.
///
/// # Arguments
///
/// * `deserializer` - The deserializer to read this column from
fn deserialize_lenient_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // csv hands every column over as text, so asking for a string always succeeds
    let raw: String = serde::Deserialize::deserialize(deserializer)?;
    Ok(raw.trim().parse().unwrap_or_default())
}

/// Deserialize a column that should hold a boolean, tolerating one that does not
///
/// # Arguments
///
/// * `deserializer` - The deserializer to read this column from
fn deserialize_lenient_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // csv hands every column over as text, so asking for a string always succeeds
    let raw: String = serde::Deserialize::deserialize(deserializer)?;
    Ok(raw.trim().eq_ignore_ascii_case("true"))
}

/// A movie, stored one per partition
///
/// This is the whole TMDB record, column for column, which is what makes it a useful thing to
/// store: it is a wide row with several variable length fields rather than a key and a counter.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Archive,
    Serialize,
    Deserialize,
    ShoalUnsortedTable,
    DeepSizeOf,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Tmdb")]
pub struct Movie {
    /// The id for this movie, which it is partitioned by
    ///
    /// Left strict on purpose: a row whose key will not parse is skipped by the loader instead of
    /// being written under partition zero.
    #[shoal(partition)]
    pub id: u64,
    /// The name of this movie, which a get can filter on
    #[shoal(filter)]
    pub title: String,
    /// The vote average
    #[serde(deserialize_with = "deserialize_lenient_f64")]
    pub vote_average: f64,
    /// The total number of votes
    #[serde(deserialize_with = "deserialize_lenient_u64")]
    pub vote_count: u64,
    /// The status of this movie
    pub status: String,
    /// The date this movie was released
    pub release_date: String,
    /// The total revenue this movie made
    #[serde(deserialize_with = "deserialize_lenient_u64")]
    pub revenue: u64,
    /// The runtime for this movie in minutes
    #[serde(deserialize_with = "deserialize_lenient_u64")]
    pub runtime: u64,
    /// Whether this is an adult movie
    #[serde(deserialize_with = "deserialize_lenient_bool")]
    pub adult: bool,
    /// The path to this movies backdrop on tmdb
    pub backdrop_path: String,
    /// The budget for this movie
    #[serde(deserialize_with = "deserialize_lenient_u64")]
    pub budget: u64,
    /// The url to this movies homepage
    pub homepage: String,
    /// The imdb id for this movie
    pub imdb_id: String,
    /// The original language for this movie
    pub original_language: String,
    /// The original title for this movie
    pub original_title: String,
    /// The overview for this movie, which an update can change
    #[shoal(update)]
    pub overview: String,
    /// The popularity of this movie
    #[serde(deserialize_with = "deserialize_lenient_f64")]
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

/// A movie listed under one of its keywords, stored many per partition
///
/// A sorted table adds `#[shoal(sort)]`, which orders the rows *within* a partition. That is what
/// makes reading every movie tagged `alien` a range of a single partition instead of a scan of the
/// whole table.
#[derive(
    Debug, Clone, PartialEq, Archive, Serialize, Deserialize, ShoalSortedTable, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Tmdb")]
pub struct MovieByKeyword {
    /// The keyword this row is partitioned by
    #[shoal(partition)]
    pub keyword: String,
    /// The title, which rows are ordered by within a keyword
    #[shoal(sort)]
    pub title: String,
}

/// The database
///
/// `#[shoal::db]` reads this struct and generates the client type, the query enum and the response
/// enum for it.
#[shoal::db]
pub struct Tmdb {
    /// Movies, one per partition
    pub movie: PersistentUnsortedTable<Movie, FileSystem>,
    /// Movies by keyword, many per partition
    pub movie_by_keyword: PersistentSortedTable<MovieByKeyword, FileSystem>,
}

/// The handle a spawned worker is waited on through
///
/// A worker ends by returning, so the outer error is a panic and the inner one is a query that
/// failed. Both are fatal to a run, which is what [`join_workers`] does with them.
type WorkerHandle = tokio::task::JoinHandle<Result<(), Errors>>;

/// A unit of work for a worker
///
/// The movie is boxed because it is the wide row this example exists to store, and an unboxed one
/// would make every slot of every worker's channel that size.
pub enum Job {
    /// Write this movie, and a row per keyword it carries
    Insert(Box<Movie>),
    /// Read the movie with this id back
    Get(u64),
}

/// What a run did, shared across every worker
#[derive(Default)]
pub struct Counts {
    /// How many rows were acknowledged as written
    inserted: AtomicUsize,
    /// How many rows came back from a get
    retrieved: AtomicUsize,
    /// How many csv rows could not be read at all
    skipped: AtomicUsize,
}

/// Adds the queries one job turns into to a worker's buffer
///
/// An insert fans out: one query for the movie itself, plus one for every keyword it carries. That
/// is why a batch holds at least `--batch` queries and usually rather more.
///
/// # Arguments
///
/// * `buffer` - The query buffer to add to
/// * `job` - The job to turn into queries
fn buffer_job(buffer: &mut Queries<TmdbClient>, job: Job) {
    match job {
        Job::Insert(movie) => {
            // one row per keyword, into the sorted table
            for keyword in &movie.keywords {
                buffer.add_mut(MovieByKeyword {
                    keyword: keyword.clone(),
                    title: movie.title.clone(),
                });
            }
            // a row converts straight into a query, so inserting is just adding the row itself
            buffer.add_mut(*movie);
        }
        // a get names the partition keys it wants, which here is one id
        Job::Get(id) => buffer.add_mut(MovieGet::new(vec![id])),
    }
}

/// Sends a worker's buffered queries and hands back a fresh buffer
///
/// Returns how many queries went out, so the caller can count them as in flight.
///
/// # Arguments
///
/// * `queries_tx` - The stream to send this batch on
/// * `buffer` - The buffer to drain
/// * `capacity` - How much room to leave in the replacement buffer
async fn send_batch(
    queries_tx: &mut ShoalQueryStream<TmdbClient>,
    buffer: &mut Queries<TmdbClient>,
    capacity: usize,
) -> Result<usize, Errors> {
    // swap the full buffer out for an empty one rather than clearing it in place
    let queries = std::mem::replace(buffer, queries_tx.query_with_capacity(capacity));
    // remember how many queries this batch is about to put outstanding
    let sent = queries.len();
    // the stamps a send returns are a zero sized type off a profiling build, so dropping them
    // here costs nothing
    queries_tx.send(queries).await?;
    Ok(sent)
}

/// Drives one stream at the server until its channel of jobs runs dry
///
/// The shape here is the one `shoal-bench`'s driver uses: top the pipeline up while there is room
/// and there is work, and otherwise wait for a response. The gate is a high water mark rather than
/// a drain - the worker resumes as soon as it is one query under the limit, so the pipeline stays
/// full instead of emptying every cycle.
///
/// # Arguments
///
/// * `client` - The client to send on
/// * `jobs` - The channel this worker takes its work from
/// * `args` - The settings for this run
/// * `counts` - The shared counters to record what came back in
async fn worker(
    client: Arc<Shoal<TmdbClient>>,
    mut jobs: Receiver<Job>,
    args: Arc<Args>,
    counts: Arc<Counts>,
) -> Result<(), Errors> {
    // each worker gets its own stream, and so its own connection out of the pool
    //
    // unordered rather than ordered: an ordered stream holds a response back until every earlier
    // one has arrived, so a single slow query would stall everything queued behind it
    let (mut queries_tx, mut results_rx) = client.stream_unordered()?;
    // leave room for a full batch plus the fan out a batch of inserts adds
    let capacity = args.batch * 2;
    let mut buffer = queries_tx.query_with_capacity(capacity);
    // how many queries are outstanding, and whether there is any more work coming
    let mut in_flight = 0usize;
    let mut drained = false;
    loop {
        // top the pipeline up while it has room and the producer has something for us
        while in_flight < args.in_flight && !drained {
            match jobs.try_recv() {
                Ok(job) => {
                    // buffer this job's queries, and send once we have a full batch
                    buffer_job(&mut buffer, job);
                    if buffer.len() >= args.batch {
                        in_flight += send_batch(&mut queries_tx, &mut buffer, capacity).await?;
                    }
                }
                Err(TryRecvError::Empty) => {
                    // the producer has not caught up, so flush a partial batch rather than
                    // stranding it until the end of the run
                    if !buffer.is_empty() {
                        in_flight += send_batch(&mut queries_tx, &mut buffer, capacity).await?;
                        continue;
                    }
                    // nothing buffered but something outstanding, so go drain a response
                    if in_flight > 0 {
                        break;
                    }
                    // nothing buffered and nothing outstanding, so it is safe to park here
                    match jobs.recv().await {
                        Some(job) => buffer_job(&mut buffer, job),
                        None => drained = true,
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    // the producer is gone, so whatever is buffered is the last batch
                    if !buffer.is_empty() {
                        in_flight += send_batch(&mut queries_tx, &mut buffer, capacity).await?;
                    }
                    drained = true;
                }
            }
        }
        // everything sent and everything answered means this worker is done
        if drained && in_flight == 0 {
            break;
        }
        // wait for the next response, whichever query it belongs to
        let Some(response) = results_rx.next().await? else {
            // the stream ended, which after a drain is the normal way out
            break;
        };
        // saturate rather than wrap: a query answering with more than one response would
        // otherwise underflow this to usize::MAX and permanently satisfy the gate above
        in_flight = in_flight.saturating_sub(1);
        // a failed query makes everything after it meaningless, so stop rather than carrying on
        response.suceeded(QuerySuceededOpts::default())?;
        // count what came back, by what it was
        match response.kind() {
            ResponseActionNames::Insert => {
                counts.inserted.fetch_add(1, Ordering::Relaxed);
            }
            ResponseActionNames::Get => {
                // a get answers with the rows it found, which is what to count
                if let Some(rows) = response.access::<Movie>()? {
                    counts.retrieved.fetch_add(rows.len(), Ordering::Relaxed);
                }
            }
            _ => (),
        }
    }
    // close the stream so the server stops holding its channel open
    queries_tx.close().await?;
    Ok(())
}

/// Spawns a worker per configured slot, each with its own channel of jobs
///
/// Returns the senders to feed and the handles to wait on.
///
/// # Arguments
///
/// * `client` - The client the workers send on
/// * `args` - The settings for this run
/// * `counts` - The shared counters the workers record into
fn spawn_workers(
    client: &Arc<Shoal<TmdbClient>>,
    args: &Arc<Args>,
    counts: &Arc<Counts>,
) -> (Vec<Sender<Job>>, Vec<WorkerHandle>) {
    let mut senders = Vec::with_capacity(args.workers);
    let mut handles = Vec::with_capacity(args.workers);
    for _ in 0..args.workers {
        // a channel per worker rather than one shared receiver, so no worker ever waits on a lock
        // to find out whether there is work for it
        let (tx, rx) = tokio::sync::mpsc::channel(args.in_flight);
        senders.push(tx);
        handles.push(tokio::spawn(worker(
            client.clone(),
            rx,
            args.clone(),
            counts.clone(),
        )));
    }
    (senders, handles)
}

/// Waits for every worker to finish and reports the first one that failed
///
/// # Arguments
///
/// * `handles` - The worker handles to wait on
async fn join_workers(handles: Vec<WorkerHandle>) {
    for handle in handles {
        // a worker that panicked and one that returned an error are both fatal to the run, since
        // the counts printed afterwards would be describing a load that did not happen
        handle
            .await
            .expect("a worker panicked")
            .expect("a worker failed");
    }
}

/// Reads the csv on a blocking thread, handing each movie to a worker
///
/// Returns the id of every movie that was queued, in file order, so the verify phase can sample
/// them without reading the file a second time. A million ids is eight megabytes, which is nothing
/// beside the rows themselves.
///
/// A row that will not deserialize is **skipped and counted**, not fatal and not a stopping point.
/// The version of this loader that lived in the old `tmdb` example stopped at the first bad row,
/// which silently truncated a run to whatever clean prefix the file happened to have.
///
/// # Arguments
///
/// * `senders` - One channel per worker, fed round robin
/// * `args` - The settings for this run
/// * `counts` - The shared counters to record skipped rows in
fn read_dataset(senders: Vec<Sender<Job>>, args: Arc<Args>, counts: Arc<Counts>) -> Vec<u64> {
    // open the dataset, saying which path failed rather than just that one did
    let mut reader = match csv::Reader::from_path(&args.dataset) {
        Ok(reader) => reader,
        Err(error) => panic!(
            "failed to open {}: {error}\n\nDownload it from \
             https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies",
            args.dataset.display()
        ),
    };
    let mut ids = Vec::new();
    // read movies until the file runs out or we hit the limit we were given
    for row in reader.deserialize::<Movie>() {
        // a row we cannot read is one row of a million, so count it and carry on
        let movie = match row {
            Ok(movie) => movie,
            Err(_) => {
                counts.skipped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        ids.push(movie.id);
        // hand this movie to the next worker in turn
        //
        // `blocking_send` rather than `send`, because this runs on a blocking thread and has no
        // runtime of its own to await on
        let worker = ids.len() % senders.len();
        if senders[worker]
            .blocking_send(Job::Insert(Box::new(movie)))
            .is_err()
        {
            // a worker died, and continuing would just fill a channel nobody is draining
            eprintln!("warning: a worker went away after {} movies", ids.len());
            break;
        }
        // say where we are, since a full load is a few minutes of silence otherwise
        if ids.len() % 100_000 == 0 {
            println!("  read {} movies", ids.len());
        }
        // stop once we have queued as many movies as we were asked for
        if Some(ids.len()) == args.limit {
            break;
        }
    }
    ids
}

/// Reads every movie in the dataset into Shoal
///
/// Returns the ids that were written, in file order.
///
/// # Arguments
///
/// * `client` - The client to write with
/// * `args` - The settings for this run
/// * `counts` - The shared counters to record what was written in
async fn load(client: &Arc<Shoal<TmdbClient>>, args: &Arc<Args>, counts: &Arc<Counts>) -> Vec<u64> {
    println!("-- loading {} --", args.dataset.display());
    let started = Instant::now();
    let (senders, handles) = spawn_workers(client, args, counts);
    // parse the csv off the runtime: it is a half gigabyte of blocking work, and leaving it on a
    // worker thread would starve the tasks draining the responses
    let reader = tokio::task::spawn_blocking({
        let args = args.clone();
        let counts = counts.clone();
        move || read_dataset(senders, args, counts)
    });
    // the reader owns every sender, so it returning is what closes the workers' channels
    let ids = reader.await.expect("the csv reader panicked");
    join_workers(handles).await;
    report("wrote", counts.inserted.load(Ordering::Relaxed), started);
    // a skipped row is worth saying out loud, since it is data that is not in the database
    let skipped = counts.skipped.load(Ordering::Relaxed);
    if skipped > 0 {
        println!("  skipped {skipped} unreadable csv rows");
    }
    println!();
    ids
}

/// Reads a sample of the loaded movies back through the same pipeline
///
/// The sample is a fixed stride over the ids in file order rather than the first *n* of them: the
/// dataset is sorted by vote count, so the head of it is the popular movies and reading only those
/// back would exercise a very different set of partitions from the rest.
///
/// # Arguments
///
/// * `client` - The client to read with
/// * `ids` - Every id that was written, in file order
/// * `args` - The settings for this run
/// * `counts` - The shared counters to record what came back in
async fn verify(
    client: &Arc<Shoal<TmdbClient>>,
    ids: &[u64],
    args: &Arc<Args>,
    counts: &Arc<Counts>,
) {
    // nothing was loaded, or nothing was asked for
    if ids.is_empty() || args.verify == 0 {
        return;
    }
    // take one id every `stride` rather than the first `--verify` of them
    let stride = std::cmp::max(1, ids.len() / args.verify);
    println!("-- reading back every {stride} movie of {} --", ids.len());
    let started = Instant::now();
    let (senders, handles) = spawn_workers(client, args, counts);
    // feed the sampled ids in, round robin across the same worker set
    for (sent, id) in ids.iter().step_by(stride).enumerate() {
        if senders[sent % senders.len()]
            .send(Job::Get(*id))
            .await
            .is_err()
        {
            eprintln!("warning: a worker went away after {sent} gets");
            break;
        }
    }
    // dropping the senders is what tells the workers there is no more work coming
    drop(senders);
    join_workers(handles).await;
    report("read", counts.retrieved.load(Ordering::Relaxed), started);
    println!();
}

/// Prints one movie in full, by its id
///
/// # Arguments
///
/// * `client` - The client to read with
/// * `id` - The id of the movie to read
async fn read_one(client: &Shoal<TmdbClient>, id: u64) -> Result<(), Errors> {
    println!("-- one whole row, by partition key --");
    let response = client.send_one(MovieGet::new(vec![id])).await?;
    // `access` reads the rows straight out of the archive that came off the wire, with no
    // deserialization at all
    match response.access::<Movie>()? {
        Some(rows) if !rows.is_empty() => {
            for movie in rows.iter() {
                println!("  {} ({})", movie.title, movie.release_date);
                println!(
                    "  {:.1} from {} votes",
                    movie.vote_average, movie.vote_count
                );
                println!("  keywords: {:?}", movie.keywords);
            }
        }
        // the id we picked is not in the slice that was loaded, which a --limit makes likely
        _ => println!("  id {id} was not loaded"),
    }
    println!();
    Ok(())
}

/// Reads a sorted partition back with a query written as text
///
/// # Arguments
///
/// * `client` - The client to read with
/// * `keyword` - The keyword to list movies for
/// * `limit` - How many titles to print before stopping
async fn read_with_shql(
    client: &Shoal<TmdbClient>,
    keyword: &str,
    limit: usize,
) -> Result<(), Errors> {
    println!("-- movies tagged '{keyword}', in title order, via SHQL --");
    // the same query the typed builders make, parsed from text
    let query = client.query().parse(&format!(
        "select * from MovieByKeyword where keyword = '{keyword}'"
    ))?;
    let mut response = client.send(query).await?;
    let mut printed = 0;
    while let Some(row) = response.next().await? {
        if let Some(rows) = row.access::<MovieByKeyword>()? {
            // a sorted table returns its rows in sort key order, which here is by title
            for tagged in rows.iter() {
                // this partition holds thousands of titles on a full load, so print a taste of it
                if printed < limit {
                    println!("  {}", tagged.title);
                }
                printed += 1;
            }
        }
    }
    println!("  ({printed} in all)\n");
    Ok(())
}

/// Prints what a phase did and how long it took
///
/// This is a rate, not a benchmark: there is no warmup and no repetition behind it. See the note
/// at the top of this file.
///
/// # Arguments
///
/// * `verb` - What the phase did to the rows
/// * `rows` - How many rows it did it to
/// * `started` - When the phase began
fn report(verb: &str, rows: usize, started: Instant) {
    let elapsed = started.elapsed();
    // guard the division, since a phase that did nothing still gets reported
    let rate = if elapsed.as_secs_f64() > 0.0 {
        rows as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    println!("  {verb} {rows} rows in {elapsed:.1?} ({rate:.0} rows/s)");
}

/// Loads the config this run uses, falling back to the defaults when there is no file
///
/// `Conf::from_file` treats a missing file as an empty one and still overlays `SHOAL_` prefixed
/// environment variables, so the fallback here is about being explicit rather than about avoiding
/// an error.
///
/// # Arguments
///
/// * `path` - Where to look for a config
fn load_conf(path: &Path) -> Conf {
    // use the file when there is one
    if path.exists() {
        println!("config: {}", path.display());
        let path = path.to_str().expect("the config path is not valid utf8");
        return Conf::from_file(path).expect("failed to load the config");
    }
    println!(
        "config: none at {}, using defaults (storage under /opt/shoal)",
        path.display()
    );
    // the defaults with one field moved, and it is not cosmetic: `Resources::default()` leaves
    // `memory` at zero, and the shard evicts whenever its usage is over that - so a literal
    // default config evicts on essentially every loop and the load never gets anywhere
    Conf::default().resources(
        Resources::default()
            .memory("4GiB")
            .expect("4GiB is a valid memory size"),
    )
}

/// Connects to a server, retrying while it finishes starting
///
/// # Arguments
///
/// * `addr` - The address the server is coming up on
async fn connect(addr: &str) -> Result<Shoal<TmdbClient>, Errors> {
    // `ShoalPool::ready` has already waited for every shard to bind, so one attempt is enough;
    // the retry exists because a loaded machine can still refuse the very first connection
    let mut last = None;
    for _ in 0..20 {
        match Shoal::<TmdbClient>::new(addr).await {
            Ok(client) => return Ok(client),
            Err(error) => last = Some(error),
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    Err(last.expect("the loop ran at least once"))
}

/// Starts a server, loads the dataset into it, and reads it back
#[tokio::main]
async fn main() -> Result<(), Errors> {
    // parse and sanity check our settings before starting anything
    let args = Args::parse();
    if let Err(error) = args.validate() {
        eprintln!("error: {error}");
        std::process::exit(2);
    }
    let args = Arc::new(args);
    // load the config off disk if there is one, and fall back to the defaults if there is not
    let conf = load_conf(&args.conf);
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    // setup tracing/telemetry. the guard has to outlive every span below it, because dropping it
    // is what flushes whatever the exporter has queued
    let traces = shoal_core::server::trace::setup(&conf);
    // start one shard per configured core, and wait until every one of them is answering
    let mut pool = ShoalPool::<Tmdb>::start(conf).expect("failed to start shoal");
    pool.ready(std::time::Duration::from_secs(30))
        .expect("a shard failed to start");
    let client = Arc::new(connect(&addr).await?);
    println!();
    let counts = Arc::new(Counts::default());
    // write the whole dataset, then read a sample of it back through the same pipeline
    let ids = load(&client, &args, &counts).await;
    verify(&client, &ids, &args, &counts).await;
    // and the two reads that are worth looking at rather than counting
    if let Some(id) = ids.first() {
        read_one(&client, *id).await?;
    }
    read_with_shql(&client, "alien", 10).await?;
    // stop the shards, which flushes everything still buffered
    pool.exit().expect("failed to stop shoal");
    // shutdown our tracer, which ships whatever the exporter still has queued
    shoal_core::server::trace::shutdown(traces);
    Ok(())
}
