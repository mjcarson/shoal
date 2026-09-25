//! Loading the TMDB csv into a running Shoal database
//!
//! The loader connects to every deployed member as the cluster's admin, reads the csv on a
//! blocking thread, and hands each movie to one of several workers. Each worker drives its own
//! unordered stream behind an in flight gate. Every write is an insert keyed by the movie's id
//! (and, for a keyword row, its keyword, title and id), and an insert replaces the row with the
//! same key, so sending a write again is always safe. A write that fails in a way that says to
//! try again - an unknown outcome, a shed, a leader between elections - is sent again after a
//! backoff, up to `--retries` times; any other failure stops the load with the error, and a
//! stopped load is finished by running it again.
//!
//! After the load a sample of the ids is read back, and a movie that is not found fails the run.

use clap::{ArgGroup, Args};
use color_eyre::eyre::{bail, eyre, WrapErr};
use shoal::client::{Shoal, ShoalQueryStream};
use shoal::shared::queries::Queries;
use shoal::shared::responses::ResponseActionNames;
use shoal::{Errors, QuerySuceededOpts};
use shoalctl::deploy::Deployment;
use shoal::shared::protocol::error::ErrorCode;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{Movie, MovieByKeyword, MovieGet, TmdbClient};

/// How long to keep trying each deployed member before loading without it
const CONNECT_DEADLINE: Duration = Duration::from_secs(10);

/// How many csv rows to read between progress lines
const PROGRESS_EVERY: usize = 100_000;

/// How long a query that failed transiently waits before it is first sent again
const RETRY_FIRST: Duration = Duration::from_millis(20);

/// The longest a query that failed transiently waits before it is sent again
const RETRY_CAP: Duration = Duration::from_millis(500);

/// Load the TMDB csv into a deployed cluster, or into a single node
#[derive(Args, Debug, Clone)]
#[command(group(ArgGroup::new("target").required(true).args(["inventory", "addr"])))]
pub struct LoadArgs {
    /// The inventory the cluster was deployed from; the loader connects to every member as its admin
    #[clap(long, short)]
    pub inventory: Option<PathBuf>,
    /// A node to load without credentials instead, such as one started by hand for development
    #[clap(long)]
    pub addr: Option<String>,
    /// The TMDB csv to load
    ///
    /// `TMDB_movie_dataset_v11.csv`, from
    /// <https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies>.
    #[clap(long)]
    pub dataset: PathBuf,
    /// Only load this many movies instead of the whole file
    #[clap(long)]
    pub limit: Option<usize>,
    /// The number of client workers, each with its own stream, spread over the members
    #[clap(long, default_value_t = 8)]
    pub workers: usize,
    /// The number of queries to buffer before sending a batch
    #[clap(long, default_value_t = 100)]
    pub batch: usize,
    /// The maximum queries each worker may have in flight at once
    ///
    /// This must be more than four times `--batch` or a worker drains its pipeline between batches
    /// and spends the load waiting on itself. Past what the cluster commits it buys nothing: the
    /// gate is a queue, a write waits behind everything in it, and one that waits longer than the
    /// server's `replication.write_timeout` (5s by default) is answered `OutcomeUnknown`.
    #[clap(long, default_value_t = 1024)]
    pub in_flight: usize,
    /// How many times to send a query again after a failure that says to try again
    ///
    /// Zero stops the load at the first failure of any kind.
    #[clap(long, default_value_t = 8)]
    pub retries: u32,
    /// How many of the loaded movies to read back and check, zero for none
    #[clap(long, default_value_t = 10_000)]
    pub verify: usize,
}

impl LoadArgs {
    /// Check that these arguments describe a load that can make progress
    ///
    /// # Errors
    ///
    /// Names what is wrong with the arguments.
    pub fn validate(&self) -> color_eyre::Result<()> {
        // a worker stops buffering once it hits its in flight limit, so a limit near the batch
        // size empties the pipeline every cycle and the load spends its time idle
        if self.in_flight <= self.batch * 4 {
            bail!(
                "--in-flight ({}) must be more than 4x --batch ({}), otherwise a worker drains its \
                 pipeline between batches",
                self.in_flight,
                self.batch
            );
        }
        // we need at least one worker to send anything
        if self.workers == 0 {
            bail!("--workers must be at least 1");
        }
        // a batch of nothing never gets sent
        if self.batch == 0 {
            bail!("--batch must be at least 1");
        }
        // the csv is opened on a blocking thread, so say it is missing before anything connects
        if !self.dataset.is_file() {
            bail!(
                "the dataset {} does not exist; download TMDB_movie_dataset_v11.csv from \
                 https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies",
                self.dataset.display()
            );
        }
        Ok(())
    }
}

/// The handle a spawned worker is waited on through
///
/// A worker ends by returning, so the outer error is a panic and the inner one is a query that
/// failed. Both stop the load.
type WorkerHandle = tokio::task::JoinHandle<Result<(), Errors>>;

/// A unit of work for a worker
///
/// The movie is boxed because it is a wide row, and an unboxed one would make every slot of every
/// worker's channel that size.
pub enum Job {
    /// Write this movie, and a row per keyword it carries
    Insert(Box<Movie>),
    /// Read the movie with this id back
    Get(u64),
}

/// What a load did, shared across every worker
#[derive(Default, Debug)]
pub struct Counts {
    /// How many rows were acknowledged as written, movies and keyword rows together
    pub inserted: AtomicUsize,
    /// How many movies were asked for by the verify phase
    pub requested: AtomicUsize,
    /// How many movies came back from the verify phase
    pub retrieved: AtomicUsize,
    /// How many csv rows could not be read at all
    pub skipped: AtomicUsize,
    /// How many queries were sent again after a failure that said to try again
    pub retried: AtomicUsize,
}

/// Connect to every member of a deployed cluster that answers, as its admin
///
/// A member that does not answer by the deadline is left out with a warning, since every member
/// routes a write to the node that owns it and the load does not need them all.
///
/// # Arguments
///
/// * `inventory` - The inventory the cluster was deployed from
///
/// # Errors
///
/// When the deployment's state cannot be read, or no member answers.
async fn connect_deployment(inventory: &PathBuf) -> color_eyre::Result<Vec<Arc<Shoal<TmdbClient>>>> {
    // the deployment's state holds the admin password and every member's address
    let deployment = Deployment::attach(inventory)?;
    let record = deployment.state.record()?;
    if record.nodes.is_empty() {
        bail!(
            "{} has no deployed nodes; run `cluster bootstrap -i {}` first",
            deployment.inventory.name,
            inventory.display()
        );
    }
    let mut clients = Vec::with_capacity(record.nodes.len());
    for (name, node) in &record.nodes {
        // each member's client address, on the inventory's client port
        let address: std::net::IpAddr = node
            .address
            .parse()
            .wrap_err_with(|| format!("{name} was recorded with the address {:?}", node.address))?;
        let addr = shoalctl::deploy::inventory::socket(address, deployment.inventory.ports.client);
        // a member that will not answer is skipped rather than fatal
        match deployment
            .connect::<TmdbClient>(&addr, Instant::now() + CONNECT_DEADLINE)
            .await
        {
            Ok(client) => {
                println!("connected to {name} at {addr}");
                clients.push(client);
            }
            Err(error) => eprintln!("warning: loading without {name}: {error}"),
        }
    }
    // with nobody answering there is nothing to load into
    if clients.is_empty() {
        bail!("no member of {} answered", deployment.inventory.name);
    }
    Ok(clients)
}

/// Connect to wherever the arguments point
///
/// # Arguments
///
/// * `args` - The settings for this load
///
/// # Errors
///
/// When nothing could be connected to.
async fn connect(args: &LoadArgs) -> color_eyre::Result<Vec<Arc<Shoal<TmdbClient>>>> {
    // the loader's two targets are the same as every other command's
    connect_targets(args.inventory.as_ref(), args.addr.as_deref()).await
}

/// Connect to a deployment's members, or to one node by address
///
/// # Arguments
///
/// * `inventory` - The inventory of a deployed cluster, connected to as its admin
/// * `addr` - A single node's client address, connected to without credentials
///
/// # Errors
///
/// When nothing could be connected to.
pub async fn connect_targets(
    inventory: Option<&PathBuf>,
    addr: Option<&str>,
) -> color_eyre::Result<Vec<Arc<Shoal<TmdbClient>>>> {
    match (inventory, addr) {
        // a deployed cluster, as its admin
        (Some(inventory), _) => connect_deployment(inventory).await,
        // one node, as nobody
        (None, Some(addr)) => {
            let client = Shoal::<TmdbClient>::new(addr)
                .await
                .map_err(|error| eyre!("could not connect to {addr}: {error}"))?;
            println!("connected to {addr}");
            Ok(vec![Arc::new(client)])
        }
        // clap's group makes one of the two required
        (None, None) => Err(eyre!("give --inventory or --addr")),
    }
}

/// One query a worker has buffered or sent, kept so a transient failure can send it again
///
/// Every write is an insert that replaces the row with its key, and a get changes nothing, so
/// sending one of these a second time is always safe: that is what lets a worker retry an
/// `OutcomeUnknown` rather than stop on it.
#[derive(Clone, Debug)]
enum Row {
    /// A movie, into the unsorted table
    Movie(Arc<Movie>),
    /// One keyword row of a movie, into the sorted table
    Keyword(MovieByKeyword),
    /// A read of the movie with this id
    Get(u64),
}

impl Row {
    /// Adds the query this row is to a buffer
    ///
    /// # Arguments
    ///
    /// * `buffer` - The query buffer to add to
    fn add_to(&self, buffer: &mut Queries<TmdbClient>) {
        match self {
            // a row converts straight into a query, so inserting is just adding the row itself
            Row::Movie(movie) => buffer.add_mut(Movie::clone(movie)),
            Row::Keyword(row) => buffer.add_mut(row.clone()),
            // a get names the partition keys it wants, which here is one id
            Row::Get(id) => buffer.add_mut(MovieGet::new(vec![*id])),
        }
    }
}

/// A row waiting out its backoff before it is sent again
#[derive(Debug)]
struct Retry {
    /// When it may be sent again
    at: Instant,
    /// What to send
    row: Row,
    /// How many times it has been sent already
    attempts: u32,
}

/// Whether a failed query says that sending it again may succeed
///
/// The codes the client's own retry repeats a bundle on: turned away before anything ran, a
/// leader that is not one, a quorum that is not there, a lost connection, a deadline, and an
/// outcome that is unknown. A retry here is a new query rather than the same identity, which is
/// safe only because every write this loader makes replaces its row.
///
/// # Arguments
///
/// * `code` - What the server answered the query with
fn retriable(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::OutcomeUnknown
            | ErrorCode::Shedding
            | ErrorCode::NotLeader
            | ErrorCode::Unavailable
            | ErrorCode::QuorumUnavailable
            | ErrorCode::ConnectionLost
            | ErrorCode::Timeout
    )
}

/// How long to wait before sending a row again, after it has been sent `attempts` times
///
/// Doubling from twenty milliseconds to half a second, the shape of the client's own retry: a
/// cluster that shed or timed out a write is overloaded, and sending it straight back adds to
/// the load that made it fail.
///
/// # Arguments
///
/// * `attempts` - How many times the row has been sent already
fn backoff(attempts: u32) -> Duration {
    // shift at most far enough to pass the cap, so a large attempt count cannot overflow
    let doubled = RETRY_FIRST.saturating_mul(1 << attempts.saturating_sub(1).min(8));
    doubled.min(RETRY_CAP)
}

/// One worker's stream and every query it has buffered, sent but not had answered, or is
/// waiting to send again
struct Pipeline {
    /// The stream this worker sends on
    queries_tx: ShoalQueryStream<TmdbClient>,
    /// The queries buffered for the next batch
    buffer: Queries<TmdbClient>,
    /// The row and attempt count behind each query in `buffer`, in the same order
    staged: Vec<(Row, u32)>,
    /// Every sent query not yet answered, by its index in the stream
    outstanding: HashMap<usize, (Row, u32)>,
    /// Rows that failed transiently, waiting out their backoff
    retries: Vec<Retry>,
    /// How much room to leave in each fresh buffer
    capacity: usize,
}

impl Pipeline {
    /// Wraps a fresh stream
    ///
    /// # Arguments
    ///
    /// * `queries_tx` - The stream to send on
    /// * `capacity` - How much room to leave in each fresh buffer
    fn new(queries_tx: ShoalQueryStream<TmdbClient>, capacity: usize) -> Self {
        let buffer = queries_tx.query_with_capacity(capacity);
        Pipeline {
            queries_tx,
            buffer,
            staged: Vec::with_capacity(capacity),
            outstanding: HashMap::new(),
            retries: Vec::new(),
            capacity,
        }
    }

    /// How many queries have been sent and not yet answered
    fn in_flight(&self) -> usize {
        self.outstanding.len()
    }

    /// Buffers one row, remembering how many times it has been sent
    ///
    /// # Arguments
    ///
    /// * `row` - The row to buffer
    /// * `attempts` - How many times it has been sent already
    fn stage(&mut self, row: Row, attempts: u32) {
        row.add_to(&mut self.buffer);
        self.staged.push((row, attempts));
    }

    /// Buffers the queries one job turns into
    ///
    /// An insert fans out: one query for the movie itself, plus one for every keyword it carries.
    /// That is why a batch holds at least `--batch` queries and usually rather more.
    ///
    /// # Arguments
    ///
    /// * `job` - The job to turn into queries
    fn stage_job(&mut self, job: Job) {
        match job {
            Job::Insert(movie) => {
                // one row per keyword, into the sorted table
                for row in MovieByKeyword::rows(&movie) {
                    self.stage(Row::Keyword(row), 0);
                }
                // then the movie itself, shared rather than copied again if it is retried
                self.stage(Row::Movie(Arc::from(movie)), 0);
            }
            Job::Get(id) => self.stage(Row::Get(id), 0),
        }
    }

    /// Buffers every row whose backoff has passed
    ///
    /// # Arguments
    ///
    /// * `now` - The time to judge the backoffs against
    fn stage_ready(&mut self, now: Instant) {
        // take the ready ones out, keeping the rest waiting in whatever order they were in
        let (ready, waiting): (Vec<Retry>, Vec<Retry>) =
            std::mem::take(&mut self.retries).into_iter().partition(|retry| retry.at <= now);
        self.retries = waiting;
        for retry in ready {
            self.stage(retry.row, retry.attempts);
        }
    }

    /// When the next row waiting out its backoff may be sent, if any is waiting
    fn next_retry(&self) -> Option<Instant> {
        self.retries.iter().map(|retry| retry.at).min()
    }

    /// Sends the buffered queries and remembers each one under its index in the stream
    ///
    /// # Errors
    ///
    /// When the batch could not be written to the stream.
    async fn send(&mut self) -> Result<(), Errors> {
        // swap the full buffer out for an empty one rather than clearing it in place
        let queries = std::mem::replace(
            &mut self.buffer,
            self.queries_tx.query_with_capacity(self.capacity),
        );
        // the stream numbers this batch's queries on from where the last batch stopped
        let base = self.queries_tx.base_index;
        // the stamps a send returns are a zero sized type off a profiling build
        self.queries_tx.send(queries).await?;
        // every query in the batch is now owed an answer under its index
        for (offset, sent) in self.staged.drain(..).enumerate() {
            self.outstanding.insert(base + offset, sent);
        }
        Ok(())
    }
}

/// Drives one stream at the cluster until its channel of jobs runs dry
///
/// Top the pipeline up while there is room and there is work, and otherwise wait for a response.
/// The gate is a high water mark rather than a drain: the worker resumes as soon as it is one
/// query under the limit, so the pipeline stays full instead of emptying every cycle. A query
/// that fails in a way that says to try again is sent again after a backoff, up to `--retries`
/// times; any other failure, or one that outlasts its retries, stops the load.
///
/// # Arguments
///
/// * `client` - The client to send on
/// * `jobs` - The channel this worker takes its work from
/// * `args` - The settings for this load
/// * `counts` - The shared counters to record what came back in
async fn worker(
    client: Arc<Shoal<TmdbClient>>,
    mut jobs: Receiver<Job>,
    args: Arc<LoadArgs>,
    counts: Arc<Counts>,
) -> Result<(), Errors> {
    // each worker gets its own stream, and so its own connection out of the pool
    //
    // unordered rather than ordered: an ordered stream holds a response back until every earlier
    // one has arrived, so a single slow query would stall everything queued behind it
    let (queries_tx, mut results_rx) = client.stream_unordered()?;
    // leave room for a full batch plus the fan out a batch of inserts adds
    let mut pipe = Pipeline::new(queries_tx, args.batch * 2);
    // whether there is any more work coming
    let mut drained = false;
    loop {
        // rows whose backoff has passed go ahead of new work
        pipe.stage_ready(Instant::now());
        // top the pipeline up while it has room and the producer has something for us
        while pipe.in_flight() < args.in_flight && !drained {
            match jobs.try_recv() {
                Ok(job) => {
                    // buffer this job's queries, and send once we have a full batch
                    pipe.stage_job(job);
                    if pipe.buffer.len() >= args.batch {
                        pipe.send().await?;
                    }
                }
                Err(TryRecvError::Empty) => {
                    // the producer has not caught up, so flush a partial batch rather than
                    // stranding it until the end of the load
                    if !pipe.buffer.is_empty() {
                        pipe.send().await?;
                        continue;
                    }
                    // nothing buffered but something outstanding or waiting to go again, so go
                    // drain a response or wait out a backoff
                    if pipe.in_flight() > 0 || !pipe.retries.is_empty() {
                        break;
                    }
                    // nothing buffered and nothing outstanding, so it is safe to park here
                    match jobs.recv().await {
                        Some(job) => pipe.stage_job(job),
                        None => drained = true,
                    }
                }
                // the producer is gone, so whatever is buffered is the last of the new work
                Err(TryRecvError::Disconnected) => drained = true,
            }
        }
        // retries staged after the producer finished, and its last partial batch, go out here
        if !pipe.buffer.is_empty() {
            pipe.send().await?;
        }
        // everything sent, answered and not waiting to go again means this worker is done
        if drained && pipe.in_flight() == 0 && pipe.retries.is_empty() {
            break;
        }
        // with nothing outstanding there is no response to wait for, only the next backoff
        if pipe.in_flight() == 0 {
            if let Some(at) = pipe.next_retry() {
                tokio::time::sleep(at.saturating_duration_since(Instant::now())).await;
            }
            continue;
        }
        // wait for the next response, whichever query it belongs to
        let Some(response) = results_rx.next().await? else {
            // the stream ended, which after a drain is the normal way out
            break;
        };
        // this query is answered, whatever the answer was
        let sent = pipe.outstanding.remove(&response.get_index());
        // a failure that says to try again sends the same row again after a backoff
        if let (Some(error), Some((row, attempts))) = (response.error(), sent) {
            if retriable(error.code()) && attempts < args.retries {
                counts.retried.fetch_add(1, Ordering::Relaxed);
                pipe.retries.push(Retry {
                    at: Instant::now() + backoff(attempts + 1),
                    row,
                    attempts: attempts + 1,
                });
                continue;
            }
        }
        // any other failed query is data that is not in the database, so stop rather than
        // carry on
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
    pipe.queries_tx.close().await?;
    // read on to the end the close just sent, which is what releases this stream's slot in the
    // client's channel map
    //
    // a stream dropped with its slot still there leaves a closed channel behind it, and a late
    // frame for it ends the read loop of whichever connection it lands on, stranding every other
    // stream's answers on that connection (item 130). anything that arrives here was already
    // answered once, so it is not counted again
    while results_rx.next().await?.is_some() {}
    Ok(())
}

/// Spawns the configured number of workers, spread round robin over the connected members
///
/// Returns the senders to feed and the handles to wait on.
///
/// # Arguments
///
/// * `clients` - One client per connected member
/// * `args` - The settings for this load
/// * `counts` - The shared counters the workers record into
fn spawn_workers(
    clients: &[Arc<Shoal<TmdbClient>>],
    args: &Arc<LoadArgs>,
    counts: &Arc<Counts>,
) -> (Vec<Sender<Job>>, Vec<WorkerHandle>) {
    let mut senders = Vec::with_capacity(args.workers);
    let mut handles = Vec::with_capacity(args.workers);
    for index in 0..args.workers {
        // a channel per worker rather than one shared receiver, so no worker ever waits on a lock
        // to find out whether there is work for it
        let (tx, rx) = tokio::sync::mpsc::channel(args.in_flight);
        senders.push(tx);
        // each worker on the next member in turn, so every member coordinates a share
        let client = clients[index % clients.len()].clone();
        handles.push(tokio::spawn(worker(client, rx, args.clone(), counts.clone())));
    }
    (senders, handles)
}

/// Waits for every worker to finish and reports the first one that failed
///
/// # Arguments
///
/// * `handles` - The worker handles to wait on
///
/// # Errors
///
/// When a worker panicked or a query failed.
async fn join_workers(handles: Vec<WorkerHandle>) -> color_eyre::Result<()> {
    let mut first = None;
    for handle in handles {
        // wait for every worker, so none is left running, and keep the first failure
        let outcome = match handle.await {
            Ok(Ok(())) => continue,
            Ok(Err(error)) => eyre!("a write failed: {error}"),
            Err(error) => eyre!("a worker panicked: {error}"),
        };
        first.get_or_insert(outcome);
    }
    match first {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Reads the csv on a blocking thread, handing each movie to a worker
///
/// Returns the id of every movie that was queued, in file order, so the verify phase can sample
/// them without reading the file a second time. A million ids is eight megabytes.
///
/// A row that will not deserialize is skipped and counted, not fatal: stopping at the first bad
/// row would silently truncate a load to whatever clean prefix the file happened to have.
///
/// # Arguments
///
/// * `senders` - One channel per worker, fed round robin
/// * `args` - The settings for this load
/// * `counts` - The shared counters to record skipped rows in
///
/// # Errors
///
/// When the csv cannot be opened.
fn read_dataset(
    senders: Vec<Sender<Job>>,
    args: Arc<LoadArgs>,
    counts: Arc<Counts>,
) -> color_eyre::Result<Vec<u64>> {
    // open the dataset, saying which path failed rather than just that one did
    let mut reader = csv::Reader::from_path(&args.dataset)
        .wrap_err_with(|| format!("failed to open {}", args.dataset.display()))?;
    let mut ids = Vec::new();
    // read movies until the file runs out or we hit the limit we were given
    for row in reader.deserialize::<Movie>() {
        // a row we cannot read is one row of a million, so count it and carry on
        let Ok(movie) = row else {
            counts.skipped.fetch_add(1, Ordering::Relaxed);
            continue;
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
            // a worker stopped on a failed write, which the join reports, so stop reading
            break;
        }
        // say where we are, since a full load is minutes of silence otherwise
        if ids.len() % PROGRESS_EVERY == 0 {
            println!("  read {} movies", ids.len());
        }
        // stop once we have queued as many movies as we were asked for
        if Some(ids.len()) == args.limit {
            break;
        }
    }
    Ok(ids)
}

/// Writes every movie in the dataset, and its keyword rows
///
/// Returns the ids that were written, in file order.
///
/// # Arguments
///
/// * `clients` - One client per connected member
/// * `args` - The settings for this load
/// * `counts` - The shared counters to record what was written in
///
/// # Errors
///
/// When the csv cannot be read or a write fails.
async fn write_all(
    clients: &[Arc<Shoal<TmdbClient>>],
    args: &Arc<LoadArgs>,
    counts: &Arc<Counts>,
) -> color_eyre::Result<Vec<u64>> {
    println!("-- loading {} --", args.dataset.display());
    let started = Instant::now();
    let (senders, handles) = spawn_workers(clients, args, counts);
    // parse the csv off the runtime: it is a half gigabyte of blocking work, and leaving it on a
    // worker thread would starve the tasks draining the responses
    let reader = tokio::task::spawn_blocking({
        let args = args.clone();
        let counts = counts.clone();
        move || read_dataset(senders, args, counts)
    });
    // the reader owns every sender, so it returning is what closes the workers' channels
    let read = reader
        .await
        .map_err(|error| eyre!("the csv reader panicked: {error}"));
    // the workers' outcome first: a failed write is why a reader stops early
    join_workers(handles)
        .await
        .wrap_err("the load stopped; loading is idempotent, so run it again to finish")?;
    let ids = read??;
    report("wrote", counts.inserted.load(Ordering::Relaxed), started);
    // a skipped row is worth saying out loud, since it is data that is not in the database
    let skipped = counts.skipped.load(Ordering::Relaxed);
    if skipped > 0 {
        println!("  skipped {skipped} unreadable csv rows");
    }
    // a retry is a sign the cluster was pushed past what it commits, so say how many there were
    let retried = counts.retried.load(Ordering::Relaxed);
    if retried > 0 {
        println!("  retried {retried} queries after a failure that said to try again");
    }
    Ok(ids)
}

/// Reads a sample of the loaded movies back and checks every one was found
///
/// The sample is a fixed stride over the ids in file order rather than the first *n* of them: the
/// dataset is sorted by vote count, so the head of it is the popular movies and reading only those
/// back would check a very different set of partitions from the rest.
///
/// # Arguments
///
/// * `clients` - One client per connected member
/// * `ids` - Every id that was written, in file order
/// * `args` - The settings for this load
/// * `counts` - The shared counters to record what came back in
///
/// # Errors
///
/// When a read fails, or a movie that was written is not found.
async fn verify(
    clients: &[Arc<Shoal<TmdbClient>>],
    ids: &[u64],
    args: &Arc<LoadArgs>,
    counts: &Arc<Counts>,
) -> color_eyre::Result<()> {
    // nothing was loaded, or nothing was asked for
    if ids.is_empty() || args.verify == 0 {
        return Ok(());
    }
    // the dataset repeats a few ids, and a repeated id is one movie to read back
    let mut sample: Vec<u64> = ids
        .iter()
        .step_by(std::cmp::max(1, ids.len() / args.verify))
        .copied()
        .collect();
    sample.sort_unstable();
    sample.dedup();
    println!("-- reading back {} of {} movies --", sample.len(), ids.len());
    let started = Instant::now();
    let (senders, handles) = spawn_workers(clients, args, counts);
    // feed the sampled ids in, round robin across the same worker set
    for (sent, id) in sample.iter().enumerate() {
        counts.requested.fetch_add(1, Ordering::Relaxed);
        if senders[sent % senders.len()].send(Job::Get(*id)).await.is_err() {
            // a worker stopped on a failed read, which the join reports
            break;
        }
    }
    // dropping the senders is what tells the workers there is no more work coming
    drop(senders);
    join_workers(handles).await?;
    let retrieved = counts.retrieved.load(Ordering::Relaxed);
    report("read", retrieved, started);
    // every movie asked for was written, so one that is missing is data that was lost
    let requested = counts.requested.load(Ordering::Relaxed);
    if retrieved < requested {
        bail!("{} of {requested} movies read back were not found", requested - retrieved);
    }
    Ok(())
}

/// Prints what a phase did and how long it took
///
/// A rate, not a benchmark: there is no warmup and no repetition behind it. Measuring Shoal is
/// `shoal-bench`'s job.
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

/// Load the dataset into wherever the arguments point, then check a sample of it
///
/// # Arguments
///
/// * `args` - The settings for this load
///
/// # Errors
///
/// When the arguments are wrong, nothing answers, a write or read fails, or a movie that was
/// written is not found.
pub async fn run(args: LoadArgs) -> color_eyre::Result<()> {
    // refuse a load that cannot make progress before connecting to anything
    args.validate()?;
    let args = Arc::new(args);
    // every member that answers, or the one node named
    let clients = connect(&args).await?;
    let counts = Arc::new(Counts::default());
    // write the whole dataset, then read a sample of it back through the same pipeline
    let ids = write_all(&clients, &args, &counts).await?;
    // the writes' retries were reported with them, so only the reads' are left to say
    let written_retries = counts.retried.load(Ordering::Relaxed);
    verify(&clients, &ids, &args, &counts).await?;
    let read_retries = counts.retried.load(Ordering::Relaxed) - written_retries;
    if read_retries > 0 {
        println!("  retried {read_retries} reads after a failure that said to try again");
    }
    println!("loaded {} movies", ids.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// A command line holding the load arguments, the way the loader binary nests them
    #[derive(Parser, Debug)]
    struct Cli {
        /// The load arguments
        #[command(flatten)]
        load: LoadArgs,
    }

    /// A load names exactly one target and a dataset, and refuses a pipeline that would stall
    #[test]
    fn a_load_names_one_target_and_a_pipeline_that_flows() {
        // a dataset that exists, since validation looks for it
        let dataset = std::env::current_exe().expect("the test binary");
        let dataset = dataset.to_str().expect("a utf8 path");
        // an inventory or an address, and never neither or both
        assert!(Cli::try_parse_from(["t", "--dataset", dataset]).is_err());
        assert!(Cli::try_parse_from(["t", "-i", "a.yml", "--addr", "h:1", "--dataset", dataset]).is_err());
        let cli = Cli::try_parse_from(["t", "-i", "a.yml", "--dataset", dataset]).expect("a load");
        cli.load.validate().expect("the defaults flow");
        // an in flight limit near the batch size drains the pipeline every cycle
        let mut stalled = cli.load.clone();
        stalled.in_flight = stalled.batch * 4;
        assert!(stalled.validate().unwrap_err().to_string().contains("--in-flight"));
        // and a dataset that is not there is said before anything connects
        let mut missing = cli.load.clone();
        missing.dataset = PathBuf::from("/nonexistent/TMDB_movie_dataset_v11.csv");
        assert!(missing.validate().unwrap_err().to_string().contains("kaggle"));
    }

    /// A failure is retried only when it says to try again, and the wait doubles up to its cap
    #[test]
    fn a_transient_failure_is_retried_after_a_capped_backoff() {
        // the codes a cluster answers an overloaded or failing over write with are retried
        for code in [
            ErrorCode::OutcomeUnknown,
            ErrorCode::Shedding,
            ErrorCode::NotLeader,
            ErrorCode::Unavailable,
            ErrorCode::QuorumUnavailable,
            ErrorCode::ConnectionLost,
            ErrorCode::Timeout,
        ] {
            assert!(retriable(code), "{code:?}");
        }
        // a query the server could not run for another reason would fail the same way again
        for code in [ErrorCode::Internal, ErrorCode::IdentityExpired] {
            assert!(!retriable(code), "{code:?}");
        }
        // the first retry waits the least, each one after it twice as long
        assert_eq!(backoff(1), RETRY_FIRST);
        assert_eq!(backoff(2), RETRY_FIRST * 2);
        assert_eq!(backoff(3), RETRY_FIRST * 4);
        // and never longer than the cap, however many there have been
        assert_eq!(backoff(6), RETRY_CAP);
        assert_eq!(backoff(u32::MAX), RETRY_CAP);
        // the defaults retry, and gate a worker at a quarter of what they used to
        let dataset = std::env::current_exe().expect("the test binary");
        let dataset = dataset.to_str().expect("a utf8 path");
        let cli = Cli::try_parse_from(["t", "-i", "a.yml", "--dataset", dataset]).expect("a load");
        assert_eq!(cli.load.retries, 8);
        assert_eq!(cli.load.in_flight, 1024);
    }

    /// A movie fans out into one keyword row per keyword, and a row that will not parse is skipped
    #[test]
    fn a_csv_row_becomes_a_movie_and_its_keyword_rows() {
        // the dataset's header and two rows: one whole, one whose id is not a number
        let csv = "id,title,vote_average,vote_count,status,release_date,revenue,runtime,adult,\
                   backdrop_path,budget,homepage,imdb_id,original_language,original_title,overview,\
                   popularity,poster_path,tagline,genres,production_companies,production_countries,\
                   spoken_languages,keywords\n\
                   27205,Inception,8.364,34495,Released,2010-07-15,825532764,148,False,/b.jpg,\
                   160000000,https://x,tt1375666,en,Inception,A thief,83.952,/p.jpg,Your mind,\
                   \"Action, Science Fiction\",Legendary,\"United Kingdom, United States of America\",\
                   \"English, French\",\"rescue, dream\"\n\
                   nope,Broken,,,,,,,,,,,,,,,,,,,,,,\n";
        let mut reader = csv::Reader::from_reader(csv.as_bytes());
        let rows: Vec<_> = reader.deserialize::<Movie>().collect();
        // the whole row parses, lists split and lenient numbers read
        let movie = rows[0].as_ref().expect("a movie");
        assert_eq!(movie.id, 27205);
        assert!(!movie.adult);
        assert_eq!(movie.genres, vec!["Action", "Science Fiction"]);
        // the unparseable key is an error, which the loader counts and skips
        assert!(rows[1].is_err());
        // and the movie is listed once per keyword, by title and id
        let keywords = MovieByKeyword::rows(movie);
        assert_eq!(keywords.len(), 2);
        assert_eq!(keywords[1].keyword, "dream");
        assert_eq!((keywords[1].title.as_str(), keywords[1].id), ("Inception", 27205));
        // ordered by title, then by id, as numbers
        let order = |title, id| MovieByKeyword::order(title, id);
        assert!(order("Alien", 9) < order("Alien", 10));
        assert!(order("Alien", u64::MAX) < order("Aliens", 0));
        assert!(order("Alien", 1) < order("Alien 3", 0));
    }
}
