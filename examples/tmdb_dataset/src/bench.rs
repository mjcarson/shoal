//! The lab's test driver: a timed mixed workload, a full read back and an acknowledged write check
//!
//! These are the three commands the [distributed cluster testing](../../docs/src/cluster-testing/overview.md)
//! chapter runs against a deployed TMDB cluster:
//!
//! - `bench` drives gets, keyword partition reads, updates and inserts for a fixed time and prints
//!   a line a second, so an outage caused by a fault shows up as the seconds it lasted rather than
//!   as a lower average. Every insert it makes is of a synthetic movie whose id is above
//!   [`SYNTHETIC_BASE`], and `--acks` writes the id of every one the cluster acknowledged.
//! - `verify-acks` reads every acknowledged id back. An acknowledged write that is not found is a
//!   lost write, and fails the run.
//! - `verify` reads every movie in the csv back and compares it field by field, and every keyword
//!   partition, comparing the set of movies under it.

use clap::{ArgGroup, Args, ValueEnum};
use color_eyre::eyre::{bail, eyre, WrapErr};
use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rkyv::rancor::Error as RkyvError;
use shoal::client::{SendOptions, Shoal, ShoalQueryStream};
use shoal::shared::protocol::read::ReadLevel;
use shoal::shared::queries::Queries;
use shoal::{Errors, QuerySuceededOpts};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::load::connect_targets;
use crate::{Movie, MovieByKeyword, MovieByKeywordGet, MovieGet, MovieUpdate, TmdbClient};

/// The first id a synthetic movie is written under
///
/// TMDB's ids are below ten million, so nothing the dataset loads can collide with a synthetic
/// movie, and a synthetic movie is never mistaken for a dataset one by `verify`.
pub const SYNTHETIC_BASE: u64 = 1 << 40;

/// How many ids a worker may use before its range would reach the next worker's
const SYNTHETIC_PER_WORKER: u64 = 1 << 32;

/// How many movies one get asks for when reading back
const READ_BACK_CHUNK: usize = 256;

/// How long a worker waits for its next answer before it says what it is still owed
const HUNG_CHECK: Duration = Duration::from_secs(10);

/// How long past the end of the run a worker waits for answers before it records them as hung
const HUNG_AFTER: Duration = Duration::from_secs(60);

/// The most rows a keyword read in the bench asks for
///
/// A popular keyword holds thousands of movies, and the bench is timing a read, not a scan.
const KEYWORD_LIMIT: usize = 50;

/// The read level a command asks for, or the cluster's default
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    /// One replica's applied state, possibly stale
    One,
    /// A quorum barrier, then the replica's state applied through it
    Quorum,
}

impl Level {
    /// Build the send options that ask for this level
    ///
    /// # Arguments
    ///
    /// * `level` - The level asked for, if any
    fn options(level: Option<Level>) -> SendOptions {
        // no level means the table's and then the cluster's default
        match level {
            Some(Level::One) => SendOptions::new().read(ReadLevel::One),
            Some(Level::Quorum) => SendOptions::new().read(ReadLevel::Quorum),
            None => SendOptions::new(),
        }
    }
}

/// The kinds of operation the bench mixes
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OpKind {
    /// A get of one movie by id
    Get,
    /// A read of one keyword partition, limited to [`KEYWORD_LIMIT`] rows
    Keyword,
    /// An update of one movie's overview, to the value it already has
    Update,
    /// An insert of a new synthetic movie
    Insert,
}

impl OpKind {
    /// Every kind, in the order they are reported
    const ALL: [OpKind; 4] = [OpKind::Get, OpKind::Keyword, OpKind::Update, OpKind::Insert];

    /// The name this kind is given in a mix and in a report
    fn name(self) -> &'static str {
        // one short name per kind
        match self {
            OpKind::Get => "get",
            OpKind::Keyword => "keyword",
            OpKind::Update => "update",
            OpKind::Insert => "insert",
        }
    }
}

/// The weights of each kind of operation in a bench run
///
/// A newtype rather than a `Vec`, because clap reads a `Vec` field as a repeated argument
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mix(pub Vec<(OpKind, u32)>);

/// Parse a mix like `get:70,keyword:15,update:10,insert:5`
///
/// # Arguments
///
/// * `raw` - The mix as given on the command line
fn parse_mix(raw: &str) -> Result<Mix, String> {
    // each comma separated entry is a kind and a weight
    let mut mix = Vec::new();
    for entry in raw.split(',').filter(|entry| !entry.is_empty()) {
        // split the kind from its weight
        let (name, weight) = entry
            .split_once(':')
            .ok_or_else(|| format!("{entry:?} is not kind:weight"))?;
        // find the kind this names
        let kind = OpKind::ALL
            .into_iter()
            .find(|kind| kind.name() == name.trim())
            .ok_or_else(|| format!("{name:?} is not one of get, keyword, update, insert"))?;
        // and read its weight
        let weight: u32 = weight
            .trim()
            .parse()
            .map_err(|error| format!("{entry:?} has a bad weight: {error}"))?;
        mix.push((kind, weight));
    }
    // a mix of nothing would never send anything
    if mix.iter().map(|(_, weight)| weight).sum::<u32>() == 0 {
        return Err("the mix has no weight".to_string());
    }
    Ok(Mix(mix))
}

/// Drive a timed mixed workload against a deployed cluster
#[derive(Args, Debug, Clone)]
#[command(group(ArgGroup::new("target").required(true).args(["inventory", "addr"])))]
pub struct BenchArgs {
    /// The inventory of the deployed cluster to drive
    #[clap(long, short)]
    pub inventory: Option<PathBuf>,
    /// A single node's client address, for a node started by hand
    #[clap(long)]
    pub addr: Option<String>,
    /// The csv the cluster was loaded from, which the ids and keywords are chosen out of
    #[clap(long)]
    pub dataset: PathBuf,
    /// How many movies from the top of the csv to choose ids and keywords from
    #[clap(long, default_value_t = 100_000)]
    pub sample: usize,
    /// How long to drive the cluster for, in seconds
    #[clap(long, default_value_t = 30)]
    pub duration: u64,
    /// How many concurrent streams to drive, spread round robin over the members
    #[clap(long, default_value_t = 8)]
    pub workers: usize,
    /// How many queries a worker sends in one bundle
    #[clap(long, default_value_t = 16)]
    pub batch: usize,
    /// How many queries a worker keeps outstanding
    #[clap(long, default_value_t = 128)]
    pub in_flight: usize,
    /// The weights of each kind of operation
    #[clap(long, default_value = "get:70,keyword:15,update:10,insert:5", value_parser = parse_mix)]
    pub mix: Mix,
    /// The level reads are served at, or the cluster's default
    #[clap(long, value_enum)]
    pub read: Option<Level>,
    /// Where to write the id of every synthetic insert the cluster acknowledged
    #[clap(long)]
    pub acks: Option<PathBuf>,
    /// Where to write the summary as json
    #[clap(long)]
    pub out: Option<PathBuf>,
    /// The seed the choices are drawn from
    #[clap(long, default_value_t = 7)]
    pub seed: u64,
    /// A label for the synthetic ids, so two runs never write the same ones
    #[clap(long, default_value_t = 0)]
    pub run: u64,
    /// Log every operation answered this many milliseconds or more after it was sent, with the
    /// member it went through and when, so a slow second can be taken apart
    #[clap(long)]
    pub slow_ms: Option<u64>,
}

/// What a worker is choosing from
struct Corpus {
    /// Movie ids that were loaded, with the overview each one was loaded with
    movies: Vec<(u64, String)>,
    /// Keywords that were loaded
    keywords: Vec<String>,
}

impl Corpus {
    /// Read the first `sample` movies of the csv
    ///
    /// # Arguments
    ///
    /// * `dataset` - The csv the cluster was loaded from
    /// * `sample` - How many movies to read
    fn read(dataset: &PathBuf, sample: usize) -> color_eyre::Result<Self> {
        // open the csv the same way the loader does
        let mut reader = csv::Reader::from_path(dataset)
            .wrap_err_with(|| format!("failed to open {}", dataset.display()))?;
        let mut movies = Vec::with_capacity(sample);
        let mut keywords = HashSet::new();
        // keep the id, overview and keywords of each movie that parses
        for row in reader.deserialize::<Movie>().take(sample) {
            let Ok(movie) = row else { continue };
            keywords.extend(movie.keywords.iter().cloned());
            movies.push((movie.id, movie.overview));
        }
        // a stable order, so a seed picks the same keyword on every run
        let mut keywords: Vec<String> = keywords.into_iter().collect();
        keywords.sort_unstable();
        if movies.is_empty() || keywords.is_empty() {
            bail!("{} held no movies with keywords to choose from", dataset.display());
        }
        Ok(Corpus { movies, keywords })
    }
}

/// One operation that has been sent and not yet answered
struct Sent {
    /// What kind of operation it was
    kind: OpKind,
    /// When its bundle was handed to the client
    at: Instant,
    /// The synthetic id it inserted, if it was an insert
    inserted: Option<u64>,
}

/// What one kind of operation did over some window
struct KindStats {
    /// The latency of every operation that succeeded, in microseconds
    latency: Histogram<u64>,
    /// How many failed, by the code they failed with
    errors: BTreeMap<String, u64>,
    /// The first message each code came with, since a code alone rarely says which failure it was
    samples: BTreeMap<String, String>,
}

impl KindStats {
    /// An empty record
    fn new() -> Self {
        KindStats {
            // one microsecond to two minutes at three significant figures
            latency: Histogram::new_with_bounds(1, 120_000_000, 3)
                .expect("the histogram bounds are valid"),
            errors: BTreeMap::new(),
            samples: BTreeMap::new(),
        }
    }
}

/// What every kind of operation did over some window
struct Window {
    /// Each kind's record
    kinds: BTreeMap<OpKind, KindStats>,
}

impl Window {
    /// An empty window
    fn new() -> Self {
        Window {
            kinds: OpKind::ALL.into_iter().map(|kind| (kind, KindStats::new())).collect(),
        }
    }

    /// Record one answered operation
    ///
    /// # Arguments
    ///
    /// * `kind` - What kind of operation it was
    /// * `outcome` - Its latency, or the code it failed with and what the failure said
    fn record(&mut self, kind: OpKind, outcome: Result<Duration, (String, String)>) {
        // every kind has a record from the start
        let stats = self.kinds.get_mut(&kind).expect("every kind has a record");
        match outcome {
            Ok(latency) => {
                // saturate rather than drop an answer slower than the histogram's top
                let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
                stats.latency.saturating_record(micros.max(1));
            }
            Err((code, msg)) => {
                stats.samples.entry(code.clone()).or_insert(msg);
                *stats.errors.entry(code).or_default() += 1;
            }
        }
    }

    /// Fold another window into this one
    ///
    /// # Arguments
    ///
    /// * `other` - The window to add
    fn add(&mut self, other: &Window) {
        // add each kind's latencies and errors to ours
        for (kind, theirs) in &other.kinds {
            let ours = self.kinds.get_mut(kind).expect("every kind has a record");
            ours.latency
                .add(&theirs.latency)
                .expect("the histograms share their bounds");
            for (code, count) in &theirs.errors {
                *ours.errors.entry(code.clone()).or_default() += count;
            }
            for (code, msg) in &theirs.samples {
                ours.samples.entry(code.clone()).or_insert_with(|| msg.clone());
            }
        }
    }

    /// One line describing this window
    ///
    /// # Arguments
    ///
    /// * `elapsed` - How long the window covered
    fn line(&self, elapsed: Duration) -> String {
        // one entry per kind that did anything
        let mut parts = Vec::new();
        for (kind, stats) in &self.kinds {
            let ok = stats.latency.len();
            let failed: u64 = stats.errors.values().sum();
            if ok + failed == 0 {
                continue;
            }
            let rate = ok as f64 / elapsed.as_secs_f64().max(1e-9);
            let mut part = format!(
                "{} {rate:.0}/s p50 {:.2}ms p99 {:.2}ms max {:.1}ms",
                kind.name(),
                stats.latency.value_at_quantile(0.50) as f64 / 1000.0,
                stats.latency.value_at_quantile(0.99) as f64 / 1000.0,
                stats.latency.max() as f64 / 1000.0,
            );
            if failed > 0 {
                part.push_str(&format!(" errors {:?}", stats.errors));
            }
            parts.push(part);
        }
        parts.join(" | ")
    }

    /// This window as json, for the summary file
    ///
    /// # Arguments
    ///
    /// * `elapsed` - How long the window covered
    fn json(&self, elapsed: Duration) -> serde_json::Value {
        // one object per kind
        let mut kinds = serde_json::Map::new();
        for (kind, stats) in &self.kinds {
            let quantile = |q: f64| stats.latency.value_at_quantile(q) as f64 / 1000.0;
            kinds.insert(
                kind.name().to_string(),
                serde_json::json!({
                    "ok": stats.latency.len(),
                    "per_sec": stats.latency.len() as f64 / elapsed.as_secs_f64().max(1e-9),
                    "p50_ms": quantile(0.50),
                    "p90_ms": quantile(0.90),
                    "p99_ms": quantile(0.99),
                    "p999_ms": quantile(0.999),
                    "max_ms": stats.latency.max() as f64 / 1000.0,
                    "errors": stats.errors,
                    "samples": stats.samples,
                }),
            );
        }
        serde_json::Value::Object(kinds)
    }
}

/// What every worker shares
struct Shared {
    /// The current second's window, swapped out by the reporter
    window: Mutex<Window>,
    /// Every synthetic id the cluster acknowledged
    acked: Mutex<Vec<u64>>,
    /// When the run started, which a slow operation's time is logged against
    started: Instant,
    /// The latency at or past which an operation is logged, if any is
    slow: Option<Duration>,
}

/// Choose an operation and add it to a bundle
///
/// # Arguments
///
/// * `rng` - Where choices come from
/// * `corpus` - What to choose ids and keywords from
/// * `mix` - The weights of each kind
/// * `next_synthetic` - The next synthetic id this worker will insert
/// * `buffer` - The bundle to add the query to
fn choose(
    rng: &mut SmallRng,
    corpus: &Corpus,
    mix: &[(OpKind, u32)],
    next_synthetic: &mut u64,
    buffer: &mut Queries<TmdbClient>,
) -> Sent {
    // pick a kind by weight
    let total: u32 = mix.iter().map(|(_, weight)| weight).sum();
    let mut pick = rng.gen_range(0..total);
    let kind = mix
        .iter()
        .find(|(_, weight)| {
            if pick < *weight {
                return true;
            }
            pick -= weight;
            false
        })
        .map_or(OpKind::Get, |(kind, _)| *kind);
    // build the query for it
    let mut inserted = None;
    match kind {
        OpKind::Get => {
            let (id, _) = &corpus.movies[rng.gen_range(0..corpus.movies.len())];
            buffer.add_mut(MovieGet::new(vec![*id]));
        }
        OpKind::Keyword => {
            let keyword = &corpus.keywords[rng.gen_range(0..corpus.keywords.len())];
            buffer.add_mut(MovieByKeywordGet::new(vec![keyword.clone()]).limit(KEYWORD_LIMIT));
        }
        OpKind::Update => {
            // write the overview it already has, so a later `verify` still matches the csv
            let (id, overview) = &corpus.movies[rng.gen_range(0..corpus.movies.len())];
            buffer.add_mut(MovieUpdate {
                partition_key: *id,
                overview: Some(overview.clone()),
            });
        }
        OpKind::Insert => {
            // a movie nobody else writes, under an id this worker owns
            let id = *next_synthetic;
            *next_synthetic += 1;
            buffer.add_mut(synthetic_movie(id));
            inserted = Some(id);
        }
    }
    Sent {
        kind,
        at: Instant::now(),
        inserted,
    }
}

/// Build the synthetic movie the bench inserts under an id
///
/// # Arguments
///
/// * `id` - The synthetic id
#[must_use]
pub fn synthetic_movie(id: u64) -> Movie {
    // enough of a movie to be a realistic row, and derived from the id alone
    Movie {
        id,
        title: format!("synthetic {id}"),
        vote_average: 5.0,
        vote_count: id % 1000,
        status: "Released".to_string(),
        release_date: "2026-09-25".to_string(),
        revenue: 0,
        runtime: 90,
        adult: false,
        backdrop_path: String::new(),
        budget: 0,
        homepage: String::new(),
        imdb_id: String::new(),
        original_language: "en".to_string(),
        original_title: format!("synthetic {id}"),
        overview: "A movie written by the cluster test driver, to check it is never lost."
            .to_string(),
        popularity: 1.0,
        poster_path: String::new(),
        tagline: String::new(),
        genres: vec!["Drama".to_string()],
        production_companies: Vec::new(),
        production_countries: Vec::new(),
        spoken_languages: vec!["English".to_string()],
        keywords: Vec::new(),
    }
}

/// Send a bundle and remember what was in it
///
/// # Arguments
///
/// * `queries_tx` - The stream to send on
/// * `buffer` - The bundle to send, replaced with an empty one
/// * `staged` - What each query in the bundle was
/// * `outstanding` - Where to remember them by index
/// * `capacity` - How large a fresh bundle should be
async fn flush(
    queries_tx: &mut ShoalQueryStream<TmdbClient>,
    buffer: &mut Queries<TmdbClient>,
    staged: &mut Vec<Sent>,
    outstanding: &mut HashMap<usize, Sent>,
    capacity: usize,
) -> Result<(), Errors> {
    // nothing to send
    if staged.is_empty() {
        return Ok(());
    }
    // swap in an empty bundle and send the full one
    let queries = std::mem::replace(buffer, queries_tx.query_with_capacity(capacity));
    let base = queries_tx.base_index;
    queries_tx.send(queries).await?;
    // every query is now owed an answer at its index in the stream
    for (offset, sent) in staged.drain(..).enumerate() {
        outstanding.insert(base + offset, sent);
    }
    Ok(())
}

/// Drive one stream until the deadline, recording every answer
///
/// A failed send ends the worker's stream, and a new one is opened after a pause, so a fault
/// shows up as the errors it caused and the worker carries on rather than the run ending.
///
/// # Arguments
///
/// * `client` - The member this worker coordinates through
/// * `index` - Which worker this is
/// * `args` - The bench's arguments
/// * `corpus` - What to choose from
/// * `shared` - Where to record what happened
/// * `until` - When to stop
/// * `members` - How many members the workers are spread over
async fn bench_worker(
    client: Arc<Shoal<TmdbClient>>,
    index: usize,
    args: Arc<BenchArgs>,
    corpus: Arc<Corpus>,
    shared: Arc<Shared>,
    until: Instant,
    members: usize,
) {
    // a seed per worker, so two workers do not repeat each other
    let mut rng = SmallRng::seed_from_u64(args.seed.wrapping_add(index as u64 * 7919));
    // this worker's own range of synthetic ids, offset by the run so reruns never overlap
    let mut next_synthetic = SYNTHETIC_BASE
        + (args.run * args.workers as u64 + index as u64) * SYNTHETIC_PER_WORKER;
    let options = Level::options(args.read);
    // open a stream, drive it until it fails or the time is up, and open another if time remains
    while Instant::now() < until {
        if let Err(error) = drive_stream(
            &client,
            &args,
            &corpus,
            &shared,
            &options,
            &mut rng,
            &mut next_synthetic,
            until,
            index % members,
        )
        .await
        {
            // everything still outstanding on it was recorded by `drive_stream`; say why and
            // pause before trying again, so a dead member is not hammered
            eprintln!("worker {index}: stream ended: {error}");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// Drive one stream until the deadline or until it fails
///
/// # Arguments
///
/// * `client` - The member this worker coordinates through
/// * `args` - The bench's arguments
/// * `corpus` - What to choose from
/// * `shared` - Where to record what happened
/// * `options` - How reads are served
/// * `rng` - Where choices come from
/// * `next_synthetic` - The next synthetic id this worker will insert
/// * `until` - When to stop
/// * `member` - Which member the stream goes through, by its place in the connect order
#[allow(clippy::too_many_arguments)]
async fn drive_stream(
    client: &Arc<Shoal<TmdbClient>>,
    args: &BenchArgs,
    corpus: &Corpus,
    shared: &Shared,
    options: &SendOptions,
    rng: &mut SmallRng,
    next_synthetic: &mut u64,
    until: Instant,
    member: usize,
) -> Result<(), Errors> {
    // an unordered stream, since answers are recorded as they land
    let (mut queries_tx, mut results_rx) = client.stream_unordered_with(options.clone())?;
    let capacity = args.batch * 2;
    let mut buffer = queries_tx.query_with_capacity(capacity);
    let mut staged = Vec::with_capacity(capacity);
    let mut outstanding: HashMap<usize, Sent> = HashMap::with_capacity(args.in_flight);
    let outcome = async {
        loop {
            // top the pipeline up while there is time left
            let live = Instant::now() < until;
            while live && outstanding.len() + staged.len() < args.in_flight {
                staged.push(choose(rng, corpus, &args.mix.0, next_synthetic, &mut buffer));
                if staged.len() >= args.batch {
                    flush(&mut queries_tx, &mut buffer, &mut staged, &mut outstanding, capacity)
                        .await?;
                }
            }
            flush(&mut queries_tx, &mut buffer, &mut staged, &mut outstanding, capacity).await?;
            // stop once time is up and every answer is in
            if outstanding.is_empty() {
                return Ok(());
            }
            // wait for the next answer; a stream owed answers well past the run's end says so,
            // and one owed them a minute past it gives up on them as hung, since a query stream
            // has no deadline of its own on the client
            let response = match tokio::time::timeout(HUNG_CHECK, results_rx.next()).await {
                Ok(next) => match next? {
                    Some(response) => response,
                    None => return Ok(()),
                },
                Err(_) => {
                    let now = Instant::now();
                    if now > until + HUNG_AFTER {
                        return Err(Errors::Server {
                            query_id: None,
                            index: None,
                            code: shoal::shared::protocol::error::ErrorCode::Timeout,
                            msg: format!("{} answers never came", outstanding.len()),
                        });
                    }
                    if now > until {
                        let oldest = outstanding.values().map(|sent| sent.at).min();
                        let mut kinds: BTreeMap<&str, usize> = BTreeMap::new();
                        for sent in outstanding.values() {
                            *kinds.entry(sent.kind.name()).or_default() += 1;
                        }
                        eprintln!(
                            "a stream is owed {} answers ({kinds:?}), the oldest sent {:.1?} ago",
                            outstanding.len(),
                            oldest.map(|at| at.elapsed()).unwrap_or_default()
                        );
                    }
                    continue;
                }
            };
            let Some(sent) = outstanding.remove(&response.get_index()) else {
                // an answer for a query that was answered already, or never sent: each query is
                // owed exactly one, so this is said rather than dropped in silence
                eprintln!(
                    "an unexpected answer for index {}: {:?} {:?}",
                    response.get_index(),
                    response.kind(),
                    response.error().map(|error| (error.code(), error.msg().to_string()))
                );
                continue;
            };
            // a failure is counted by its code, a success by its latency
            let outcome = match response.error() {
                Some(error) => Err((format!("{:?}", error.code()), error.msg().to_string())),
                None => {
                    // a get that found nothing is a success for a keyword with fewer rows than
                    // asked, and a failure for a movie that was loaded
                    let opts = QuerySuceededOpts {
                        get: sent.kind == OpKind::Get,
                        ..QuerySuceededOpts::default()
                    };
                    match response.suceeded(opts) {
                        Ok(()) => Ok(sent.at.elapsed()),
                        Err(_) => Err(("NotFound".to_string(), String::new())),
                    }
                }
            };
            // a slow answer is logged with when it was sent and through which member
            if let Some(slow) = shared.slow {
                let took = sent.at.elapsed();
                if took >= slow {
                    eprintln!(
                        "slow {} via member {member} sent at t={:.3}s took {:.3}s id {:?}: {}",
                        sent.kind.name(),
                        sent.at.duration_since(shared.started).as_secs_f64(),
                        took.as_secs_f64(),
                        sent.inserted,
                        match &outcome {
                            Ok(_) => "ok".to_string(),
                            Err((code, msg)) => format!("{code} {msg}"),
                        }
                    );
                }
            }
            // an acknowledged insert is one the cluster promised to keep
            if let (Ok(_), Some(id)) = (&outcome, sent.inserted) {
                shared.acked.lock().expect("the ack list is never poisoned").push(id);
            }
            shared
                .window
                .lock()
                .expect("the window is never poisoned")
                .record(sent.kind, outcome);
        }
    }
    .await;
    // whatever was still owed when the stream ended will never be answered on it
    if let Err(error) = &outcome {
        let code = match error {
            Errors::Server { code, .. } => format!("{code:?}"),
            other => format!("{:?}", std::mem::discriminant(other)),
        };
        let msg = error.to_string();
        let mut window = shared.window.lock().expect("the window is never poisoned");
        for (_, sent) in outstanding.drain() {
            window.record(sent.kind, Err((format!("stream:{code}"), msg.clone())));
        }
        return outcome;
    }
    // close the stream and read it to its end, which releases its slot in the client; a
    // stream that does not reach its end in time says so rather than holding the run open
    let queries_tx_base = queries_tx.base_index;
    queries_tx.close().await?;
    let drained = tokio::time::timeout(HUNG_AFTER, async {
        let mut after_close = 0usize;
        while results_rx.next().await?.is_some() {
            after_close += 1;
        }
        Ok::<usize, Errors>(after_close)
    })
    .await;
    match drained {
        Ok(Ok(0)) => {}
        Ok(Ok(extra)) => eprintln!("a stream answered {extra} more queries after its close"),
        Ok(Err(error)) => return Err(error),
        Err(_) => eprintln!(
            "a stream did not reach its end within {HUNG_AFTER:?} of its close, with nothing owed; \
             it stood at (next, end, pending) {:?} having sent {} queries",
            results_rx.progress(),
            queries_tx_base
        ),
    }
    Ok(())
}

/// Run the bench
///
/// # Arguments
///
/// * `args` - What to run
pub async fn bench(args: BenchArgs) -> color_eyre::Result<()> {
    // a pipeline smaller than a bundle would never fill one
    if args.in_flight < args.batch || args.workers == 0 || args.batch == 0 {
        bail!("--in-flight must be at least --batch, and both and --workers at least 1");
    }
    let args = Arc::new(args);
    // choose from what the cluster was loaded with
    let corpus = Arc::new(Corpus::read(&args.dataset, args.sample)?);
    println!(
        "choosing from {} movies and {} keywords",
        corpus.movies.len(),
        corpus.keywords.len()
    );
    let clients = connect_targets(args.inventory.as_ref(), args.addr.as_deref()).await?;
    // start every worker against its member
    let started = Instant::now();
    let shared = Arc::new(Shared {
        window: Mutex::new(Window::new()),
        acked: Mutex::new(Vec::new()),
        started,
        slow: args.slow_ms.map(Duration::from_millis),
    });
    let until = started + Duration::from_secs(args.duration);
    let mut handles = Vec::with_capacity(args.workers);
    for index in 0..args.workers {
        handles.push(tokio::spawn(bench_worker(
            clients[index % clients.len()].clone(),
            index,
            args.clone(),
            corpus.clone(),
            shared.clone(),
            until,
            clients.len(),
        )));
    }
    // print a line a second until every worker is done, and keep the whole run's total
    let mut total = Window::new();
    let mut series = Vec::new();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.tick().await;
    let mut last = Instant::now();
    loop {
        ticker.tick().await;
        let finished = handles.iter().all(tokio::task::JoinHandle::is_finished);
        // swap out this second's window
        let window = std::mem::replace(
            &mut *shared.window.lock().expect("the window is never poisoned"),
            Window::new(),
        );
        let now = Instant::now();
        let elapsed = now - last;
        last = now;
        let second = (now - started).as_secs_f64();
        println!("t={second:5.1}s {}", window.line(elapsed));
        series.push(serde_json::json!({ "t": second, "kinds": window.json(elapsed) }));
        total.add(&window);
        if finished {
            break;
        }
    }
    for handle in handles {
        handle.await.map_err(|error| eyre!("a worker panicked: {error}"))?;
    }
    // the whole run
    let elapsed = started.elapsed();
    println!("-- total over {elapsed:.1?} --");
    println!("{}", total.line(elapsed));
    // the first message each failure code came with
    for (kind, stats) in &total.kinds {
        for (code, msg) in &stats.samples {
            println!("  {} {code}: {msg}", kind.name());
        }
    }
    let acked = std::mem::take(&mut *shared.acked.lock().expect("the ack list is never poisoned"));
    println!("{} synthetic inserts acknowledged", acked.len());
    // the acknowledged ids, one a line, for `verify-acks`
    if let Some(path) = &args.acks {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .wrap_err_with(|| format!("failed to open {}", path.display()))?;
        for id in &acked {
            writeln!(file, "{id}")?;
        }
    }
    // and the summary, with the per second series
    if let Some(path) = &args.out {
        let summary = serde_json::json!({
            "workers": args.workers,
            "batch": args.batch,
            "in_flight": args.in_flight,
            "read": format!("{:?}", args.read),
            "seconds": elapsed.as_secs_f64(),
            "total": total.json(elapsed),
            "acked": acked.len(),
            "series": series,
        });
        std::fs::write(path, serde_json::to_vec_pretty(&summary)?)
            .wrap_err_with(|| format!("failed to write {}", path.display()))?;
    }
    Ok(())
}

/// Read every acknowledged synthetic insert back
#[derive(Args, Debug, Clone)]
#[command(group(ArgGroup::new("target").required(true).args(["inventory", "addr"])))]
pub struct VerifyAcksArgs {
    /// The inventory of the deployed cluster to read
    #[clap(long, short)]
    pub inventory: Option<PathBuf>,
    /// A single node's client address, for a node started by hand
    #[clap(long)]
    pub addr: Option<String>,
    /// The file `bench --acks` wrote
    #[clap(long)]
    pub acks: PathBuf,
    /// The level the reads are served at
    #[clap(long, value_enum, default_value = "quorum")]
    pub read: Level,
    /// Read through every member in turn rather than spreading the reads over them
    #[clap(long)]
    pub every_member: bool,
    /// Read through this member alone, by its position in the connect order printed at the start
    #[clap(long)]
    pub member: Option<usize>,
    /// How many ids one get asks for
    #[clap(long, default_value_t = READ_BACK_CHUNK)]
    pub chunk: usize,
    /// How long the client retries a bundle under one identity, in seconds; zero sends each once
    #[clap(long, default_value_t = 30)]
    pub retry_secs: u64,
}

/// Read a set of movie ids through one member and return the rows found
///
/// # Arguments
///
/// * `client` - The member to read through
/// * `ids` - The ids to read
/// * `options` - How the reads are served
/// * `chunk` - How many ids one get asks for
/// * `retry` - How long the client retries a bundle under one identity, if at all
async fn read_movies(
    client: &Shoal<TmdbClient>,
    ids: &[u64],
    options: &SendOptions,
    chunk: usize,
    retry: Option<Duration>,
) -> color_eyre::Result<HashMap<u64, Movie>> {
    let mut found = HashMap::with_capacity(ids.len());
    // one get per chunk, a few chunks to a bundle
    for bundle in ids.chunks(chunk * 8) {
        let mut queries = Queries::<TmdbClient>::default();
        for chunk in bundle.chunks(chunk) {
            queries.add_mut(MovieGet::new(chunk.to_vec()));
        }
        // the reads are retried by the client under one identity if a member is failing over
        let options = match retry {
            Some(within) => options.clone().retry(within),
            None => options.clone(),
        };
        let mut results = client
            .send_with(queries, &options)
            .await
            .map_err(|error| eyre!("a read back failed: {error}"))?;
        while let Some(response) = results
            .next()
            .await
            .map_err(|error| eyre!("a read back failed: {error}"))?
        {
            if let Some(error) = response.error() {
                bail!("a read back failed with {:?}", error.code());
            }
            if let Some(rows) = response.access::<Movie>()? {
                for row in rows.iter() {
                    let movie: Movie = rkyv::deserialize::<Movie, RkyvError>(row)?;
                    found.insert(movie.id, movie);
                }
            }
        }
    }
    Ok(found)
}

/// Check that every acknowledged synthetic insert is there
///
/// # Arguments
///
/// * `args` - What to check
pub async fn verify_acks(args: VerifyAcksArgs) -> color_eyre::Result<()> {
    // every acknowledged id, once
    let raw = std::fs::read_to_string(&args.acks)
        .wrap_err_with(|| format!("failed to read {}", args.acks.display()))?;
    let mut ids: Vec<u64> = raw
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.trim().parse::<u64>())
        .collect::<Result<_, _>>()
        .wrap_err("the ack file holds a line that is not an id")?;
    ids.sort_unstable();
    ids.dedup();
    let clients = connect_targets(args.inventory.as_ref(), args.addr.as_deref()).await?;
    let options = Level::options(Some(args.read));
    // read through each member in turn, or spread over them
    let rounds: Vec<Vec<usize>> = if let Some(member) = args.member {
        vec![vec![member]]
    } else if args.every_member {
        (0..clients.len()).map(|member| vec![member]).collect()
    } else {
        vec![(0..clients.len()).collect()]
    };
    let mut lost_total = 0;
    for members in rounds {
        let started = Instant::now();
        let mut found = 0;
        let mut lost = Vec::new();
        // split the ids over the members of this round
        let share = ids.len().div_ceil(members.len()).max(1);
        for (member, slice) in members.iter().zip(ids.chunks(share)) {
            let rows = read_movies(
                &clients[*member],
                slice,
                &options,
                args.chunk,
                (args.retry_secs > 0).then(|| Duration::from_secs(args.retry_secs)),
            )
            .await?;
            for id in slice {
                match rows.get(id) {
                    Some(movie) if *movie == synthetic_movie(*id) => found += 1,
                    Some(_) => lost.push((*id, "changed")),
                    None => lost.push((*id, "missing")),
                }
            }
        }
        println!(
            "read {} acknowledged inserts through member(s) {members:?} in {:.1?}: {found} found, {} lost",
            ids.len(),
            started.elapsed(),
            lost.len()
        );
        for (id, why) in lost.iter().take(20) {
            println!("  {id} {why}");
        }
        lost_total += lost.len();
    }
    if lost_total > 0 {
        bail!("{lost_total} acknowledged writes were not read back");
    }
    Ok(())
}

/// Read the whole dataset back and compare it with the csv
#[derive(Args, Debug, Clone)]
#[command(group(ArgGroup::new("target").required(true).args(["inventory", "addr"])))]
pub struct VerifyArgs {
    /// The inventory of the deployed cluster to read
    #[clap(long, short)]
    pub inventory: Option<PathBuf>,
    /// A single node's client address, for a node started by hand
    #[clap(long)]
    pub addr: Option<String>,
    /// The csv the cluster was loaded from
    #[clap(long)]
    pub dataset: PathBuf,
    /// Only the first this many movies, if the load was limited
    #[clap(long)]
    pub limit: Option<usize>,
    /// The level the reads are served at
    #[clap(long, value_enum, default_value = "quorum")]
    pub read: Level,
    /// Check movies only, not keyword partitions
    #[clap(long)]
    pub movies_only: bool,
    /// How many members read in parallel, each a share of the ids
    #[clap(long, default_value_t = 4)]
    pub parallel: usize,
    /// Read through this member alone, by its place in the connect order printed at the start,
    /// so with `--read one` it is that member's own copy that is compared with the csv
    #[clap(long)]
    pub member: Option<usize>,
}

/// A hash of one keyword row's sort key, so a partition's rows compare as a set of numbers
///
/// # Arguments
///
/// * `order` - The row's sort key
fn order_hash(order: &str) -> u64 {
    // any stable hash will do; this one is in std
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    order.hash(&mut hasher);
    hasher.finish()
}

/// Check every movie and keyword row the csv should have put in the cluster
///
/// # Arguments
///
/// * `args` - What to check
pub async fn verify(args: VerifyArgs) -> color_eyre::Result<()> {
    // the first pass: every csv row of each id, since the csv holds some ids more than once and
    // the loader deals rows out to workers round robin, so whichever of an id's rows is written
    // last is not fixed. every row of every id contributed keyword rows, since an insert of a
    // new sort key does not remove the old one
    let started = Instant::now();
    let mut reader = csv::Reader::from_path(&args.dataset)
        .wrap_err_with(|| format!("failed to open {}", args.dataset.display()))?;
    let mut expected: HashMap<u64, Vec<Movie>> = HashMap::new();
    let mut keywords: HashMap<String, HashSet<u64>> = HashMap::new();
    let mut read = 0;
    for row in reader.deserialize::<Movie>() {
        let Ok(movie) = row else { continue };
        read += 1;
        for keyword_row in MovieByKeyword::rows(&movie) {
            keywords
                .entry(keyword_row.keyword)
                .or_default()
                .insert(order_hash(&keyword_row.order));
        }
        expected.entry(movie.id).or_default().push(movie);
        if Some(read) == args.limit {
            break;
        }
    }
    let keyword_rows: usize = keywords.values().map(HashSet::len).sum();
    let duplicated = expected.values().filter(|rows| rows.len() > 1).count();
    println!(
        "expecting {} movies ({duplicated} of them on more than one csv row) and {keyword_rows} \
         keyword rows under {} keywords (read in {:.1?})",
        expected.len(),
        keywords.len(),
        started.elapsed()
    );
    let mut clients = connect_targets(args.inventory.as_ref(), args.addr.as_deref()).await?;
    // one member alone, when asked: its own copy is what a `One` read through it serves
    if let Some(member) = args.member {
        let Some(client) = clients.get(member).cloned() else {
            bail!("there is no member {member}: {} connected", clients.len());
        };
        clients = vec![client];
    }
    let options = Level::options(Some(args.read));
    // movies: split the ids over parallel readers, each through a member
    let started = Instant::now();
    let mut ids: Vec<u64> = expected.keys().copied().collect();
    ids.sort_unstable();
    let expected = Arc::new(expected);
    let share = ids.len().div_ceil(args.parallel.max(1)).max(1);
    let mut tasks = Vec::new();
    for (index, slice) in ids.chunks(share).enumerate() {
        let client = clients[index % clients.len()].clone();
        let slice = slice.to_vec();
        let options = options.clone();
        let expected = expected.clone();
        tasks.push(tokio::spawn(async move {
            let rows = read_movies(
                &client,
                &slice,
                &options,
                READ_BACK_CHUNK,
                Some(Duration::from_secs(30)),
            )
            .await?;
            let mut missing = Vec::new();
            let mut changed = Vec::new();
            let mut earlier = 0;
            for id in &slice {
                // the csv rows this id may hold, the last one first
                let candidates = expected.get(id).map_or(&[][..], Vec::as_slice);
                match rows.get(id) {
                    Some(movie) if candidates.last() == Some(movie) => (),
                    // an id on several csv rows may end on any of them
                    Some(movie) if candidates.contains(movie) => earlier += 1,
                    Some(_) => changed.push(*id),
                    None => missing.push(*id),
                }
            }
            Ok::<_, color_eyre::Report>((missing, changed, earlier))
        }));
    }
    let mut missing = Vec::new();
    let mut changed = Vec::new();
    let mut earlier = 0;
    for task in tasks {
        let (m, c, e) = task.await.map_err(|error| eyre!("a reader panicked: {error}"))??;
        missing.extend(m);
        changed.extend(c);
        earlier += e;
    }
    println!(
        "read {} movies back in {:.1?}: {} missing, {} different from every csv row of their id \
         ({earlier} duplicated ids hold one of their earlier rows)",
        ids.len(),
        started.elapsed(),
        missing.len(),
        changed.len()
    );
    for id in missing.iter().take(10) {
        println!("  missing {id}");
    }
    for id in changed.iter().take(10) {
        println!("  changed {id}");
    }
    // keyword partitions: read each whole and compare its set of sort keys
    let mut keyword_failures = Vec::new();
    if !args.movies_only {
        let started = Instant::now();
        let mut names: Vec<String> = keywords.keys().cloned().collect();
        names.sort_unstable();
        let keywords = Arc::new(keywords);
        let share = names.len().div_ceil(args.parallel.max(1)).max(1);
        let mut tasks = Vec::new();
        for (index, slice) in names.chunks(share).enumerate() {
            let client = clients[index % clients.len()].clone();
            let slice = slice.to_vec();
            let options = options.clone().retry(Duration::from_secs(30));
            let keywords = keywords.clone();
            tasks.push(tokio::spawn(async move {
                let mut failures = Vec::new();
                for bundle in slice.chunks(64) {
                    let mut queries = Queries::<TmdbClient>::default();
                    for keyword in bundle {
                        queries.add_mut(MovieByKeywordGet::new(vec![keyword.clone()]));
                    }
                    let mut results = client
                        .send_with(queries, &options)
                        .await
                        .map_err(|error| eyre!("a keyword read failed: {error}"))?;
                    while let Some(response) = results
                        .next()
                        .await
                        .map_err(|error| eyre!("a keyword read failed: {error}"))?
                    {
                        let keyword = &bundle[response.get_index()];
                        if let Some(error) = response.error() {
                            failures.push(format!("{keyword}: {:?}", error.code()));
                            continue;
                        }
                        let mut seen = HashSet::new();
                        if let Some(rows) = response.access::<MovieByKeyword>()? {
                            for row in rows.iter() {
                                seen.insert(order_hash(row.order.as_str()));
                            }
                        }
                        if keywords.get(keyword) != Some(&seen) {
                            failures.push(format!(
                                "{keyword}: {} rows, expected {}",
                                seen.len(),
                                keywords.get(keyword).map_or(0, HashSet::len)
                            ));
                        }
                    }
                }
                Ok::<_, color_eyre::Report>(failures)
            }));
        }
        for task in tasks {
            keyword_failures.extend(task.await.map_err(|error| eyre!("a reader panicked: {error}"))??);
        }
        println!(
            "read {} keyword partitions back in {:.1?}: {} different",
            names.len(),
            started.elapsed(),
            keyword_failures.len()
        );
        for failure in keyword_failures.iter().take(10) {
            println!("  {failure}");
        }
    }
    if !missing.is_empty() || !changed.is_empty() || !keyword_failures.is_empty() {
        bail!(
            "the cluster disagrees with the csv: {} missing, {} changed movies, {} keyword partitions",
            missing.len(),
            changed.len(),
            keyword_failures.len()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A mix is kinds and weights, and a mix with no weight is refused
    #[test]
    fn a_mix_names_kinds_and_weights() {
        // the default parses to every kind
        let mix = parse_mix("get:70,keyword:15,update:10,insert:5").expect("the default mix");
        assert_eq!(mix.0.len(), 4);
        assert_eq!(mix.0[0], (OpKind::Get, 70));
        // an unknown kind, a missing weight and an empty mix are each refused
        assert!(parse_mix("scan:5").is_err());
        assert!(parse_mix("get").is_err());
        assert!(parse_mix("get:0").is_err());
    }

    /// A synthetic id can never be a dataset id, and a synthetic movie is a function of its id
    #[test]
    fn synthetic_movies_are_apart_from_the_dataset() {
        // TMDB ids are far below the base
        assert!(SYNTHETIC_BASE > 100_000_000);
        // the same id builds the same movie, which is what `verify-acks` compares against
        assert_eq!(synthetic_movie(SYNTHETIC_BASE), synthetic_movie(SYNTHETIC_BASE));
        assert_ne!(synthetic_movie(SYNTHETIC_BASE), synthetic_movie(SYNTHETIC_BASE + 1));
    }
}
