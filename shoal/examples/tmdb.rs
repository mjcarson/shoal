//! A tour of Shoal, in one file that runs with no setup
//!
//! ```sh
//! cargo run --example tmdb
//! ```
//!
//! No config file, no dataset, no flags. The example starts a server against a temporary
//! directory, writes a dozen movies into it, and reads them back four different ways.
//!
//! # What it shows, in the order it shows it
//!
//! 1. **Two table types.** [`Movie`] is unsorted - one row per partition, keyed by its id.
//!    [`MovieByKeyword`] is sorted - many rows per partition, ordered by a sort key within it.
//! 2. **A projection.** [`MovieSummary`] names three fields of a movie, and a get answered with it
//!    reads only those fields out of the archive instead of every field of every row.
//! 3. **A filter.** A get can narrow itself to rows whose field is in a set.
//! 4. **SHQL.** The same query, written as text and parsed.
//!
//! # What it used to be
//!
//! This example was 1,247 lines, and most of them were a benchmark harness: an eighteen flag
//! command line, a worker pool with an in flight gate, latency histograms, baseline comparison,
//! core pinning, and both halves of two profiling instrumentations. It also needed a 65 MB CSV at
//! a hard coded absolute path that was not in this repository and that no script fetched, so a
//! clean checkout could not run it at all.
//!
//! All of that now lives in `shoal-bench` as purpose built workloads, which measure the paths
//! through the engine one at a time instead of measuring all of them at once. See
//! `docs/src/features/purpose-built-workloads.md`.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::server::conf::{DefaultStorageSettings, Networking, Resources, Storage, TraceLevel, Tracing, OtlpTracing};
use shoal::server::tables::storage::fs::conf::{
    FileSystemLatencyWriterConf, FileSystemTableConf, FileSystemThroughputWriterConf,
};
use shoal::shared::queries::Queries;
use shoal::{
    Conf, Errors, FileSystem, PersistentSortedTable, PersistentUnsortedTable, Shoal, ShoalPool,
    ShoalProjection, ShoalSortedTable, ShoalUnsortedTable,
};

/// A movie, stored one per partition
///
/// `#[shoal(partition)]` is the only required attribute: it names the field a row is placed and
/// found by. `#[shoal(filter)]` makes a field something a get can narrow on, and
/// `#[shoal(update)]` makes one something an update can change.
#[derive(
    Debug, Clone, PartialEq, Archive, Serialize, Deserialize, ShoalUnsortedTable, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Tmdb")]
pub struct Movie {
    /// The id this movie is partitioned by
    #[shoal(partition)]
    pub id: u64,
    /// The title, which a get can filter on
    #[shoal(filter)]
    pub title: String,
    /// The year it came out, which a get can filter on
    #[shoal(filter)]
    pub year: u64,
    /// A one line summary, which an update can change
    #[shoal(update)]
    pub tagline: String,
    /// Its average rating out of ten
    pub vote_average: f64,
}

/// A movie listed under one of its keywords, stored many per partition
///
/// A sorted table adds `#[shoal(sort)]`, which orders the rows *within* a partition. That is what
/// makes "every movie tagged `alien`, in title order" a range of a single partition rather than a
/// scan of the whole table.
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

/// Three fields of a movie, for listing them
///
/// A movie is a wide row and listing one needs very little of it. A get answered with a projection
/// copies only the fields named here out of the archive it read, instead of deserializing every
/// field of every row and throwing most of them away.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize, ShoalProjection, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_projection(table = "Movie")]
pub struct MovieSummary {
    /// The id this movie was partitioned by
    #[shoal(partition)]
    pub id: u64,
    /// The title
    pub title: String,
    /// The rating
    pub vote_average: f64,
}

/// The database
///
/// `#[shoal::db]` reads this struct and generates the client type, the query enum and the response
/// enum for it. `#[shoal(projections(...))]` registers which projections a table can be read with.
#[shoal::db]
pub struct Tmdb {
    /// Movies, one per partition
    #[shoal(projections(MovieSummary))]
    pub movie: PersistentUnsortedTable<Movie, FileSystem>,
    /// Movies by keyword, many per partition
    pub movie_by_keyword: PersistentSortedTable<MovieByKeyword, FileSystem>,
}

/// The movies this example writes, with the keywords each is listed under
const MOVIES: [(u64, &str, u64, &str, f64, &[&str]); 12] = [
    (
        78,
        "Blade Runner",
        1982,
        "Man has made his match... now it's his problem.",
        8.1,
        &["android", "dystopia", "noir"],
    ),
    (
        348,
        "Alien",
        1979,
        "In space no one can hear you scream.",
        8.2,
        &["alien", "space", "horror"],
    ),
    (
        679,
        "Aliens",
        1986,
        "This time it's war.",
        7.9,
        &["alien", "space", "war"],
    ),
    (
        218,
        "The Terminator",
        1984,
        "Your future is in his hands.",
        7.7,
        &["android", "time-travel"],
    ),
    (
        280,
        "Terminator 2",
        1991,
        "It's nothing personal.",
        8.1,
        &["android", "time-travel"],
    ),
    (
        62,
        "2001: A Space Odyssey",
        1968,
        "An epic drama of adventure and exploration.",
        8.1,
        &["space", "ai"],
    ),
    (
        1892,
        "Return of the Jedi",
        1983,
        "The Empire falls.",
        7.9,
        &["space", "war"],
    ),
    (
        11,
        "Star Wars",
        1977,
        "A long time ago in a galaxy far, far away...",
        8.2,
        &["space", "war"],
    ),
    (
        601,
        "E.T.",
        1982,
        "He is afraid. He is alone. He is three million light years from home.",
        7.5,
        &["alien", "family"],
    ),
    (
        813,
        "Close Encounters",
        1977,
        "We are not alone.",
        7.4,
        &["alien", "ufo"],
    ),
    (
        152,
        "Star Trek: The Motion Picture",
        1979,
        "The human adventure is just beginning.",
        6.4,
        &["space", "alien"],
    ),
    (
        105,
        "Back to the Future",
        1985,
        "He was never in time for his classes...",
        8.3,
        &["time-travel", "comedy"],
    ),
];

/// Builds a config that stores its data in a temporary directory
///
/// A real deployment loads this from `shoal.yml` with `Conf::from_file`. Building it in code is
/// what lets this example run with nothing set up.
///
/// # Arguments
///
/// * `dir` - The directory to store this server's data in
fn config(dir: &std::path::Path) -> Conf {
    // two shards is enough to show partitions being routed without asking for a whole machine
    Conf::default()
        .resources(
            Resources::default()
                .cores(2)
                .memory("256MiB")
                .expect("256MiB is a valid memory size"),
        )
        .networking(Networking::default().port(12345))
        // a remote sink only if one was named, so the example still runs against nothing
        //
        // `SHOAL_OTLP_ENDPOINT` is the full URL including `/v1/traces`, and `SHOAL_OTLP_TENANT`
        // is the `X-Scope-OrgID` a multi tenant collector reads its tenant out of
        .tracing(tracing_conf())
        .storage(
            Storage::default().default_settings(
                DefaultStorageSettings::default().filesystem(
                    FileSystemTableConf::default()
                        .latency_sensitive(FileSystemLatencyWriterConf::default().path(dir))
                        .throughput_sensitive(FileSystemThroughputWriterConf::default().path(dir)),
                ),
            ),
        )
}

/// Builds the tracing settings this run uses
///
/// The example writes to stdout with no configuration at all. It exports to a collector only when
/// `SHOAL_OTLP_ENDPOINT` names one, so that pointing the example at a Tempo or an OTel collector
/// is a shell variable rather than an edit:
///
/// ```bash
/// SHOAL_OTLP_ENDPOINT=http://127.0.0.1:4318/v1/traces \
///     SHOAL_OTLP_TENANT=Shoal cargo run --example tmdb
/// ```
fn tracing_conf() -> Tracing {
    // everything gets a stdout layer at Info
    let tracing = Tracing::default().level(TraceLevel::Info);
    // and a remote sink only when one was named
    let Ok(endpoint) = std::env::var("SHOAL_OTLP_ENDPOINT") else {
        return tracing;
    };
    // point an OTLP over HTTP exporter at it
    let mut otlp = OtlpTracing::new(endpoint);
    // a multi tenant collector rejects an export that names no tenant, so pass one if we have it
    if let Ok(tenant) = std::env::var("SHOAL_OTLP_TENANT") {
        otlp = otlp.header("X-Scope-OrgID", tenant);
    }
    tracing.otlp(otlp)
}

/// Writes every movie, and a row per keyword for each of them
///
/// Both tables are written in one batch. A `Queries` bundle may mix tables and query kinds freely;
/// the server routes each one to whichever shard owns its partition.
///
/// # Arguments
///
/// * `client` - The client to write with
async fn write_movies(client: &Shoal<TmdbClient>) -> Result<(), Errors> {
    // one bundle for everything, since a batch costs one round trip however much is in it
    let mut batch: Queries<TmdbClient> = client.query();
    for (id, title, year, tagline, vote_average, keywords) in MOVIES {
        // a row converts straight into a query, so inserting is just adding the row itself
        batch.add_mut(Movie {
            id,
            title: title.to_string(),
            year,
            tagline: tagline.to_string(),
            vote_average,
        });
        // and one row per keyword, into the sorted table
        for keyword in keywords {
            batch.add_mut(MovieByKeyword {
                keyword: (*keyword).to_string(),
                title: title.to_string(),
            });
        }
    }
    // drain the responses, which is what makes the writes durable before we read them back
    let mut written = 0;
    let mut responses = client.send(batch).await?;
    while responses.next().await?.is_some() {
        written += 1;
    }
    println!("wrote {written} rows\n");
    Ok(())
}

/// Reads one movie back by its id
///
/// # Arguments
///
/// * `client` - The client to read with
async fn read_one(client: &Shoal<TmdbClient>) -> Result<(), Errors> {
    println!("-- a whole row, by partition key --");
    // a get names the partition keys it wants
    let response = client.send_one(MovieGet::new(vec![348])).await?;
    // `access` reads the rows straight out of the archive that came off the wire, with no
    // deserialization at all
    if let Some(rows) = response.access::<Movie>()? {
        for movie in rows.iter() {
            println!("  {} ({}) - {}", movie.title, movie.year, movie.tagline);
        }
    }
    println!();
    Ok(())
}

/// Reads several movies back as a projection
///
/// # Arguments
///
/// * `client` - The client to read with
async fn read_projected(client: &Shoal<TmdbClient>) -> Result<(), Errors> {
    println!("-- three fields of four rows, as a projection --");
    // `.projection::<T>()` asks the server to answer with `T` instead of the whole row
    let response = client
        .send_one(MovieGet::new(vec![11, 62, 105, 348]).projection::<MovieSummary>())
        .await?;
    if let Some(rows) = response.access::<MovieSummary>()? {
        for movie in rows.iter() {
            println!("  {:<24} {:.1}", movie.title, movie.vote_average);
        }
    }
    println!();
    Ok(())
}

/// Reads movies filtered to a set of years
///
/// # Arguments
///
/// * `client` - The client to read with
async fn read_filtered(client: &Shoal<TmdbClient>) -> Result<(), Errors> {
    println!("-- only the rows from 1982, out of six partitions --");
    // a filter is a membership test, one set per filterable field, so `= 1982` and
    // `IN (1982, 1979)` are the same shape. a field left `None` is not filtered on at all.
    let filter = MovieFilter {
        year: Some(vec![1982]),
        ..Default::default()
    };
    let response = client
        .send_one(MovieGet::new(vec![78, 348, 601, 813, 11, 105]).filters(filter))
        .await?;
    if let Some(rows) = response.access::<Movie>()? {
        for movie in rows.iter() {
            println!("  {} ({})", movie.title, movie.year);
        }
    }
    println!();
    Ok(())
}

/// Reads a sorted partition back with a query written as text
///
/// # Arguments
///
/// * `client` - The client to read with
async fn read_with_shql(client: &Shoal<TmdbClient>) -> Result<(), Errors> {
    println!("-- every movie tagged 'alien', in title order, via SHQL --");
    // the same query the typed builders make, parsed from text
    let query = client
        .query()
        .parse("select * from MovieByKeyword where keyword = 'alien'")?;
    let mut response = client.send(query).await?;
    while let Some(row) = response.next().await? {
        if let Some(rows) = row.access::<MovieByKeyword>()? {
            // a sorted table returns its rows in sort key order, which here is by title
            for tagged in rows.iter() {
                println!("  {}", tagged.title);
            }
        }
    }
    println!();
    Ok(())
}

/// Starts a server, writes some movies, and reads them back four ways
#[tokio::main]
async fn main() -> Result<(), Errors> {
    // a temporary directory under `target/`, not `/tmp`
    //
    // glommio silently disables O_DIRECT on tmpfs, and `/tmp` usually is one, so a server rooted
    // there quietly runs a buffered write path instead of the one it was built for
    let dir =
        tempfile::TempDir::new_in(target_dir()).expect("failed to create a temporary directory");
    let conf = config(dir.path());
    let addr = format!("127.0.0.1:{}", conf.networking.port);
    // setup tracing/telemetry. the guard has to outlive every span below it, because dropping
    // it is what flushes whatever the exporter has queued
    let traces = shoal_core::server::trace::setup(&conf);
    // start one shard per configured core. this returns as soon as the shard threads are spawned,
    // which is why the client below retries rather than assuming the server is up.
    let pool = ShoalPool::<Tmdb>::start(conf).expect("failed to start shoal");
    let client = connect(&addr).await?;
    println!();
    // write, then read the same rows back four different ways
    write_movies(&client).await?;
    read_one(&client).await?;
    read_projected(&client).await?;
    read_filtered(&client).await?;
    read_with_shql(&client).await?;
    // stop the shards, which flushes everything still buffered
    pool.exit().expect("failed to stop shoal");
    // shutdown our tracer, which ships whatever the exporter still has queued
    shoal_core::server::trace::shutdown(traces);
    Ok(())
}

/// Finds a directory on a real filesystem to put this run's data in
///
/// Cargo sets `CARGO_TARGET_TMPDIR` for tests and not for examples, so this works it out from
/// where the binary itself is: `target/debug/examples/tmdb` sits two levels below `target/`, which
/// is on the same real filesystem as the repository.
///
/// It matters which filesystem this lands on. Glommio silently falls back to buffered IO when
/// `O_DIRECT` is unavailable, which it is on tmpfs, so an example rooted in `/tmp` would run a
/// different write path from the one a deployment runs and would never say so.
fn target_dir() -> std::path::PathBuf {
    // walk up from the binary until a directory called `target` turns up
    if let Ok(exe) = std::env::current_exe() {
        for ancestor in exe.ancestors() {
            if ancestor.file_name().is_some_and(|name| name == "target") {
                return ancestor.to_path_buf();
            }
        }
    }
    // nothing above the binary looked like a target directory, so fall back to the current
    // directory, which for `cargo run` is the workspace root
    std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
}

/// Connects to a server, retrying while it finishes starting
///
/// # Arguments
///
/// * `addr` - The address the server is coming up on
async fn connect(addr: &str) -> Result<Shoal<TmdbClient>, Errors> {
    // `ShoalPool::start` spawns its shards and returns without waiting for them to bind, so the
    // first connection can arrive before the listener does
    let mut last = None;
    for _ in 0..200 {
        match Shoal::<TmdbClient>::new(addr).await {
            Ok(client) => return Ok(client),
            Err(error) => last = Some(error),
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    Err(last.expect("the loop ran at least once"))
}
