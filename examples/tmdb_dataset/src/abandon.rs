//! `abandon`: readers that stop reading, driven through every member of a deployed cluster
//!
//! The lab's check of client cancels ([F75](../../../docs/src/features/client-cancel.md)). Workers
//! ask for bundles of wide answers - several gets, each of several movies whose overviews are made
//! long - read the first answer or so of each, and drop the stream, the way a reader that seeks
//! away or gives up does. A client that cancels tells the server, which stops writing what is left
//! and runs none of it that has not run; one built not to leaves the server to finish every
//! bundle into a socket whose reader throws it away. A foreground worker asks for one small movie
//! at a time beside them, which is what the abandoned work costs everyone else.
//!
//! Each member's cancel counters and answer bytes are read before and after, and the run prints
//! what the members wrote and what they were spared. The movies are synthetic, at ids from
//! [`ABANDON_BASE`], so `verify` still compares the dataset with the csv afterwards. They are
//! left in place, so a second run reuses them.

use clap::{ArgGroup, Args};
use color_eyre::eyre::{bail, eyre, WrapErr};
use shoal::client::{Routing, Shoal};
use shoal::shared::auth::Credentials;
use shoal::shared::protocol::stats::{CancelCounters, NodeStats};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::bench::synthetic_movie;
use crate::{MovieGet, TmdbClient};

/// The first id an `abandon` run's movies are written at, far above the dataset's, the driver's
/// synthetic inserts and `contend`'s counters
pub const ABANDON_BASE: u64 = 1 << 52;

/// Drop result streams early, with and without cancels, and print what the members were spared
#[derive(Args, Debug, Clone)]
#[command(group(ArgGroup::new("target").required(true).args(["inventory", "addr"])))]
pub struct AbandonArgs {
    /// The inventory of the deployed cluster to drive
    #[clap(long, short)]
    pub inventory: Option<PathBuf>,
    /// A single node's client address, for a node started by hand
    #[clap(long)]
    pub addr: Option<String>,
    /// Whether a dropped stream cancels what it is still owed
    #[clap(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub cancel: bool,
    /// How many wide movies the gets read from
    #[clap(long, default_value_t = 256)]
    pub movies: u64,
    /// The bytes of each wide movie's overview
    #[clap(long, default_value_t = 64 * 1024)]
    pub overview_bytes: usize,
    /// How many gets a bundle carries, each answered on its own
    #[clap(long, default_value_t = 16)]
    pub gets: usize,
    /// How many movies each get asks for
    #[clap(long, default_value_t = 16)]
    pub per_get: usize,
    /// How many answers a worker reads before it drops the stream
    #[clap(long, default_value_t = 1)]
    pub read: usize,
    /// How many workers abandon bundles, spread over every member
    #[clap(long, default_value_t = 6)]
    pub workers: usize,
    /// How many seconds the workers run
    #[clap(long, default_value_t = 20)]
    pub duration: u64,
    /// Skip writing the wide movies, which a run before this one already wrote
    #[clap(long)]
    pub reuse: bool,
}

/// What one worker did
#[derive(Debug, Default)]
struct Tally {
    /// Bundles sent and dropped
    bundles: u64,
    /// Answers read before each was dropped
    answers: u64,
    /// Requests that failed
    failed: u64,
    /// The foreground's waits, in microseconds
    waits: Vec<u64>,
}

/// Connect to every member, as the admin of a deployed cluster or as nobody to one node
///
/// # Arguments
///
/// * `args` - Where the cluster is, and whether its clients cancel
async fn connect(args: &AbandonArgs) -> color_eyre::Result<Vec<Arc<Shoal<TmdbClient>>>> {
    // the client every member is reached through, cancelling as asked
    let build = |addr: String, credentials: Option<Credentials>| {
        let cancel = args.cancel;
        async move {
            let mut builder = Shoal::<TmdbClient>::builder()
                .endpoint(&addr)
                .routing(Routing::Endpoints)
                .cancel_abandoned(cancel);
            if let Some(credentials) = credentials {
                builder = builder.credentials(credentials);
            }
            builder
                .build()
                .await
                .map(Arc::new)
                .map_err(|error| eyre!("could not connect to {addr}: {error:?}"))
        }
    };
    match (&args.inventory, &args.addr) {
        // a deployed cluster, as its admin
        (Some(inventory), None) => {
            let deployment = shoaladm::deploy::Deployment::attach(inventory)?;
            let record = deployment.state.record()?;
            let password = deployment.state.password()?;
            let mut clients = Vec::with_capacity(record.nodes.len());
            for (name, node) in &record.nodes {
                let address: std::net::IpAddr = node.address.parse().wrap_err_with(|| {
                    format!("{name} was recorded with the address {:?}", node.address)
                })?;
                let addr =
                    shoaladm::deploy::inventory::socket(address, deployment.inventory.ports.client);
                let credentials =
                    Credentials::scram(deployment.inventory.admin.clone(), password.clone());
                clients.push(build(addr.clone(), Some(credentials)).await?);
                println!("connected to {name} at {addr}");
            }
            if clients.is_empty() {
                bail!("the inventory has no deployed nodes");
            }
            Ok(clients)
        }
        // one node, as nobody
        (None, Some(addr)) => Ok(vec![build(addr.clone(), None).await?]),
        // clap's group makes one of the two required, and only one
        _ => Err(eyre!("give --inventory or --addr")),
    }
}

/// Each member's own figures, read through the member
///
/// # Arguments
///
/// * `clients` - A client to each member
async fn figures(clients: &[Arc<Shoal<TmdbClient>>]) -> color_eyre::Result<Vec<NodeStats>> {
    let mut all = Vec::with_capacity(clients.len());
    for client in clients {
        // the member's own row of the view it answers
        let view = shoaladm::cluster::stats::read_stats(client, None)
            .await
            .map_err(|error| eyre!("reading a member's figures: {error}"))?;
        let own = view
            .members
            .iter()
            .find(|member| member.node == view.answered_by)
            .and_then(|member| member.stats.clone())
            .ok_or_else(|| eyre!("a member answered no figures of its own"))?;
        all.push(own);
    }
    Ok(all)
}

/// The answer bytes and cancel counters of some figures, summed over the members
///
/// # Arguments
///
/// * `figures` - Each member's figures
fn totals(figures: &[NodeStats]) -> (u64, CancelCounters) {
    let mut bytes = 0u64;
    let mut cancels = CancelCounters::default();
    for stats in figures {
        bytes += stats
            .queries
            .ops
            .iter()
            .map(|op| op.bytes_out_total)
            .sum::<u64>();
        cancels.absorb(&stats.cancels.totals);
    }
    (bytes, cancels)
}

/// One worker: send a bundle of wide gets, read a few answers, drop the rest, until told to stop
///
/// # Arguments
///
/// * `client` - The member this worker reads through
/// * `args` - The bundles' shape
/// * `seed` - Where in the movies this worker starts
/// * `until` - When to stop
async fn abandon_worker(
    client: Arc<Shoal<TmdbClient>>,
    args: AbandonArgs,
    seed: u64,
    until: Instant,
) -> Tally {
    let mut tally = Tally::default();
    let mut next = seed;
    while Instant::now() < until {
        // a bundle of gets, each of several wide movies, walking the movies in turn
        let mut queries = client.query();
        for _ in 0..args.gets {
            let ids = (0..args.per_get as u64)
                .map(|offset| ABANDON_BASE + (next + offset) % args.movies)
                .collect();
            next += args.per_get as u64;
            queries = queries.add(MovieGet::new(ids));
        }
        // read the first answers, then let the stream go with the rest still owed
        match client.send(queries).await {
            Ok(mut stream) => {
                for _ in 0..args.read {
                    match stream.next().await {
                        Ok(Some(_)) => tally.answers += 1,
                        Ok(None) => break,
                        Err(_) => {
                            tally.failed += 1;
                            break;
                        }
                    }
                }
                drop(stream);
                tally.bundles += 1;
            }
            Err(_) => tally.failed += 1,
        }
    }
    tally
}

/// The foreground: one small movie at a time, timed, until told to stop
///
/// # Arguments
///
/// * `client` - The member it reads through
/// * `until` - When to stop
async fn foreground(client: Arc<Shoal<TmdbClient>>, until: Instant) -> Tally {
    let mut tally = Tally::default();
    let small = ABANDON_BASE - 1;
    while Instant::now() < until {
        let started = Instant::now();
        match client.send_one(MovieGet::new(vec![small])).await {
            Ok(_) => {
                tally.answers += 1;
                tally
                    .waits
                    .push(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX));
            }
            Err(_) => tally.failed += 1,
        }
    }
    tally
}

/// A percentile of some waits in microseconds, as milliseconds
///
/// # Arguments
///
/// * `waits` - The waits, sorted
/// * `quantile` - The quantile, between zero and one
#[allow(clippy::cast_precision_loss)]
fn percentile_ms(waits: &[u64], quantile: f64) -> f64 {
    if waits.is_empty() {
        return 0.0;
    }
    let at = ((waits.len() - 1) as f64 * quantile).round() as usize;
    waits[at] as f64 / 1000.0
}

/// Drop result streams early through every member and print what the members were spared
///
/// # Arguments
///
/// * `args` - What to drive and through which cluster
///
/// # Errors
///
/// When the cluster cannot be reached or a member's figures cannot be read.
#[allow(clippy::cast_precision_loss)]
pub async fn abandon(args: AbandonArgs) -> color_eyre::Result<()> {
    let clients = connect(&args).await?;
    // the wide movies, and one small one for the foreground
    if !args.reuse {
        let overview = "o".repeat(args.overview_bytes);
        for chunk in (0..args.movies).collect::<Vec<_>>().chunks(16) {
            let mut queries = clients[0].query();
            for offset in chunk {
                let mut movie = synthetic_movie(ABANDON_BASE + offset);
                movie.overview.clone_from(&overview);
                queries = queries.add(movie);
            }
            clients[0]
                .exec(queries)
                .await
                .map_err(|error| eyre!("writing the wide movies: {error:?}"))?;
        }
        clients[0]
            .send_one(synthetic_movie(ABANDON_BASE - 1))
            .await
            .map_err(|error| eyre!("writing the small movie: {error:?}"))?;
    }
    // where every member's counters stand before the run
    let (bytes_before, cancels_before) = totals(&figures(&clients).await?);
    // the workers, over every member, and the foreground on the first
    let started = Instant::now();
    let until = started + Duration::from_secs(args.duration);
    let mut tasks = Vec::with_capacity(args.workers);
    for index in 0..args.workers.max(1) {
        let client = clients[index % clients.len()].clone();
        let seed = index as u64 * 7919;
        tasks.push(tokio::spawn(abandon_worker(
            client,
            args.clone(),
            seed,
            until,
        )));
    }
    let front = tokio::spawn(foreground(clients[0].clone(), until));
    let mut total = Tally::default();
    for task in tasks {
        let tally = task
            .await
            .map_err(|error| eyre!("a worker panicked: {error}"))?;
        total.bundles += tally.bundles;
        total.answers += tally.answers;
        total.failed += tally.failed;
    }
    let mut front = front
        .await
        .map_err(|error| eyre!("the foreground panicked: {error}"))?;
    let elapsed = started.elapsed().as_secs_f64();
    // the counters the members report a tick or two late, once the last bundles are done
    tokio::time::sleep(Duration::from_secs(4)).await;
    let (bytes_after, cancels_after) = totals(&figures(&clients).await?);
    let cancels = cancels_after.since(&cancels_before);
    let bytes = bytes_after.saturating_sub(bytes_before);
    front.waits.sort_unstable();
    let mib = |bytes: u64| bytes as f64 / (1024.0 * 1024.0);
    println!(
        "cancel {}: {} workers over {} members dropped {} bundles of {}x{} movies in {elapsed:.1}s \
         ({:.1}/s), reading {} answers, {} failed",
        if args.cancel { "on" } else { "off" },
        args.workers.max(1),
        clients.len(),
        total.bundles,
        args.gets,
        args.per_get,
        total.bundles as f64 / elapsed,
        total.answers,
        total.failed,
    );
    println!(
        "members wrote {:.1} MiB of answers ({:.1} MiB/s); cancels {} received, {} forwarded, {} \
         queries refused, {} answers dropped ({:.1} MiB unwritten), {} streams cut, {} unrecorded",
        mib(bytes),
        mib(bytes) / elapsed,
        cancels.received,
        cancels.forwarded,
        cancels.refused,
        cancels.dropped,
        mib(cancels.dropped_bytes),
        cancels.cut,
        cancels.unrecorded,
    );
    println!(
        "foreground: {} reads ({:.0}/s), p50 {:.2} ms, p99 {:.2} ms, {} failed",
        front.answers,
        front.answers as f64 / elapsed,
        percentile_ms(&front.waits, 0.5),
        percentile_ms(&front.waits, 0.99),
        front.failed,
    );
    Ok(())
}
