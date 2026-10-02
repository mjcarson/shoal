//! `contend`: conditional writes raced through every member of a deployed cluster
//!
//! The lab's check of conditional writes ([F68](../../../docs/src/features/conditional-writes.md)).
//! Workers spread over every member increment a set of counters by compare and swap - each reads
//! a movie's title, a number, and replaces the movie with the next number only if its title is
//! still the one it read - and every answer is applied or refused by name. Once they stop, every
//! counter read through every member at quorum has to equal the increments applied to it. On the
//! sorted table, every worker races to insert one keyword row expecting none and then to delete
//! it expecting one, round after round, and exactly one of each has to be applied.
//!
//! The movies are synthetic, at ids from [`CONTEND_BASE`], and the keyword partition is named
//! `contend-<run>`, so `verify` still compares the dataset with the csv afterwards. The counters
//! are deleted again at the end.

use clap::{ArgGroup, Args};
use color_eyre::eyre::{bail, eyre};
use shoal::client::{SendOptions, Shoal};
use shoal::shared::protocol::read::ReadLevel;
use shoal::shared::queries::{ConditionRefusal, ConditionalInsert, ConditionalWrite};
use shoal::Errors;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::bench::synthetic_movie;
use crate::load::connect_targets;
use crate::{
    Movie, MovieByKeyword, MovieByKeywordDelete, MovieByKeywordFilter, MovieByKeywordGet,
    MovieDelete, MovieFilter, MovieGet, TmdbClient,
};

/// The first id a `contend` run's counters are written at, far above the dataset's and the
/// driver's synthetic inserts
pub const CONTEND_BASE: u64 = 1 << 50;

/// How far apart two runs' counters are, so a second run never meets a first's
const RUN_STRIDE: u64 = 1 << 20;

/// Race conditional writes through every member and check every one was judged in order
#[derive(Args, Debug, Clone)]
#[command(group(ArgGroup::new("target").required(true).args(["inventory", "addr"])))]
pub struct ContendArgs {
    /// The inventory of the deployed cluster to drive
    #[clap(long, short)]
    pub inventory: Option<PathBuf>,
    /// A single node's client address, for a node started by hand
    #[clap(long)]
    pub addr: Option<String>,
    /// How many counters the workers share
    #[clap(long, default_value_t = 16)]
    pub keys: u64,
    /// How many workers race, spread over every member
    #[clap(long, default_value_t = 24)]
    pub workers: usize,
    /// How many compare and swaps each worker attempts
    #[clap(long, default_value_t = 200)]
    pub rounds: u64,
    /// How many keyword rows the sorted race inserts and deletes, one at a time
    #[clap(long, default_value_t = 20)]
    pub sorted_rounds: u64,
    /// Which run this is, so two runs write apart
    #[clap(long, default_value_t = 0)]
    pub run: u64,
}

/// What a worker saw of its compare and swaps
#[derive(Debug, Default)]
struct Tally {
    /// The increments applied, by counter id
    applied: HashMap<u64, u64>,
    /// The refusals, by reason
    refused: HashMap<ConditionRefusal, u64>,
    /// Every answer that was neither, which a healthy cluster never gives
    failed: Vec<String>,
}

/// Send one conditional write and say whether it was applied, or why it was refused
///
/// # Arguments
///
/// * `client` - The client to send with
/// * `query` - The conditional write
async fn outcome<Q: Into<<TmdbClient as shoal::shared::traits::QuerySupport>::QueryKinds>>(
    client: &Shoal<TmdbClient>,
    query: Q,
) -> Result<Option<ConditionRefusal>, Errors> {
    // a refused write fails its response by name, with why
    match client.send_one(query).await {
        Ok(_) => Ok(None),
        Err(Errors::Refused { reason, .. }) => Ok(Some(reason)),
        Err(error) => Err(error),
    }
}

/// Read one movie's title at quorum, if the movie exists
///
/// # Arguments
///
/// * `client` - The client to read with
/// * `id` - The movie's id
async fn title_of(client: &Shoal<TmdbClient>, id: u64) -> Result<Option<String>, Errors> {
    // a quorum read, so a worker's view is never older than a write it saw answered
    let options = SendOptions::new().read(ReadLevel::Quorum);
    match client.send_one_with(MovieGet::new(vec![id]), &options).await {
        Ok(response) => Ok(response
            .access::<Movie>()?
            .and_then(|rows| rows.first().map(|movie| movie.title.to_string()))),
        // a get that found nothing is no movie
        Err(Errors::QueryDidNotSucceed { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

/// A counter movie at a value
///
/// # Arguments
///
/// * `id` - The counter's id
/// * `value` - The value its title holds
fn counter(id: u64, value: u64) -> Movie {
    // a synthetic movie whose title is the count
    let mut movie = synthetic_movie(id);
    movie.title = value.to_string();
    movie
}

/// Run one worker's compare and swaps
///
/// # Arguments
///
/// * `client` - The member this worker writes through
/// * `index` - This worker's place among the workers
/// * `ids` - The counters every worker shares
/// * `rounds` - How many compare and swaps to attempt
async fn swap_worker(
    client: Arc<Shoal<TmdbClient>>,
    index: usize,
    ids: Arc<Vec<u64>>,
    rounds: u64,
) -> Tally {
    let mut tally = Tally::default();
    for round in 0..rounds {
        // each worker walks the counters from its own place, so all of them are contended
        let id = ids[(index + round as usize) % ids.len()];
        // read the counter as it is now
        let seen = match title_of(&client, id).await {
            Ok(Some(title)) => title,
            Ok(None) => {
                tally.failed.push(format!("counter {id} was not found"));
                continue;
            }
            Err(error) => {
                tally.failed.push(format!("reading counter {id}: {error:?}"));
                continue;
            }
        };
        let Ok(value) = seen.parse::<u64>() else {
            tally.failed.push(format!("counter {id} holds {seen:?}"));
            continue;
        };
        // and move it on only if it still holds what was read
        let swap = counter(id, value + 1).if_matches(MovieFilter {
            title: Some(vec![seen]),
        });
        match outcome(&client, swap).await {
            Ok(None) => *tally.applied.entry(id).or_default() += 1,
            Ok(Some(reason)) => *tally.refused.entry(reason).or_default() += 1,
            Err(error) => tally.failed.push(format!("swapping counter {id}: {error:?}")),
        }
    }
    tally
}

/// Race conditional writes through every member and check every one was judged in order
///
/// # Arguments
///
/// * `args` - What to race and through which cluster
///
/// # Errors
///
/// When the cluster cannot be reached, or any write was answered other than as the committed
/// order says it must be.
pub async fn contend(args: ContendArgs) -> color_eyre::Result<()> {
    // connect to every member, and lay the counters out apart from any other run
    let clients = connect_targets(args.inventory.as_ref(), args.addr.as_deref()).await?;
    let base = CONTEND_BASE + args.run * RUN_STRIDE;
    let ids: Vec<u64> = (base..base + args.keys.max(1)).collect();
    let mut problems = Vec::new();
    // every counter starts at zero
    for id in &ids {
        clients[0]
            .send_one(counter(*id, 0))
            .await
            .map_err(|error| eyre!("seeding counter {id}: {error:?}"))?;
    }
    // a counter that exists refuses an insert expecting none
    let clobber = outcome(&clients[0], counter(ids[0], 999).if_absent()).await;
    if !matches!(clobber, Ok(Some(ConditionRefusal::RowExists))) {
        problems.push(format!("an insert over counter {} was answered {clobber:?}", ids[0]));
    }
    // the race: every worker through a member of its own, round robin
    let started = Instant::now();
    let shared = Arc::new(ids.clone());
    let mut tasks = Vec::with_capacity(args.workers);
    for index in 0..args.workers.max(1) {
        let client = clients[index % clients.len()].clone();
        tasks.push(tokio::spawn(swap_worker(
            client,
            index,
            shared.clone(),
            args.rounds,
        )));
    }
    let mut total = Tally::default();
    for task in tasks {
        let tally = task.await.map_err(|error| eyre!("a worker panicked: {error}"))?;
        for (id, count) in tally.applied {
            *total.applied.entry(id).or_default() += count;
        }
        for (reason, count) in tally.refused {
            *total.refused.entry(reason).or_default() += count;
        }
        total.failed.extend(tally.failed);
    }
    let applied: u64 = total.applied.values().sum();
    let refused: u64 = total.refused.values().sum();
    println!(
        "{} workers over {} members made {} compare and swaps on {} counters in {:.1?}: {applied} \
         applied, {refused} refused {:?}, {} failed",
        args.workers.max(1),
        clients.len(),
        args.workers.max(1) as u64 * args.rounds,
        ids.len(),
        started.elapsed(),
        total.refused,
        total.failed.len()
    );
    for failure in total.failed.iter().take(10) {
        problems.push(failure.clone());
    }
    // a counter can only be refused because another worker moved it
    if total
        .refused
        .keys()
        .any(|reason| *reason != ConditionRefusal::RowMismatch)
    {
        problems.push(format!("a counter was refused for another reason: {:?}", total.refused));
    }
    // every counter, through every member, holds exactly the increments applied to it
    for id in &ids {
        let expected = total.applied.get(id).copied().unwrap_or_default().to_string();
        for (member, client) in clients.iter().enumerate() {
            let seen = title_of(client, *id)
                .await
                .map_err(|error| eyre!("reading counter {id} through member {member}: {error:?}"))?;
            if seen.as_deref() != Some(expected.as_str()) {
                problems.push(format!(
                    "counter {id} through member {member} is {seen:?}, and {expected} increments were applied"
                ));
            }
        }
    }
    // a delete built on a value the counter never held is refused, and the counter stays
    let stale = MovieDelete::new(ids[0]).if_matches(MovieFilter {
        title: Some(vec!["stale".to_string()]),
    });
    let stale = outcome(&clients[0], stale).await;
    if !matches!(stale, Ok(Some(ConditionRefusal::RowMismatch))) {
        problems.push(format!("a stale delete of counter {} was answered {stale:?}", ids[0]));
    }
    // the sorted race: every worker inserts one row expecting none, then deletes it expecting one
    let keyword = format!("contend-{}", args.run);
    let mut inserted = HashMap::<ConditionRefusal, u64>::new();
    let mut deleted = HashMap::<ConditionRefusal, u64>::new();
    let started = Instant::now();
    for round in 0..args.sorted_rounds {
        // one row, named by its round
        let row = MovieByKeyword {
            keyword: keyword.clone(),
            order: MovieByKeyword::order("contend", round),
            title: "contend".to_string(),
            id: round,
        };
        for phase in ["insert", "delete"] {
            // every worker at once
            let mut tasks = Vec::with_capacity(args.workers);
            for index in 0..args.workers.max(1) {
                let client = clients[index % clients.len()].clone();
                let row = row.clone();
                tasks.push(tokio::spawn(async move {
                    if phase == "insert" {
                        outcome(&client, row.if_absent()).await
                    } else {
                        let delete = MovieByKeywordDelete::new(row.keyword, row.order);
                        outcome(&client, delete.if_matches(MovieByKeywordFilter)).await
                    }
                }));
            }
            // exactly one applied, and every other refused for the reason the order gives
            let lost = if phase == "insert" {
                ConditionRefusal::RowExists
            } else {
                ConditionRefusal::RowMissing
            };
            let mut won = 0;
            for task in tasks {
                match task.await.map_err(|error| eyre!("a worker panicked: {error}"))? {
                    Ok(None) => won += 1,
                    Ok(Some(reason)) => {
                        let tally = if phase == "insert" {
                            &mut inserted
                        } else {
                            &mut deleted
                        };
                        *tally.entry(reason).or_default() += 1;
                        if reason != lost {
                            problems.push(format!("the {phase} of row {round} was refused as {reason:?}"));
                        }
                    }
                    Err(error) => problems.push(format!("the {phase} of row {round}: {error:?}")),
                }
            }
            if won != 1 {
                problems.push(format!("{won} {phase}s of row {round} were applied"));
            }
        }
    }
    println!(
        "{} sorted rounds of {} racing writers in {:.1?}: inserts refused {inserted:?}, deletes refused {deleted:?}",
        args.sorted_rounds,
        args.workers.max(1),
        started.elapsed()
    );
    // and the partition every row was raced in is empty, through every member
    let quorum = SendOptions::new().read(ReadLevel::Quorum);
    for (member, client) in clients.iter().enumerate() {
        let left = match client
            .send_one_with(MovieByKeywordGet::new(vec![keyword.clone()]), &quorum)
            .await
        {
            Ok(response) => response.access::<MovieByKeyword>()?.map_or(0, |rows| rows.len()),
            Err(Errors::QueryDidNotSucceed { .. }) => 0,
            Err(error) => {
                return Err(eyre!("reading {keyword} through member {member}: {error:?}"))
            }
        };
        if left > 0 {
            problems.push(format!("{keyword} through member {member} still holds {left} rows"));
        }
    }
    // the counters are removed, so the table holds the dataset and nothing of this run
    for id in &ids {
        clients[0]
            .send_one(MovieDelete::new(*id))
            .await
            .map_err(|error| eyre!("removing counter {id}: {error:?}"))?;
    }
    // a run that saw anything the committed order forbids fails by name
    if !problems.is_empty() {
        for problem in &problems {
            println!("  {problem}");
        }
        bail!("{} answers were not what the committed order gives", problems.len());
    }
    if refused == 0 {
        bail!("no compare and swap was ever refused, so nothing was raced");
    }
    println!("every conditional write was answered as the committed order gives");
    Ok(())
}
