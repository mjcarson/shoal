//! `reclaim`: posters filled, deleted and replaced, and the pool devices' footprint read back
//!
//! The lab's evidence that space comes back ([F92](../../../docs/src/features/reclamation-and-device-lifecycle.md)).
//! `fill` puts posters through every member in turn, `delete` deletes them, `wait` reads every
//! device's chunk bytes until they are within a margin of where they started, or since item 301
//! until they hold steady for a while, which is the lab's settle between a bench's arms, and `churn` puts,
//! replaces and deletes for a while, printing the footprint every ten seconds so a steady one
//! shows. `read` reads every poster back through the members in turn and counts what each was
//! answered, which is how Q31's lab run told the posters a restore brought back from the ones it
//! stranded ([Q31](../../../docs/src/object-storage/contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17)).
//! Every poster is kept under `reclaim/`, so a second run finds the first's, and `--first` sets one
//! run's posters beside another's.

use clap::{ArgGroup, Args, ValueEnum};
use color_eyre::eyre::bail;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use shoal::shared::protocol::admin::{AdminKind, AdminOutcome, AdminRequest};
use shoal::Shoal;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::load::connect_targets;
use crate::{Posters, TmdbClient};

/// What `reclaim` does
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclaimAction {
    /// Put every poster through the members in turn
    Fill,
    /// Delete every poster
    Delete,
    /// Read the devices until their chunk bytes are within the margin, or hold steady
    Wait,
    /// Put, replace and delete posters for a while, printing the footprint as it goes
    Churn,
    /// Read every poster back and count what each was answered
    Read,
}

/// Fill, delete, wait for or churn the posters reclamation is measured by
#[derive(Args, Debug, Clone)]
#[command(group(ArgGroup::new("target").required(true).args(["inventory", "addr"])))]
pub struct ReclaimArgs {
    /// What to do
    #[clap(value_enum)]
    pub action: ReclaimAction,
    /// The inventory of the deployed cluster
    #[clap(long, short)]
    pub inventory: Option<PathBuf>,
    /// A single node's client address, for a node started by hand
    #[clap(long)]
    pub addr: Option<String>,
    /// How many posters
    #[clap(long, default_value_t = 2000)]
    pub objects: u64,
    /// The index of the first poster, so one run's posters can sit beside another's
    #[clap(long, default_value_t = 0)]
    pub first: u64,
    /// Each poster's size, in bytes
    #[clap(long, default_value_t = 1 << 20)]
    pub size: u64,
    /// How many puts or deletes are in flight at once
    #[clap(long, default_value_t = 4)]
    pub concurrency: usize,
    /// How long a churn runs, in seconds
    #[clap(long, default_value_t = 1800)]
    pub duration: u64,
    /// The chunk bytes `wait` stops at, over every device
    #[clap(long, default_value_t = 0)]
    pub margin: u64,
    /// How long `wait` waits at most, in seconds
    #[clap(long, default_value_t = 1800)]
    pub timeout: u64,
    /// With `wait`, stop too once the devices' chunk bytes have held the same for this many
    /// seconds: a store whose reclamation is done, whatever it keeps
    /// ([item 301](../../../docs/src/appendix/resolved/a-settle-waits-for-reclamation.md))
    #[clap(long)]
    pub steady: Option<u64>,
}

/// Seeded bytes for a poster, different for every poster and every replace of it
///
/// # Arguments
///
/// * `len` - How many
/// * `seed` - What sets them apart
fn poster_bytes(len: u64, seed: u64) -> Vec<u8> {
    (0..len)
        .map(|index| (shoal::shared::rendezvous::mix64(index ^ seed.rotate_left(40)) >> 24) as u8)
        .collect()
}

/// A poster's path
///
/// # Arguments
///
/// * `index` - Which poster
fn path_of(index: u64) -> String {
    format!("reclaim/{index:06}.bin")
}

/// One device's figures, as its own node reported them
#[derive(Debug, Clone, Default)]
struct DeviceFigures {
    /// Where it is mounted
    path: String,
    /// The chunks its slices hold
    chunks: u64,
    /// The bytes those chunks' files take
    chunk_bytes: u64,
    /// The bytes staged on it and not yet applied or discarded
    staged_bytes: u64,
}

/// Every device's figures, each from the member that reports it, by node and id
///
/// # Arguments
///
/// * `clients` - A client of every member
async fn footprint(clients: &[Arc<Shoal<TmdbClient>>]) -> BTreeMap<(String, String), DeviceFigures> {
    let mut found = BTreeMap::new();
    for client in clients {
        let request = AdminRequest {
            op: shoal::uuid::Uuid::new_v4(),
            expected_version: 0,
            kind: AdminKind::Devices,
        };
        // a member that does not answer is read again at the next sample
        let Ok(response) = client.admin(&request).await else {
            continue;
        };
        let Ok(AdminOutcome::Read(view)) = response.outcome else {
            continue;
        };
        for member in view["members"].as_array().into_iter().flatten() {
            let node = member["node"].as_str().unwrap_or_default().to_string();
            for device in member["devices"].as_array().into_iter().flatten() {
                let slices = device["slices"].as_array().cloned().unwrap_or_default();
                // a member that knows only another's id reports no slices of it
                if slices.is_empty() {
                    continue;
                }
                let sum = |field: &str| slices.iter().map(|slice| slice[field].as_u64().unwrap_or(0)).sum::<u64>();
                let figures = DeviceFigures {
                    path: device["path"].as_str().unwrap_or_default().to_string(),
                    chunks: sum("chunks"),
                    chunk_bytes: sum("chunk_bytes"),
                    staged_bytes: sum("staged_bytes"),
                };
                let id = device["id"].as_str().unwrap_or_default().to_string();
                found.insert((node.clone(), id), figures);
            }
        }
    }
    found
}

/// Print the footprint on one line, its total first, then each device's
///
/// # Arguments
///
/// * `started` - When the run started
/// * `devices` - Every device's figures
fn print_footprint(started: Instant, devices: &BTreeMap<(String, String), DeviceFigures>) {
    let total: u64 = devices.values().map(|figures| figures.chunk_bytes).sum();
    let chunks: u64 = devices.values().map(|figures| figures.chunks).sum();
    let staged: u64 = devices.values().map(|figures| figures.staged_bytes).sum();
    let each: Vec<String> = devices
        .iter()
        .map(|((node, _), figures)| format!("{}:{}={}", &node[..node.len().min(8)], figures.path, figures.chunk_bytes))
        .collect();
    println!(
        "{:>7.1}s chunk_bytes {total} chunks {chunks} staged {staged} {}",
        started.elapsed().as_secs_f64(),
        each.join(" ")
    );
}

/// Fill, delete, wait for or churn the posters
///
/// # Arguments
///
/// * `args` - What to do, through which cluster, and how much of it
///
/// # Errors
///
/// When no member can be reached, a put or delete is refused, or `wait` runs out.
pub async fn reclaim(args: ReclaimArgs) -> color_eyre::Result<()> {
    // every member as the deployment's user, each sending only through its own
    let clients = connect_targets(
        args.inventory.as_ref(),
        args.addr.as_deref(),
        shoal::client::Routing::Endpoints,
    )
    .await?;
    let started = Instant::now();
    print_footprint(started, &footprint(&clients).await);
    match args.action {
        ReclaimAction::Fill => {
            let size = args.size;
            fan_out(&clients, &args, move |client, index| async move {
                let bytes = poster_bytes(size, index);
                client.bucket::<Posters>().put(&path_of(index), &bytes).await.map(|_| ())
            })
            .await?;
        }
        ReclaimAction::Delete => {
            fan_out(&clients, &args, |client, index| async move {
                client.bucket::<Posters>().delete(&path_of(index)).await.map(|_| ())
            })
            .await?;
        }
        ReclaimAction::Wait => {
            // until the devices hold no more than the margin, hold the same for the steady window,
            // or the wait runs out
            let deadline = started + Duration::from_secs(args.timeout);
            let mut readings: Vec<(Instant, u64)> = Vec::new();
            loop {
                let devices = footprint(&clients).await;
                print_footprint(started, &devices);
                let total: u64 = devices.values().map(|figures| figures.chunk_bytes).sum();
                if total <= args.margin {
                    println!("within the margin of {} bytes after {:.1}s", args.margin, started.elapsed().as_secs_f64());
                    return Ok(());
                }
                // the same total for the whole window is a reclamation that is done (item 301)
                readings.push((Instant::now(), total));
                if let Some(steady) = args.steady {
                    if held_steady(&readings, Duration::from_secs(steady)) {
                        println!("steady at {total} chunk bytes for {steady}s after {:.1}s", started.elapsed().as_secs_f64());
                        return Ok(());
                    }
                }
                if Instant::now() > deadline {
                    bail!("the devices held {total} chunk bytes after {}s, past the margin of {}", args.timeout, args.margin);
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
        ReclaimAction::Churn => churn(&clients, &args, started).await?,
        ReclaimAction::Read => read_back(&clients, &args).await?,
    }
    print_footprint(started, &footprint(&clients).await);
    Ok(())
}

/// Whether the devices' chunk bytes have held the same for a window: the last reading's total,
/// unchanged in every reading back to one at least the window before it (item 301)
///
/// # Arguments
///
/// * `readings` - Each reading's time and the devices' chunk bytes, in order
/// * `window` - How long they must hold the same
fn held_steady(readings: &[(Instant, u64)], window: Duration) -> bool {
    let Some(&(last_at, last)) = readings.last() else {
        return false;
    };
    // back from the last reading for as long as the total is the same
    let mut since = last_at;
    for &(at, total) in readings.iter().rev() {
        if total != last {
            break;
        }
        since = at;
    }
    last_at.duration_since(since) >= window
}

/// Run one operation for every poster, a member at a time in turn, so many in flight
///
/// # Arguments
///
/// * `clients` - A client of every member
/// * `args` - How many posters and how many in flight
/// * `op` - The operation, for a client and a poster
async fn fan_out<F, Fut>(clients: &[Arc<Shoal<TmdbClient>>], args: &ReclaimArgs, op: F) -> color_eyre::Result<()>
where
    F: Fn(Arc<Shoal<TmdbClient>>, u64) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<(), shoal::Errors>> + Send,
{
    // the posters dealt out to the workers, each worker through the members in turn
    let next = Arc::new(AtomicU64::new(args.first));
    let end = args.first + args.objects;
    let mut workers = Vec::new();
    for worker in 0..args.concurrency.max(1) {
        let (clients, next, op) = (clients.to_vec(), next.clone(), op.clone());
        workers.push(tokio::spawn(async move {
            let mut failed = 0u64;
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= end {
                    return failed;
                }
                let client = clients[(index as usize + worker) % clients.len()].clone();
                if let Err(error) = op(client, index).await {
                    failed += 1;
                    eprintln!("{}: {error:?}", path_of(index));
                }
            }
        }));
    }
    let mut failed = 0;
    for worker in workers {
        failed += worker.await?;
    }
    if failed > 0 {
        bail!("{failed} of {} operations failed", args.objects);
    }
    Ok(())
}

/// Put, replace and delete posters for a while, printing the footprint every ten seconds
///
/// Half of every operation replaces a poster that is there, a quarter deletes one, and a quarter
/// puts one that is not, so the number of posters stays near where it started.
///
/// # Arguments
///
/// * `clients` - A client of every member
/// * `args` - How many posters, how many in flight and for how long
/// * `started` - When the run started
async fn churn(clients: &[Arc<Shoal<TmdbClient>>], args: &ReclaimArgs, started: Instant) -> color_eyre::Result<()> {
    let until = started + Duration::from_secs(args.duration);
    let counts = Arc::new([AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)]);
    let mut workers = Vec::new();
    for worker in 0..args.concurrency.max(1) {
        let (clients, counts, first, objects, size) =
            (clients.to_vec(), counts.clone(), args.first, args.objects.max(1), args.size);
        workers.push(tokio::spawn(async move {
            let mut rng = SmallRng::seed_from_u64(0xc4u64 << 32 | worker as u64);
            let mut round = 0u64;
            while Instant::now() < until {
                round += 1;
                let index = first + rng.gen_range(0..objects);
                let client = &clients[(round as usize + worker) % clients.len()];
                let posters = client.bucket::<Posters>();
                // a replace, a delete, or a put
                let roll = rng.gen_range(0..4);
                let outcome = if roll < 3 {
                    let bytes = poster_bytes(size, (worker as u64) << 40 | round);
                    posters.put(&path_of(index), &bytes).await.map(|_| ())
                } else {
                    posters.delete(&path_of(index)).await.map(|_| ())
                };
                let slot = match (roll < 3, outcome.is_ok()) {
                    (true, true) => 0,
                    (false, true) => 1,
                    (_, false) => 3,
                };
                counts[slot].fetch_add(1, Ordering::Relaxed);
                if let Err(error) = outcome {
                    eprintln!("{}: {error:?}", path_of(index));
                }
            }
        }));
    }
    // the footprint every ten seconds while they run
    while Instant::now() < until {
        tokio::time::sleep(Duration::from_secs(10).min(until.saturating_duration_since(Instant::now()))).await;
        print!(
            "puts {} deletes {} failed {} | ",
            counts[0].load(Ordering::Relaxed),
            counts[1].load(Ordering::Relaxed),
            counts[3].load(Ordering::Relaxed)
        );
        print_footprint(started, &footprint(clients).await);
    }
    for worker in workers {
        worker.await?;
    }
    let failed = counts[3].load(Ordering::Relaxed);
    if failed > 0 {
        bail!("{failed} operations of the churn failed");
    }
    Ok(())
}

/// What one read of a poster was answered, named so reads answered alike are counted together
///
/// # Arguments
///
/// * `answer` - The read's bytes, or its error
/// * `expected` - The bytes `fill` put
fn answered_as(answer: Result<Vec<u8>, shoal::Errors>, expected: &[u8]) -> (String, String) {
    match answer {
        // the bytes that were put, or other ones
        Ok(bytes) if bytes == expected => ("whole".to_string(), String::new()),
        Ok(bytes) => ("wrong bytes".to_string(), format!("{} bytes read of {}", bytes.len(), expected.len())),
        // a refusal by the bucket, or a failure of the machinery under it, by its code
        Err(shoal::Errors::Object { outcome, msg, .. }) => (format!("object {outcome:?}"), msg),
        Err(shoal::Errors::Server { code, msg, .. }) => (format!("server {code:?}"), msg),
        Err(error) => ("other".to_string(), format!("{error:?}")),
    }
}

/// Read every poster back through the members in turn, and print what each was answered
///
/// A poster is whole when its bytes are the ones `fill` put it with. Every other answer is counted
/// by its kind, with the first message of each, so a read that strands posters says how.
///
/// # Arguments
///
/// * `clients` - A client of every member
/// * `args` - Which posters, how large, and how many reads in flight
///
/// # Errors
///
/// When a worker cannot be joined, or a poster did not read back whole.
async fn read_back(clients: &[Arc<Shoal<TmdbClient>>], args: &ReclaimArgs) -> color_eyre::Result<()> {
    // each kind of answer, how many posters were given it, and its first message
    let tally: Arc<Mutex<BTreeMap<String, (u64, String)>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let next = Arc::new(AtomicU64::new(args.first));
    let end = args.first + args.objects;
    let mut workers = Vec::new();
    for worker in 0..args.concurrency.max(1) {
        let (clients, next, tally, size) = (clients.to_vec(), next.clone(), tally.clone(), args.size);
        workers.push(tokio::spawn(async move {
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= end {
                    return;
                }
                // through the members in turn, as `fill` put them
                let client = &clients[(index as usize + worker) % clients.len()];
                let answer = client.bucket::<Posters>().get(&path_of(index)).await.map(|(_, bytes)| bytes);
                let (kind, msg) = answered_as(answer, &poster_bytes(size, index));
                let mut tally = tally.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                let entry = tally.entry(kind).or_insert_with(|| (0, msg));
                entry.0 += 1;
            }
        }));
    }
    for worker in workers {
        worker.await?;
    }
    // every kind of answer on a line of its own, the whole ones first
    let tally = tally.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
    let whole = tally.get("whole").map_or(0, |(count, _)| *count);
    println!("read {} posters from {}: {whole} whole", args.objects, path_of(args.first));
    for (kind, (count, msg)) in tally.iter().filter(|(kind, _)| kind.as_str() != "whole") {
        println!("  {count} {kind}: {msg}");
    }
    if whole < args.objects {
        bail!("{} of {} posters did not read back whole", args.objects - whole, args.objects);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::held_steady;
    use std::time::{Duration, Instant};

    /// The chunk bytes hold steady only once they stayed the same for the whole window: a total
    /// still falling, or the same for less than the window, is not; the same from the first
    /// reading is, which is why the lab's settle sleeps out a reclamation's grace first (item 301)
    #[test]
    fn chunk_bytes_hold_steady_once_unchanged_for_the_window() {
        let start = Instant::now();
        let at = |secs: u64| start + Duration::from_secs(secs);
        let window = Duration::from_secs(6);
        assert!(!held_steady(&[], window));
        // falling the whole time
        assert!(!held_steady(&[(at(0), 9), (at(2), 7), (at(4), 5), (at(6), 3), (at(8), 1)], window));
        // the same for four seconds, then for six
        assert!(!held_steady(&[(at(0), 9), (at(2), 5), (at(4), 5), (at(6), 5)], window));
        assert!(held_steady(&[(at(0), 9), (at(2), 5), (at(4), 5), (at(6), 5), (at(8), 5)], window));
        // the same from the first reading
        assert!(held_steady(&[(at(0), 9), (at(6), 9)], window));
    }
}
