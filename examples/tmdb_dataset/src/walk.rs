//! `walk`: every tablet of every table walked on every member, and checked
//!
//! The lab's check of the tablet walk ([F87](../../../docs/src/features/tablet-walk.md)). Through
//! each member in turn it walks every tablet of each table it is asked for, page by page, with
//! the admin read that answers from the copy that member holds. Every page is checked as it
//! lands: its keys ascend past the cursor and stay inside the tablet, its cursor is its last key,
//! and its applied index never goes below an earlier page's. Every member's keys of a table are
//! then compared with every other member's and, given the csv, with the partition keys of what
//! `load` wrote, a keyword's rows with the movies that carry it. A bucket's stripe table is
//! walked too, each stripe's key checked against the stripe it names and the placement group
//! that holds it. It prints how long each page held a shard's loop and took to read, and exits
//! with an error on any violation.

use clap::{ArgGroup, Args};
use color_eyre::eyre::{bail, eyre};
use shoal::client::{Routing, Shoal};
use shoal::shared::objects::stripe_key;
use shoal::shared::placement::{tablet_of, tablet_range, TABLET_COUNT};
use shoal::shared::pool_placement::pg_of;
use shoal::shared::protocol::admin::{
    AdminKind, AdminOutcome, AdminRequest, WalkAnswer, WALK_DEFAULT_LIMIT,
};
use shoal::shared::protocol::error::ErrorCode;
use shoal::shared::traits::PartitionKeySupport;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::load::{connect_named, COPY_ID_STRIDE};
use crate::{Movie, MovieByKeyword, MovieRelease, TmdbClient};

/// How many times a page a copy cannot answer yet is asked again
const RETRIES: usize = 500;

/// How long to wait before asking again a copy that is starting, installing or behind
const RETRY_AFTER: Duration = Duration::from_millis(200);

/// How long to wait before asking again a shard reading as many pages as it reads at once
///
/// A shard sheds a page past the few it reads at once, and one of those is done in about a
/// millisecond, so a shed page is asked again soon rather than after a copy's wait.
const SHED_AFTER: Duration = Duration::from_millis(2);

/// Walk every tablet of the tables on every member and check what comes back
#[derive(Args, Debug, Clone)]
#[command(group(ArgGroup::new("target").required(true).multiple(true).args(["inventory", "addr"])))]
pub struct WalkArgs {
    /// The inventory of the deployed cluster to walk
    #[clap(long, short)]
    pub inventory: Option<PathBuf>,
    /// A single node's client address: a node started by hand, or with `--inventory` the one
    /// member of the deployment to walk, as its admin
    #[clap(long)]
    pub addr: Option<String>,
    /// The tables to walk, by name
    #[clap(long, value_delimiter = ',', default_values_t = ["Movie".to_string(), "MovieByKeyword".to_string(), "MovieRelease".to_string()])]
    pub tables: Vec<String>,
    /// The csv `load` read, so every member's keys are compared with what it wrote
    #[clap(long)]
    pub dataset: Option<PathBuf>,
    /// How many csv rows `load` read
    #[clap(long)]
    pub limit: Option<usize>,
    /// How many copies of the csv `load` wrote (`load --copies`)
    #[clap(long, default_value_t = 1)]
    pub copies: u64,
    /// The first tablet to walk
    #[clap(long, default_value_t = 0)]
    pub from: u16,
    /// One past the last tablet to walk
    #[clap(long, default_value_t = TABLET_COUNT as u16)]
    pub to: u16,
    /// The most partitions a page holds
    #[clap(long, default_value_t = WALK_DEFAULT_LIMIT)]
    pub page: u32,
    /// How many tablets each member walks at once
    #[clap(long, default_value_t = 4)]
    pub concurrency: usize,
    /// The placement groups a tablet of the stripe table's pool holds, to check each stripe's group
    #[clap(long, default_value_t = 1)]
    pub groups_per_tablet: u32,
}

/// One page's figures, as the checks and the summary read them
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageCheck {
    /// The tablet walked
    pub tablet: u16,
    /// The key the walk is past, or none before its first page
    pub after: Option<u64>,
    /// The highest applied index a page of the walk was read at
    pub floor: Option<u64>,
}

impl PageCheck {
    /// The check of a tablet's first page
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn new(tablet: u16) -> Self {
        PageCheck {
            tablet,
            after: None,
            floor: None,
        }
    }

    /// Hold a page to the walk's rules, and move past it
    ///
    /// Returns every rule it broke, by name.
    ///
    /// # Arguments
    ///
    /// * `page` - The page
    pub fn observe(&mut self, page: &WalkAnswer) -> Vec<String> {
        let mut broken = Vec::new();
        let (low, high) = tablet_range(self.tablet);
        // every key inside the tablet, ascending, past the cursor
        let mut last = self.after;
        for key in &page.keys {
            if !(low..=high).contains(key) {
                broken.push(format!("{key:016x} is not a key of tablet {}", self.tablet));
            }
            if last.is_some_and(|last| *key <= last) {
                broken.push(format!("{key:016x} came after {:016x}", last.unwrap_or_default()));
            }
            last = Some(*key);
        }
        // the cursor is the last key handed out
        if page.next.is_some() && page.next != page.keys.last().copied() {
            broken.push(format!("the cursor {:?} is not the page's last key", page.next));
        }
        // and the applied index never goes back
        if let (Some(floor), Some(boundary)) = (self.floor, page.boundary) {
            if boundary < floor {
                broken.push(format!("a page was read at {boundary}, below the walk's {floor}"));
            }
        }
        self.floor = self.floor.max(page.boundary);
        self.after = page.next;
        broken
    }
}

/// What one member's walk of one table came to
#[derive(Debug, Default)]
struct TableWalk {
    /// Every key, with its live rows
    keys: BTreeMap<u64, u32>,
    /// Every tablet whose first page said its copy led
    led: HashSet<u16>,
    /// How many pages
    pages: usize,
    /// How many partitions came from memory
    resident: u64,
    /// How many from an archive
    archived: u64,
    /// How many archived records had moved under a page
    reprobed: u64,
    /// How many pages were asked again
    retries: usize,
    /// How long each page held its shard's loop, in microseconds
    held: Vec<u64>,
    /// How long each page's reads took, in microseconds
    read: Vec<u64>,
    /// Every stripe row handed out, by key: its consumer, object and index
    stripes: BTreeMap<u64, (u64, u128, u64)>,
    /// The rules pages broke
    broken: Vec<String>,
    /// How long it took
    took: Duration,
}

/// Ask a member for one page of a walk, asking again while its copy cannot answer yet
///
/// # Arguments
///
/// * `client` - The member
/// * `kind` - The walk
/// * `retries` - Where the count of pages asked again is kept
async fn ask_page(
    client: &Shoal<TmdbClient>,
    kind: AdminKind,
    retries: &mut usize,
) -> color_eyre::Result<WalkAnswer> {
    for _ in 0..RETRIES {
        let request = AdminRequest {
            op: shoal::uuid::Uuid::new_v4(),
            expected_version: 0,
            kind: kind.clone(),
        };
        // a member restarting drops its connections, and is asked again once it is back
        let response = match client.admin(&request).await {
            Ok(response) => response,
            Err(error) => {
                *retries += 1;
                eprintln!("a walk was not answered, asking again: {error}");
                tokio::time::sleep(RETRY_AFTER).await;
                continue;
            }
        };
        match response.outcome {
            Ok(AdminOutcome::Read(json)) => return Ok(serde_json::from_value(json)?),
            Ok(other) => bail!("a walk answered {other:?}"),
            // a shard reading its fill of pages is asked again soon
            Err(error) if error.code() == ErrorCode::Shedding => {
                *retries += 1;
                tokio::time::sleep(SHED_AFTER).await;
            }
            // a copy starting, installing or behind is asked again after a while
            Err(error) if matches!(error.code(), ErrorCode::Unavailable | ErrorCode::Timeout) => {
                *retries += 1;
                tokio::time::sleep(RETRY_AFTER).await;
            }
            Err(error) => bail!("a walk was refused: {} ({:?})", error.msg, error.code()),
        }
    }
    bail!("a page was refused {RETRIES} times")
}

/// Walk one tablet of a table through a member, every page checked
///
/// # Arguments
///
/// * `client` - The member
/// * `table` - The table
/// * `tablet` - The tablet
/// * `page` - The most partitions a page holds
async fn walk_tablet(
    client: Arc<Shoal<TmdbClient>>,
    table: String,
    tablet: u16,
    page: u32,
) -> color_eyre::Result<TableWalk> {
    let mut walk = TableWalk::default();
    let mut check = PageCheck::new(tablet);
    let mut first = true;
    loop {
        let kind = AdminKind::Walk {
            table: table.clone(),
            tablet,
            low: None,
            high: None,
            after: check.after,
            limit: Some(page),
            bytes: None,
            at_least: check.floor,
            // the lab's check of every copy is an operator's walk, a driver's class
            class: None,
        };
        let answer = ask_page(&client, kind, &mut walk.retries).await?;
        walk.pages += 1;
        walk.broken.extend(check.observe(&answer));
        // its figures, and whether this copy led when the walk began
        if first && answer.led == Some(true) {
            walk.led.insert(tablet);
        }
        first = false;
        walk.resident += u64::from(answer.resident);
        walk.archived += u64::from(answer.archived);
        walk.reprobed += u64::from(answer.reprobed);
        walk.held.push(answer.held_us);
        walk.read.push(answer.read_us);
        for (key, rows) in answer.keys.iter().zip(&answer.rows) {
            walk.keys.insert(*key, *rows);
        }
        for (key, stripe) in answer.keys.iter().zip(answer.stripes.iter().flatten()) {
            let object = u128::from_str_radix(&stripe.object, 16)
                .map_err(|error| eyre!("a stripe's object {:?} is not hex: {error}", stripe.object))?;
            walk.stripes.insert(*key, (stripe.consumer, object, stripe.stripe));
        }
        if answer.next.is_none() {
            return Ok(walk);
        }
    }
}

/// Walk tablets of a table through one member, several at once
///
/// # Arguments
///
/// * `client` - The member
/// * `table` - The table
/// * `args` - The walk's settings
async fn walk_table(
    client: Arc<Shoal<TmdbClient>>,
    table: &str,
    args: &WalkArgs,
) -> color_eyre::Result<TableWalk> {
    let started = Instant::now();
    let mut whole = TableWalk::default();
    let tablets: Vec<u16> = (args.from..args.to.min(TABLET_COUNT as u16)).collect();
    // a few tablets at a time, so a member's shards are kept busy without shedding
    for chunk in tablets.chunks(args.concurrency.max(1)) {
        let mut tasks = Vec::with_capacity(chunk.len());
        for tablet in chunk {
            tasks.push(tokio::spawn(walk_tablet(
                client.clone(),
                table.to_string(),
                *tablet,
                args.page,
            )));
        }
        for task in tasks {
            let walk = task.await??;
            whole.keys.extend(walk.keys);
            whole.led.extend(walk.led);
            whole.pages += walk.pages;
            whole.resident += walk.resident;
            whole.archived += walk.archived;
            whole.reprobed += walk.reprobed;
            whole.retries += walk.retries;
            whole.held.extend(walk.held);
            whole.read.extend(walk.read);
            whole.stripes.extend(walk.stripes);
            whole.broken.extend(walk.broken);
        }
    }
    whole.took = started.elapsed();
    Ok(whole)
}

/// The p50, the p99 and the most of some figures
///
/// # Arguments
///
/// * `figures` - The figures
fn spread(figures: &[u64]) -> (u64, u64, u64) {
    let mut sorted = figures.to_vec();
    sorted.sort_unstable();
    let at = |q: f64| {
        sorted
            .get(((sorted.len() as f64 * q) as usize).min(sorted.len().saturating_sub(1)))
            .copied()
            .unwrap_or(0)
    };
    (at(0.5), at(0.99), sorted.last().copied().unwrap_or(0))
}

/// Every partition key and its live rows each table should hold after `load`, from the csv
///
/// # Arguments
///
/// * `args` - Where the csv is and how much of it was loaded
fn expected_keys(args: &WalkArgs) -> color_eyre::Result<HashMap<String, BTreeMap<u64, u32>>> {
    let Some(dataset) = &args.dataset else {
        return Ok(HashMap::new());
    };
    let mut reader = csv::Reader::from_path(dataset)?;
    let mut movies: BTreeMap<u64, u32> = BTreeMap::new();
    let mut releases: BTreeMap<u64, u32> = BTreeMap::new();
    let mut keywords: HashMap<u64, HashSet<String>> = HashMap::new();
    let mut read = 0;
    for row in reader.deserialize::<Movie>() {
        let Ok(movie) = row else { continue };
        read += 1;
        // every copy `load` wrote, each under ids of its own
        for copy in 0..args.copies.max(1) {
            let mut copied = movie.clone();
            copied.id += copy * COPY_ID_STRIDE;
            movies.insert(copied.get_partition_key(), 1);
            releases.insert(MovieRelease::from_movie(&copied).get_partition_key(), 1);
            for keyword in MovieByKeyword::rows(&copied) {
                keywords
                    .entry(keyword.get_partition_key())
                    .or_default()
                    .insert(keyword.order);
            }
        }
        if Some(read) == args.limit {
            break;
        }
    }
    // a keyword's partition holds one row for each movie listed under it
    let keywords: BTreeMap<u64, u32> = keywords
        .into_iter()
        .map(|(key, orders)| (key, orders.len() as u32))
        .collect();
    println!(
        "expecting {} movies, {} release rows and {} keyword partitions from {read} csv rows",
        movies.len(),
        releases.len(),
        keywords.len()
    );
    Ok(HashMap::from([
        ("Movie".to_string(), movies),
        ("MovieRelease".to_string(), releases),
        ("MovieByKeyword".to_string(), keywords),
    ]))
}

/// Walk every tablet of the tables on every member, check every page, and compare the members
///
/// # Arguments
///
/// * `args` - What to walk
pub async fn walk(args: WalkArgs) -> color_eyre::Result<()> {
    let expected = expected_keys(&args)?;
    // every member as its admin, or the one node asked for
    let members: Vec<(String, Arc<Shoal<TmdbClient>>)> = match (&args.inventory, &args.addr) {
        (Some(inventory), None) => connect_named(inventory, Routing::Endpoints).await?,
        _ => crate::load::connect_targets(args.inventory.as_ref(), args.addr.as_deref(), Routing::Endpoints)
            .await?
            .into_iter()
            .enumerate()
            .map(|(index, client)| (format!("member {index}"), client))
            .collect(),
    };
    let mut violations = Vec::new();
    for table in &args.tables {
        let mut walks: Vec<(String, TableWalk)> = Vec::with_capacity(members.len());
        for (name, client) in &members {
            let walk = walk_table(client.clone(), table, &args).await?;
            let rows: u64 = walk.keys.values().map(|rows| u64::from(*rows)).sum();
            let (held_p50, held_p99, held_max) = spread(&walk.held);
            let (read_p50, read_p99, read_max) = spread(&walk.read);
            let seconds = walk.took.as_secs_f64().max(f64::EPSILON);
            println!(
                "{name} {table}: {} partitions, {rows} rows in {} pages, {} resident, {} archived, {} moved, {} retried, {} tablets led; held p50 {held_p50} p99 {held_p99} max {held_max} us, read p50 {read_p50} p99 {read_p99} max {read_max} us; {:.1?}, {:.0} partitions/s, {:.0} tablets/s",
                walk.keys.len(),
                walk.pages,
                walk.resident,
                walk.archived,
                walk.reprobed,
                walk.retries,
                walk.led.len(),
                walk.took,
                walk.keys.len() as f64 / seconds,
                f64::from(args.to.saturating_sub(args.from)) / seconds,
            );
            // every page's rules
            for broken in &walk.broken {
                violations.push(format!("{name} {table}: {broken}"));
            }
            // the keys the csv put there, with a keyword's rows
            if let Some(expected) = expected.get(table) {
                let walked: BTreeMap<u64, u32> = walk
                    .keys
                    .iter()
                    .filter(|(key, _)| (args.from..args.to).contains(&(tablet_of(**key) as u16)))
                    .map(|(key, rows)| (*key, *rows))
                    .collect();
                let wanted: BTreeMap<u64, u32> = expected
                    .iter()
                    .filter(|(key, _)| (args.from..args.to).contains(&(tablet_of(**key) as u16)))
                    .map(|(key, rows)| (*key, *rows))
                    .collect();
                let missing = wanted.keys().filter(|key| !walked.contains_key(key)).count();
                let extra = walked.keys().filter(|key| !wanted.contains_key(key)).count();
                let rows_differ = walked
                    .iter()
                    .filter(|(key, rows)| wanted.get(key).is_some_and(|want| want != *rows))
                    .count();
                if missing + extra + rows_differ > 0 {
                    violations.push(format!(
                        "{name} {table}: {missing} partitions missing, {extra} not in the csv, {rows_differ} with other rows than the csv's"
                    ));
                }
            }
            // a stripe's key is the stripe it names, in the placement group that holds it
            for (key, (consumer, object, stripe)) in &walk.stripes {
                if stripe_key(*consumer, *object, *stripe) != *key {
                    violations.push(format!("{name} {table}: {key:016x} is not the key of its stripe"));
                }
                let (low, high) = pg_of(*key, args.groups_per_tablet).range(args.groups_per_tablet);
                if !(low..=high).contains(key) {
                    violations.push(format!("{name} {table}: {key:016x} is outside its placement group"));
                }
            }
            walks.push((name.clone(), walk));
        }
        // every member holds the same partitions, and every tablet one leader
        if let Some(((first_name, first), rest)) = walks.split_first() {
            for (name, walk) in rest {
                if walk.keys != first.keys {
                    violations.push(format!("{name} {table} differs from {first_name}"));
                }
            }
            let mut leaders: HashMap<u16, usize> = HashMap::new();
            for (_, walk) in &walks {
                for tablet in &walk.led {
                    *leaders.entry(*tablet).or_default() += 1;
                }
            }
            let doubled = leaders.values().filter(|count| **count > 1).count();
            let led = leaders.len();
            println!(
                "{table}: {led} tablets led by one member, {doubled} by more than one while walked"
            );
        }
    }
    // every violation, then the verdict
    for violation in &violations {
        eprintln!("violation: {violation}");
    }
    if violations.is_empty() {
        println!("every page held to the walk's rules and every member matched");
        Ok(())
    } else {
        bail!("{} violations", violations.len())
    }
}

#[cfg(test)]
mod tests {
    use super::{PageCheck, WalkAnswer};
    use shoal::shared::placement::tablet_range;

    /// A page of keys, with its cursor and boundary
    ///
    /// # Arguments
    ///
    /// * `keys` - The keys
    /// * `next` - The cursor
    /// * `boundary` - The applied index
    fn page(keys: Vec<u64>, next: Option<u64>, boundary: Option<u64>) -> WalkAnswer {
        WalkAnswer {
            table: "Movie".to_string(),
            tablet: 3,
            low: tablet_range(3).0,
            high: tablet_range(3).1,
            after: None,
            rows: keys.iter().map(|_| 1).collect(),
            keys,
            bytes: 0,
            boundary,
            led: Some(false),
            next,
            group: None,
            resident: 0,
            archived: 0,
            reprobed: 0,
            held_us: 0,
            read_us: 0,
            queued_us: 0,
            paced_us: 0,
            stripes: None,
        }
    }

    /// A walk that holds to every rule passes, and each broken rule is named
    #[test]
    fn a_page_check_names_every_broken_rule() {
        let (low, high) = tablet_range(3);
        // two pages that hold to every rule
        let mut check = PageCheck::new(3);
        assert!(check.observe(&page(vec![low, low + 5], Some(low + 5), Some(10))).is_empty());
        assert!(check.observe(&page(vec![low + 9, high], None, Some(12))).is_empty());
        // keys out of order, a key of another tablet, a cursor that is not the last key, and a
        // boundary below the walk's floor
        let mut check = PageCheck::new(3);
        assert!(check.observe(&page(vec![low + 5], Some(low + 5), Some(10))).is_empty());
        assert_eq!(check.observe(&page(vec![low + 4], None, Some(10))).len(), 1);
        let mut check = PageCheck::new(3);
        assert_eq!(check.observe(&page(vec![high + 1], None, None)).len(), 1);
        let mut check = PageCheck::new(3);
        assert_eq!(check.observe(&page(vec![low, low + 1], Some(low), None)).len(), 1);
        let mut check = PageCheck::new(3);
        assert!(check.observe(&page(vec![low], Some(low), Some(10))).is_empty());
        assert_eq!(check.observe(&page(vec![low + 1], None, Some(9))).len(), 1);
        // and a duplicate across pages is a key that does not come after the cursor
        let mut check = PageCheck::new(3);
        assert!(check.observe(&page(vec![low + 2], Some(low + 2), None)).is_empty());
        assert_eq!(check.observe(&page(vec![low + 2], None, None)).len(), 1);
    }
}
