//! The three legs of the spike, each run on a cluster bootstrapped for it, and what they share
//!
//! - `rate`: rows a second a group, as inserts, overwrites and conditional updates of resident
//!   rows, at depth one and thirty-two; and one row rewritten in a loop at four sizes (Q17).
//! - `rows`: bytes a row on disk and in memory after a load of millions, read again after a
//!   restart; then the cold commit, on the rows that restart left cold.
//! - `size`: an object held inline, swept from 1 KiB to 1 MiB, for the inline threshold.
//! - `remedy`: a supplement run after the rounds. Cold commits under load, alone and after the
//!   read S7's coordinator makes, and a writer beside each: whether the read removes the stall.
//!
//! Every cell is read against the same things: the driver's own latencies and rate, every
//! host's device and network counters, and every member's WAL counters, before and after.

pub mod rate;
pub mod remedy;
pub mod rows;
pub mod size;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use color_eyre::eyre::bail;
use shoal::shared::protocol::read::ReadLevel;

use crate::cluster::{now_ms, Client, CounterDelta, Counters, Group, Lab};
use crate::drive::{self, Answer, Cell, Op, Outcome, Plan, Query, Work};
use crate::keys::{object_hash, stripe_hash, Cursor, StripeKey, Tablets};
use crate::record::{append, Record};
use crate::shape;
use crate::stats::Rng;
use crate::{ObjectMetaGet, StripeMetaGet};

/// The table a cell writes, and for an object how many bytes it holds inline
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// `StripeMeta`
    Stripe,
    /// `StripeMetaDigest`
    Digest,
    /// `ObjectMeta`, holding this many bytes inline
    Object(usize),
}

impl Kind {
    /// The table's name as the schema spells it
    #[must_use]
    pub fn table(self) -> &'static str {
        use shoal::shared::traits::PartitionKeySupport;
        match self {
            Kind::Stripe => <crate::StripeMeta as PartitionKeySupport>::name(),
            Kind::Digest => <crate::StripeMetaDigest as PartitionKeySupport>::name(),
            Kind::Object(_) => <crate::ObjectMeta as PartitionKeySupport>::name(),
        }
    }

    /// The short name a cell is labelled with
    #[must_use]
    pub fn short(self) -> &'static str {
        match self {
            Kind::Stripe => "stripe",
            Kind::Digest => "digest",
            Kind::Object(_) => "object",
        }
    }
}

/// A key of either table
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Key {
    /// A stripe row's key
    Stripe(StripeKey),
    /// An object row's key
    Object(u64),
}

impl Key {
    /// The partition hash the key routes by
    #[must_use]
    pub fn hash(&self) -> u64 {
        match self {
            Key::Stripe(key) => stripe_hash(key),
            Key::Object(key) => object_hash(*key),
        }
    }
}

/// The whole row a key names, at a sequence or version, as an insert
///
/// # Arguments
///
/// * `kind` - The table
/// * `key` - The key
/// * `seq` - The sequence or version it holds
#[must_use]
pub fn insert(kind: Kind, key: &Key, seq: u64) -> Query {
    match (kind, key) {
        (Kind::Stripe, Key::Stripe(key)) => shape::stripe_row(*key, seq).into(),
        (Kind::Digest, Key::Stripe(key)) => shape::digest_row(*key, seq).into(),
        (Kind::Object(inline), Key::Object(key)) => shape::object_row(*key, seq, inline, *key).into(),
        _ => unreachable!("a key is only ever paired with its own table"),
    }
}

/// A change to the row a key names, applied only if it holds the sequence or version read
///
/// # Arguments
///
/// * `kind` - The table
/// * `key` - The key
/// * `read` - The sequence or version the writer read
#[must_use]
pub fn commit(kind: Kind, key: &Key, read: u64) -> Query {
    match (kind, key) {
        (Kind::Stripe, Key::Stripe(key)) => shape::stripe_commit(*key, read).into(),
        (Kind::Object(inline), Key::Object(key)) => shape::object_change(*key, read, inline, *key).into(),
        _ => unreachable!("only stripe and object rows are committed to"),
    }
}

/// A get of the row a key names
///
/// # Arguments
///
/// * `key` - The key
#[must_use]
pub fn get(key: &Key) -> Query {
    match key {
        Key::Stripe(key) => StripeMetaGet::new(vec![*key]).into(),
        Key::Object(key) => ObjectMetaGet::new(vec![*key]).into(),
    }
}

/// Which member leads each tablet of a table, so a write is sent to its group's leader
///
/// Every write a cell makes goes to the member that leads the key's group, so a cell measures a
/// group's commit and not a hop between members. A client of today's Shoal does not route by
/// topology ([D7](../../../docs/src/direction/shard-aware-routing.md)); the hop is the
/// coordinator's and is left out on purpose, and the page says so.
#[derive(Debug, Clone)]
pub struct Route {
    /// The leading member of each tablet, by its place in the lab's members
    by_tablet: Vec<usize>,
}

impl Route {
    /// The route of a table's groups
    ///
    /// # Arguments
    ///
    /// * `groups` - Every group of the table
    #[must_use]
    pub fn of(groups: &[Group]) -> Self {
        // every tablet starts at the first member and is moved to its group's leader
        let mut by_tablet = vec![0; shoal::server::ring::TABLET_COUNT];
        for group in groups {
            for tablet in &group.tablets {
                by_tablet[usize::from(*tablet)] = group.leader;
            }
        }
        Route { by_tablet }
    }

    /// The member leading the group a key lands in
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    #[must_use]
    pub fn leader(&self, key: &Key) -> usize {
        self.leader_of_hash(key.hash())
    }

    /// The member leading the group a partition hash lands in
    ///
    /// # Arguments
    ///
    /// * `hash` - The partition hash
    #[must_use]
    pub fn leader_of_hash(&self, hash: u64) -> usize {
        self.by_tablet[usize::from(crate::keys::tablet_of(hash))]
    }
}

/// The groups a leg aims cells at: one a Zen1 host leads, one europa leads, and a control
#[derive(Debug, Clone)]
pub struct Targets {
    /// A group led by a member that is not the fast host, the lab's worst case
    pub slow: Group,
    /// A group led by the fast host
    pub fast: Group,
    /// Another group led by the same member as `slow`, on the same shard where one is
    pub control: Group,
}

impl Targets {
    /// Choose the groups of a table to aim at
    ///
    /// # Arguments
    ///
    /// * `lab` - The cluster
    /// * `groups` - Every group of the table
    /// * `fast` - The name of the fast host's member
    ///
    /// # Errors
    ///
    /// When no member but the fast one leads a group, or the fast one leads none.
    pub fn choose(lab: &Lab, groups: &[Group], fast: &str) -> color_eyre::Result<Targets> {
        // the fast member, by name
        let fast_member = lab.members.iter().position(|member| member.name == fast);
        let led_by_fast = |group: &&Group| Some(group.leader) == fast_member;
        let Some(slow) = groups.iter().find(|group| !led_by_fast(group)).cloned() else {
            bail!("every group of {} is led by {fast}", groups.first().map_or("?", |g| g.table.as_str()));
        };
        let Some(fast) = groups.iter().find(led_by_fast).cloned() else {
            bail!("{fast} leads no group");
        };
        // the control: the slow group's leader and shard if it leads another there, else any
        // other group it leads, else any other group a slow member leads
        let others = || groups.iter().filter(|group| group.id != slow.id);
        let control = others()
            .find(|group| group.leader == slow.leader && group.shard == slow.shard)
            .or_else(|| others().find(|group| group.leader == slow.leader))
            .or_else(|| others().find(|group| !led_by_fast(group)))
            .cloned()
            .ok_or_else(|| color_eyre::eyre::eyre!("no second group for a control"))?;
        Ok(Targets {
            slow,
            fast,
            control,
        })
    }
}

/// Everything a leg needs: the cluster, the round, where records go, and how big to be
pub struct Ctx {
    /// The cluster
    pub lab: Lab,
    /// The round, which decides the order cells run in
    pub round: u32,
    /// Whether this is a quick run, a fraction of every count and window, never a measurement
    pub quick: bool,
    /// The file records are added to
    pub out: PathBuf,
    /// The member on the fast host, whose groups are labelled apart
    pub fast: String,
}

impl Ctx {
    /// A count at full size, or a quick run's
    ///
    /// # Arguments
    ///
    /// * `full` - The full count
    /// * `quick` - The quick run's
    #[must_use]
    pub fn scale(&self, full: u64, quick: u64) -> u64 {
        if self.quick {
            quick
        } else {
            full
        }
    }

    /// A cell's warm-up and window
    ///
    /// # Arguments
    ///
    /// * `window` - The window at full size, in seconds
    #[must_use]
    pub fn plan(&self, window: u64) -> Plan {
        if self.quick {
            Plan {
                warmup: Duration::from_secs(1),
                window: Duration::from_secs(2),
            }
        } else {
            Plan {
                warmup: Duration::from_secs(5),
                window: Duration::from_secs(window),
            }
        }
    }

    /// Whether this round runs its cells in reverse, as every even round does
    #[must_use]
    pub fn reversed(&self) -> bool {
        self.round % 2 == 0
    }

    /// A record of this round
    ///
    /// # Arguments
    ///
    /// * `measurement` - The measurement
    /// * `cell` - The cell
    /// * `side` - The side
    #[must_use]
    pub fn record(&self, measurement: &str, cell: &str, side: &str) -> Record {
        Record::new(measurement, cell, side, self.round, self.quick)
    }

    /// Add records to the output, and print a line for each
    ///
    /// # Arguments
    ///
    /// * `records` - The records
    ///
    /// # Errors
    ///
    /// When the output cannot be written.
    pub fn emit(&self, records: &[Record]) -> color_eyre::Result<()> {
        for record in records {
            // one line a record, so a run can be followed
            let rate = record.metrics.get("per_sec").copied().unwrap_or(0.0);
            let p50 = record.metrics.get("p50_us").copied().unwrap_or(0.0);
            let p99 = record.metrics.get("p99_us").copied().unwrap_or(0.0);
            println!(
                "{} | {} | {} | {rate:.0}/s p50 {p50:.0} µs p99 {p99:.0} µs{}",
                record.measurement,
                record.cell,
                record.side,
                match record.metrics.get("failed") {
                    Some(failed) if *failed > 0.0 => format!(" FAILED {failed}"),
                    _ => String::new(),
                }
            );
        }
        append(&self.out, records)
    }

    /// Every member's client, in member order
    #[must_use]
    pub fn clients(&self) -> Arc<Vec<Client>> {
        Arc::new(self.lab.members.iter().map(|member| member.client.clone()).collect())
    }
}

/// What a cell is read against, taken before it and after it
pub struct Snap {
    /// Every host's device and network counters
    pub counters: Counters,
    /// Every member's WAL syncs, bytes and appends since its shards started, together
    pub wal: (u64, u64, u64),
    /// When it was taken, by this host's clock
    pub at_ms: u64,
}

/// Take a snapshot of the counters a cell is read against
///
/// # Arguments
///
/// * `lab` - The cluster
///
/// # Errors
///
/// When a host or a member cannot be read.
pub async fn snap(lab: &Lab) -> color_eyre::Result<Snap> {
    // the WAL's counters from every member's own shards
    let reports = lab.replication().await?;
    let mut wal = (0, 0, 0);
    for report in &reports {
        for shard in &report.shards {
            wal.0 += shard.wal_syncs;
            wal.1 += shard.wal_bytes;
            wal.2 += shard.wal_appends;
        }
    }
    // and every host's devices and network
    let counters = lab.counters().await?;
    Ok(Snap {
        counters,
        wal,
        at_ms: now_ms(),
    })
}

/// Fill a record with what a cell did and what it cost the hosts
///
/// # Arguments
///
/// * `record` - The record
/// * `cell` - What the driver saw
/// * `lab` - The cluster, for the counters' hosts
/// * `before` - The snapshot taken before the cell
/// * `after` - The snapshot taken after it
pub fn fill(record: &mut Record, cell: &Cell, lab: &Lab, before: &Snap, after: &Snap) {
    // the driver's own figures
    let latency = cell.latency.summary();
    let ops = (cell.applied + cell.refused + cell.failed).max(1) as f64;
    record
        .set("per_sec", cell.per_sec())
        .set("applied", cell.applied as f64)
        .set("refused", cell.refused as f64)
        .set("failed", cell.failed as f64)
        .set("secs", cell.secs)
        .set("p50_us", latency.p50)
        .set("p99_us", latency.p99)
        .set("p999_us", latency.p999)
        .set("max_us", latency.max)
        .set("mean_us", latency.mean)
        .set("exhausted", if cell.exhausted { 1.0 } else { 0.0 });
    if !cell.part.is_empty() {
        let part = cell.part.summary();
        let rest = cell.rest.summary();
        record
            .set("part_p50_us", part.p50)
            .set("part_p99_us", part.p99)
            .set("rest_p50_us", rest.p50)
            .set("rest_p99_us", rest.p99);
    }
    if let Some(error) = cell.errors.first() {
        record.label("error", error.clone());
    }
    // what the hosts did over the cell, warm-up included, a completed operation each
    let delta: CounterDelta = lab.delta(&before.counters, &after.counters);
    let wal_syncs = after.wal.0.saturating_sub(before.wal.0) as f64;
    let wal_bytes = after.wal.1.saturating_sub(before.wal.1) as f64;
    let wal_appends = after.wal.2.saturating_sub(before.wal.2) as f64;
    // the operations of the whole cell, warm-up included, which is what the counters cover
    let whole = (cell.per_sec() * (after.at_ms.saturating_sub(before.at_ms) as f64 / 1e3)).max(ops);
    for (host, read) in &delta.read_by_host {
        record.set(&format!("device_read_per_op:{host}"), *read as f64 / whole);
    }
    for (host, written) in &delta.written_by_host {
        record.set(&format!("device_written_per_op:{host}"), *written as f64 / whole);
    }
    record
        .set("device_written_per_op", delta.device_written as f64 / whole)
        .set("device_read_per_op", delta.device_read as f64 / whole)
        .set("flushes_per_op", delta.flushes as f64 / whole)
        .set("nic_peak_share", delta.nic_peak_share)
        .set("nic_tx_per_op", delta.nic_tx as f64 / whole)
        .set("wal_bytes_per_append", if wal_appends > 0.0 { wal_bytes / wal_appends } else { 0.0 })
        .set("wal_bytes_per_op_per_replica", wal_bytes / whole / lab.members.len().max(1) as f64)
        .set("wal_appends_per_sync", if wal_syncs > 0.0 { wal_appends / wal_syncs } else { 0.0 });
}

/// Run one cell between two snapshots and record it
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `record` - The record to fill
/// * `plan` - The warm-up and window
/// * `workers` - The cell's workers
///
/// # Errors
///
/// When the counters cannot be read.
pub async fn measured(
    ctx: &Ctx,
    mut record: Record,
    plan: Plan,
    workers: Vec<Box<dyn Work>>,
) -> color_eyre::Result<(Record, Cell)> {
    // counters on both sides of the cell
    let before = snap(&ctx.lab).await?;
    let cell = drive::run(plan, workers).await;
    let after = snap(&ctx.lab).await?;
    fill(&mut record, &cell, &ctx.lab, &before, &after);
    Ok((record, cell))
}

/// Where a worker's keys come from
pub enum Keys {
    /// Fresh keys from a cursor, each taken once
    Fresh(Cursor),
    /// A list of keys, each with the sequence it holds, taken in turn and again when `cycle`
    List {
        /// The keys and their sequences
        keys: Vec<(Key, u64)>,
        /// The next to take
        next: usize,
        /// Whether to start again at the end
        cycle: bool,
    },
}

impl Keys {
    /// A list of keys at sequence zero
    ///
    /// # Arguments
    ///
    /// * `keys` - The keys
    /// * `cycle` - Whether to start again at the end
    #[must_use]
    pub fn list(keys: Vec<Key>, cycle: bool) -> Self {
        Keys::List {
            keys: keys.into_iter().map(|key| (key, 0)).collect(),
            next: 0,
            cycle,
        }
    }

    /// A list of keys at a sequence
    ///
    /// # Arguments
    ///
    /// * `keys` - The keys
    /// * `seq` - The sequence each holds
    /// * `cycle` - Whether to start again at the end
    #[must_use]
    pub fn at(keys: Vec<Key>, seq: u64, cycle: bool) -> Self {
        Keys::List {
            keys: keys.into_iter().map(|key| (key, seq)).collect(),
            next: 0,
            cycle,
        }
    }

    /// The next key and the sequence it holds, or none when they are spent
    ///
    /// # Arguments
    ///
    /// * `kind` - The table, which a fresh cursor's keys are made for
    fn take(&mut self, kind: Kind) -> Option<(usize, Key, u64)> {
        match self {
            Keys::Fresh(cursor) => {
                let key = match kind {
                    Kind::Stripe | Kind::Digest => Key::Stripe(cursor.next_stripe()),
                    Kind::Object(_) => Key::Object(cursor.next_object()),
                };
                Some((usize::MAX, key, 0))
            }
            Keys::List { keys, next, cycle } => {
                // past the end: again from the start, or spent
                if *next >= keys.len() {
                    if !*cycle || keys.is_empty() {
                        return None;
                    }
                    *next = 0;
                }
                let at = *next;
                *next += 1;
                Some((at, keys[at].0, keys[at].1))
            }
        }
    }

    /// Move a listed key's sequence on after its write applied, or drop it after one failed
    ///
    /// A write whose answer never came may or may not have applied, so its key's sequence is no
    /// longer known and the key is not used again.
    ///
    /// # Arguments
    ///
    /// * `at` - The key's place in the list
    /// * `applied` - Whether the write applied
    fn settle(&mut self, at: usize, applied: bool) {
        if let Keys::List { keys, next, .. } = self {
            if at >= keys.len() {
                return;
            }
            if applied {
                keys[at].1 += 1;
            } else {
                keys.remove(at);
                // the list shifted under the cursor
                if *next > at {
                    *next -= 1;
                }
            }
        }
    }

    /// The keys a list holds now, with their sequences
    #[must_use]
    pub fn listed(&self) -> Vec<(Key, u64)> {
        match self {
            Keys::Fresh(_) => Vec::new(),
            Keys::List { keys, .. } => keys.clone(),
        }
    }
}

/// How a commit reads its row first, as S7's coordinator does
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFirst {
    /// No read: the commit alone
    No,
    /// A `Quorum` read through the group's leader, then the commit through it
    Leader,
    /// A `One` read through a follower, then the commit through that follower
    Follower,
    /// A `One` read through every member, then the commit through the leader
    Every,
    /// No read: the commit alone, through a follower
    Through,
}

/// What a writer does with each key
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Write {
    /// Insert the whole row, which never reads what was there
    Insert,
    /// Update the row if it holds the sequence the writer knows, reading it first or not
    Commit(ReadFirst),
    /// Read the row at `One` through a member
    Get,
}

/// A worker that writes or reads one key at a time
pub struct Writer {
    /// The table
    pub kind: Kind,
    /// What it does with each key
    pub write: Write,
    /// Its keys
    pub keys: Keys,
    /// Which member leads each tablet
    pub route: Arc<Route>,
    /// Every member's client
    pub clients: Arc<Vec<Client>>,
    /// The member a get is sent through, and a follower's commit
    pub home: usize,
    /// The key the last operation was for, by its place in the list
    pending: Option<usize>,
}

impl Writer {
    /// A worker over some keys
    ///
    /// # Arguments
    ///
    /// * `kind` - The table
    /// * `write` - What it does with each key
    /// * `keys` - Its keys
    /// * `route` - Which member leads each tablet
    /// * `clients` - Every member's client
    /// * `home` - The member its gets are sent through
    #[must_use]
    pub fn new(
        kind: Kind,
        write: Write,
        keys: Keys,
        route: Arc<Route>,
        clients: Arc<Vec<Client>>,
        home: usize,
    ) -> Box<dyn Work> {
        Box::new(Writer::build(kind, write, keys, route, clients, home))
    }

    /// A worker over some keys, unboxed, for a mixture to hold
    ///
    /// # Arguments
    ///
    /// * `kind` - The table
    /// * `write` - What it does with each key
    /// * `keys` - Its keys
    /// * `route` - Which member leads each tablet
    /// * `clients` - Every member's client
    /// * `home` - The member its gets are sent through
    #[must_use]
    pub fn build(
        kind: Kind,
        write: Write,
        keys: Keys,
        route: Arc<Route>,
        clients: Arc<Vec<Client>>,
        home: usize,
    ) -> Writer {
        Writer {
            kind,
            write,
            keys,
            route,
            clients,
            home,
            pending: None,
        }
    }
}

impl Writer {
    /// The first member from this worker's home that does not lead a key's group
    ///
    /// # Arguments
    ///
    /// * `key` - The key
    fn follower(&self, key: &Key) -> usize {
        let lead = self.route.leader(key);
        (0..self.clients.len())
            .map(|offset| (self.home + offset) % self.clients.len())
            .find(|member| *member != lead)
            .unwrap_or(lead)
    }
}

impl Work for Writer {
    /// The next key's operation
    fn next(&mut self, last: Option<&Outcome>) -> Option<Op> {
        // the last key's sequence moves on, or the key is dropped, by what it was answered
        if let (Some(at), Some(last)) = (self.pending.take(), last) {
            if matches!(self.write, Write::Commit(_)) {
                self.keys.settle(at, matches!(last.answer, Answer::Applied));
            }
        }
        let (at, key, seq) = self.keys.take(self.kind)?;
        self.pending = Some(at);
        let leader = self.clients[self.route.leader(&key)].clone();
        Some(match self.write {
            Write::Insert => drive::write(leader, insert(self.kind, &key, seq)),
            Write::Get => drive::read(self.clients[self.home].clone(), get(&key), ReadLevel::One),
            Write::Commit(ReadFirst::No) => drive::write(leader, commit(self.kind, &key, seq)),
            Write::Commit(ReadFirst::Leader) => drive::read_then_write(
                vec![leader.clone()],
                get(&key),
                ReadLevel::Quorum,
                leader,
                commit(self.kind, &key, seq),
            ),
            Write::Commit(ReadFirst::Follower) => {
                let follower = self.clients[self.follower(&key)].clone();
                drive::read_then_write(
                    vec![follower.clone()],
                    get(&key),
                    ReadLevel::One,
                    follower,
                    commit(self.kind, &key, seq),
                )
            }
            Write::Commit(ReadFirst::Through) => {
                let follower = self.clients[self.follower(&key)].clone();
                drive::write(follower, commit(self.kind, &key, seq))
            }
            Write::Commit(ReadFirst::Every) => drive::read_then_write(
                self.clients.iter().cloned().collect(),
                get(&key),
                ReadLevel::One,
                leader,
                commit(self.kind, &key, seq),
            ),
        })
    }
}

/// A worker that inserts and reads in a seeded mixture
pub struct Mixer {
    /// The inserting half
    pub insert: Writer,
    /// The reading half
    pub read: Writer,
    /// Where each choice comes from
    pub rng: Rng,
    /// The share of operations that read, in percent
    pub read_percent: u64,
}

impl Work for Mixer {
    /// The next operation, a read or an insert by the mixture
    fn next(&mut self, last: Option<&Outcome>) -> Option<Op> {
        // neither half tracks sequences, so the last answer changes nothing
        let _ = last;
        if self.rng.below(100) < self.read_percent {
            self.read.next(None)
        } else {
            self.insert.next(None)
        }
    }
}

/// Write rows as fast as a depth allows, failing on any refusal or failure
///
/// The preload is not a measurement: its rate is reported and nothing else.
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `kind` - The table
/// * `keys` - The keys, each written once at sequence zero
/// * `route` - Which member leads each tablet
/// * `depth` - How many inserts are in flight
///
/// # Errors
///
/// When an insert fails.
pub async fn preload(
    ctx: &Ctx,
    kind: Kind,
    keys: Vec<Key>,
    route: &Arc<Route>,
    depth: usize,
) -> color_eyre::Result<f64> {
    // the keys shared out, one worker taking every depth-th
    let count = keys.len();
    let keys = Arc::new(keys);
    let next = Arc::new(AtomicU64::new(0));
    let clients = ctx.clients();
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(depth);
    for _ in 0..depth {
        let (keys, next, clients, route) = (keys.clone(), next.clone(), clients.clone(), route.clone());
        tasks.push(tokio::spawn(async move {
            let mut failures = Vec::new();
            loop {
                // the next key nobody has taken
                let at = usize::try_from(next.fetch_add(1, Ordering::Relaxed)).unwrap_or(usize::MAX);
                let Some(key) = keys.get(at) else { break };
                let client = clients[route.leader(key)].clone();
                // an insert that times out is sent again: it replaces the row, so it is safe
                let mut tries = 0;
                loop {
                    let outcome = drive::write(client.clone(), insert(kind, key, 0)).await;
                    match outcome.answer {
                        Answer::Applied => break,
                        _ if tries < 3 => tries += 1,
                        answer => {
                            failures.push(format!("{key:?}: {answer:?}"));
                            break;
                        }
                    }
                }
            }
            failures
        }));
    }
    let mut failures = Vec::new();
    for task in tasks {
        failures.extend(task.await?);
    }
    if !failures.is_empty() {
        bail!("{} preload inserts failed, the first {:?}", failures.len(), failures.first());
    }
    Ok(count as f64 / started.elapsed().as_secs_f64())
}

/// The keys of one table, each worker's own share, from a fresh cursor
///
/// # Arguments
///
/// * `space` - The space the cell writes in
/// * `workers` - How many workers
/// * `tablets` - The tablets the keys must land in, or none for any
#[must_use]
pub fn fresh(space: u64, workers: usize, tablets: Option<&Arc<Tablets>>) -> Vec<Keys> {
    (0..workers)
        .map(|worker| Keys::Fresh(Cursor::new(space, worker, workers, tablets.cloned())))
        .collect()
}

/// A list of keys dealt out to workers, one in every `workers`
///
/// # Arguments
///
/// * `keys` - The keys
/// * `workers` - How many workers
#[must_use]
pub fn deal<T: Clone>(keys: &[T], workers: usize) -> Vec<Vec<T>> {
    let mut hands = vec![Vec::new(); workers.max(1)];
    for (at, key) in keys.iter().enumerate() {
        hands[at % workers.max(1)].push(key.clone());
    }
    hands
}

/// The first `count` keys of a space, of one table, in the tablets given or any
///
/// # Arguments
///
/// * `kind` - The table
/// * `space` - The space
/// * `count` - How many
/// * `tablets` - The tablets they must land in, or none for any
#[must_use]
pub fn keys_of(kind: Kind, space: u64, count: u64, tablets: Option<&Arc<Tablets>>) -> Vec<Key> {
    let mut cursor = Cursor::new(space, 0, 1, tablets.cloned());
    (0..count)
        .map(|_| match kind {
            Kind::Stripe | Kind::Digest => Key::Stripe(cursor.next_stripe()),
            Kind::Object(_) => Key::Object(cursor.next_object()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A listed key moves its sequence on when applied and is dropped when not
    #[test]
    fn a_listed_key_is_settled_by_its_answer() {
        let keys = vec![Key::Object(1), Key::Object(2), Key::Object(3)];
        let mut list = Keys::list(keys, true);
        let (first, ..) = list.take(Kind::Object(0)).expect("a key");
        list.settle(first, true);
        let (second, ..) = list.take(Kind::Object(0)).expect("a key");
        list.settle(second, false);
        // the dropped key is gone and the applied one moved on
        let listed = list.listed();
        assert_eq!(listed, vec![(Key::Object(1), 1), (Key::Object(3), 0)]);
        // and the cursor carries on with the third
        let (_, key, _) = list.take(Kind::Object(0)).expect("a key");
        assert_eq!(key, Key::Object(3));
    }

    /// Dealt keys go one in every `workers` to each
    #[test]
    fn keys_are_dealt_round_the_table() {
        let hands = deal(&[1, 2, 3, 4, 5], 2);
        assert_eq!(hands, vec![vec![1, 3, 5], vec![2, 4]]);
    }
}
