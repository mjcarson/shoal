//! Leg `rows`: bytes a row on disk and in memory, then the cold commit
//!
//! 1. **A baseline.** Filler rows only, then every node restarted: what a node holds with none
//!    of the measured rows, read as each member reports it.
//! 2. **The load.** Millions of stripe rows, a million with a digest a chunk, and a million
//!    object rows, written once each.
//! 3. **The seal.** Filler rows are written until every group of the three tables has archived
//!    and durably checkpointed everything it applied on every member. A restart applies
//!    everything above a group's checkpoint again, resident, so this is what makes the rows cold.
//! 4. **The figures, sealed and then cold.** Each member's rows, archive bytes, indexes and
//!    resident memory, then every node restarted and the same again.
//! 5. **The cold commit**, on the rows the restart left cold: S7's commit at depth one, cold and
//!    then warm on the same rows; the same commit after a read through the leader, through a
//!    follower and through every member; cold against warm at depth thirty-two; and a writer of
//!    resident rows beside a writer of cold ones in its own group, against one in another.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use color_eyre::eyre::bail;
use shoal::shared::protocol::stats::NodeStats;

use crate::cluster::{now_ms, Lab};
use crate::drive::{self, Plan, Work};
use crate::keys::{objects_by_group, stripes_by_group, Tablets};
use crate::measure::rate::{QUIET, SETTLE_LIMIT};
use crate::measure::{
    deal, fill, keys_of, preload, snap, Ctx, Key, Keys, Kind, ReadFirst, Route, Targets, Write, Writer,
};

/// The space the loaded stripe rows are numbered in
const LOAD_STRIPE: u64 = 10;

/// The space the loaded digest rows are numbered in
const LOAD_DIGEST: u64 = 11;

/// The space the loaded object rows are numbered in
const LOAD_OBJECT: u64 = 12;

/// The space filler rows are numbered in
const FILLER: u64 = 13;

/// How many rows the leg loads into each table
struct Counts {
    /// Stripe rows
    stripe: u64,
    /// Digest rows
    digest: u64,
    /// Object rows
    object: u64,
}

/// Run the leg
///
/// # Arguments
///
/// * `ctx` - The leg
///
/// # Errors
///
/// When the cluster cannot be driven or read, or the rows never become cold.
pub async fn run(ctx: &mut Ctx) -> color_eyre::Result<()> {
    // conditional writes need the newest wire version
    ctx.lab.activate().await?;
    let counts = Counts {
        stripe: ctx.scale(2_000_000, 60_000),
        digest: ctx.scale(1_000_000, 20_000),
        object: ctx.scale(1_000_000, 20_000),
    };
    let measured_tables = [Kind::Stripe.table(), Kind::Digest.table(), Kind::Object(0).table()];
    let filler_table = <crate::Filler as shoal::shared::traits::PartitionKeySupport>::name();
    let mut every = measured_tables.to_vec();
    every.push(filler_table);
    let settled = ctx.lab.settle(&every, QUIET, SETTLE_LIMIT).await?;
    let filler_route = Arc::new(Route::of(&settled[filler_table]));
    let mut filler_next = 0;
    // 1. the baseline: filler alone, every node restarted
    write_filler(ctx, &filler_route, &mut filler_next, ctx.scale(16_384, 1_024)).await?;
    ctx.lab.restart().await?;
    ctx.lab.settle(&every, QUIET, SETTLE_LIMIT).await?;
    let baseline = ctx.lab.node_stats(now_ms()).await?;
    emit_nodes(ctx, "baseline", &baseline, &BTreeMap::new())?;
    // 2. the load, every table at once in turn, each write to its group's leader
    let settled = ctx.lab.settle(&every, QUIET, SETTLE_LIMIT).await?;
    let mut loaded = Vec::new();
    let members = ctx.lab.members.len().max(1) as f64;
    let first = snap(&ctx.lab).await?;
    let mut before = snap(&ctx.lab).await?;
    for (kind, space, count) in [
        (Kind::Stripe, LOAD_STRIPE, counts.stripe),
        (Kind::Digest, LOAD_DIGEST, counts.digest),
        (Kind::Object(0), LOAD_OBJECT, counts.object),
    ] {
        let route = Arc::new(Route::of(&settled[kind.table()]));
        let keys = keys_of(kind, space, count, None);
        let rate = preload(ctx, kind, keys, &route, 96).await?;
        // what the hosts wrote for this table's load, and through the WAL
        let after = snap(&ctx.lab).await?;
        let delta = ctx.lab.delta(&before.counters, &after.counters);
        let wal_bytes = after.wal.1.saturating_sub(before.wal.1) as f64;
        let mut record = ctx.record("rows", &format!("load {}", kind.short()), "-");
        record
            .set("per_sec", rate)
            .set("rows", count as f64)
            .set("device_written_per_row", delta.device_written as f64 / count as f64)
            .set("wal_bytes_per_row_per_replica", wal_bytes / count as f64 / members);
        loaded.push(record);
        before = after;
    }
    // every member's figures with the rows loaded and resident, before any filler
    let resident = ctx.lab.node_stats(now_ms()).await?;
    emit_nodes(ctx, "loaded", &resident, &BTreeMap::new())?;
    // 3. the seal: filler until every measured group has archived and checkpointed it all
    let mut rounds = 0;
    loop {
        write_filler(ctx, &filler_route, &mut filler_next, ctx.scale(4_096, 1_024)).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let reports = ctx.lab.replication().await?;
        if Lab::archived(&reports, &measured_tables) {
            break;
        }
        rounds += 1;
        if rounds > 400 {
            bail!("the loaded rows were never archived and checkpointed");
        }
    }
    let after_seal = snap(&ctx.lab).await?;
    let sealing = ctx.lab.delta(&before.counters, &after_seal.counters);
    let loading = ctx.lab.delta(&first.counters, &before.counters);
    let total_rows = (counts.stripe + counts.digest + counts.object) as f64;
    for record in &mut loaded {
        record
            .set("load_device_written", loading.device_written as f64)
            .set("seal_device_written", sealing.device_written as f64)
            .set("filler_rows", filler_next as f64);
    }
    ctx.emit(&loaded)?;
    // 4. the figures sealed, then cold after every node restarted
    let disk = ctx.lab.disk_usage()?;
    let sealed = ctx.lab.node_stats(now_ms()).await?;
    emit_nodes(ctx, "sealed", &sealed, &disk)?;
    ctx.lab.restart().await?;
    let settled = ctx.lab.settle(&every, QUIET, SETTLE_LIMIT).await?;
    let cold = ctx.lab.node_stats(now_ms()).await?;
    emit_nodes(ctx, "cold", &cold, &BTreeMap::new())?;
    // what a row costs, the largest member's figure
    let mut summary = ctx.record("rows", "per row", "-");
    summary.set("rows", total_rows);
    let largest = |stats: &[NodeStats], figure: fn(&NodeStats) -> u64| {
        stats.iter().map(figure).max().unwrap_or(0) as f64
    };
    let partitions = |stats: &NodeStats| stats.total.partitions;
    summary
        .set("partitions_cold", largest(&cold, partitions))
        .set(
            "archive_map_per_partition",
            largest(&cold, |stats| stats.archive_map_bytes) / largest(&cold, partitions).max(1.0),
        )
        .set(
            "archive_map_per_row_delta",
            (largest(&cold, |stats| stats.archive_map_bytes) - largest(&baseline, |stats| stats.archive_map_bytes))
                / total_rows,
        )
        .set(
            "resident_per_row_delta",
            (largest(&cold, |stats| stats.resident_bytes) - largest(&baseline, |stats| stats.resident_bytes))
                / total_rows,
        )
        .set(
            "table_index_per_row_loaded",
            (largest(&resident, |stats| stats.table_index_bytes) - largest(&baseline, |stats| stats.table_index_bytes))
                / total_rows,
        )
        .set(
            "memory_per_row_loaded",
            (largest(&resident, |stats| stats.memory_bytes) - largest(&baseline, |stats| stats.memory_bytes))
                / total_rows,
        )
        .set(
            "resident_per_row_loaded",
            (largest(&resident, |stats| stats.resident_bytes) - largest(&baseline, |stats| stats.resident_bytes))
                / total_rows,
        )
        .set(
            "lru_per_partition_sealed",
            largest(&sealed, |stats| stats.lru_bytes) / largest(&sealed, partitions).max(1.0),
        )
        .set("memory_cold", largest(&cold, |stats| stats.memory_bytes));
    // and each table's archive bytes a row, from the members' own count
    for kind in [Kind::Stripe, Kind::Digest, Kind::Object(0)] {
        let per_row = sealed
            .iter()
            .filter_map(|stats| stats.tables.iter().find(|table| table.table == kind.table()))
            .map(|table| table.bytes as f64 / table.partitions.max(1) as f64)
            .fold(0.0, f64::max);
        summary.set(&format!("archive_bytes_per_row_{}", kind.short()), per_row);
    }
    ctx.emit(&[summary])?;
    // 5. the cold commit, on the rows the restart left cold
    cold_commits(ctx, &settled, &counts).await
}

/// Write filler rows, each to its group's leader
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `route` - Which member leads each of the filler table's tablets
/// * `next` - The next filler row's number, moved on past the ones written
/// * `count` - How many to write
///
/// # Errors
///
/// When a filler row cannot be written.
pub(crate) async fn write_filler(ctx: &Ctx, route: &Arc<Route>, next: &mut u64, count: u64) -> color_eyre::Result<()> {
    // filler rows are numbered on from the last written, so none is written twice
    let keys: Vec<Key> = (*next..*next + count)
        .map(|n| Key::Object(crate::keys::object_key(FILLER, n)))
        .collect();
    *next += count;
    filler_preload(ctx, route, keys).await
}

/// Insert filler rows as fast as a depth of sixty-four allows
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `route` - Which member leads each of the filler table's tablets
/// * `keys` - The filler rows' keys
///
/// # Errors
///
/// When a filler row cannot be written.
async fn filler_preload(ctx: &Ctx, route: &Arc<Route>, keys: Vec<Key>) -> color_eyre::Result<()> {
    // the filler table's rows go where their own group's leader is
    let clients = ctx.clients();
    let keys = Arc::new(keys);
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..64 {
        let (keys, next, clients, route) = (keys.clone(), next.clone(), clients.clone(), route.clone());
        tasks.push(tokio::spawn(async move {
            loop {
                let at = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let Some(Key::Object(key)) = keys.get(at) else { break };
                let hash = <crate::Filler as shoal::shared::traits::PartitionKeySupport>::get_partition_key_from_values(key);
                let leader = route.leader_of_hash(hash);
                // a filler row lost to a timeout matters to nobody
                let _ = clients[leader].send_one(crate::shape::filler(*key)).await;
            }
        }));
    }
    for task in tasks {
        task.await?;
    }
    Ok(())
}

/// Emit a record a member of its figures at a point in the leg
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `side` - `baseline`, `sealed` or `cold`
/// * `stats` - Every member's figures, in member order
/// * `disk` - The bytes each member's directories hold on disk, by `<node> <directory>`
///
/// # Errors
///
/// When the output cannot be written.
fn emit_nodes(
    ctx: &Ctx,
    side: &str,
    stats: &[NodeStats],
    disk: &BTreeMap<String, u64>,
) -> color_eyre::Result<()> {
    let mut records = Vec::new();
    for (member, stats) in ctx.lab.members.iter().zip(stats) {
        let mut record = ctx.record("rows", &format!("node {}", member.name), side);
        record
            .set("partitions", stats.total.partitions as f64)
            .set("archive_bytes", stats.total.bytes as f64)
            .set("memory_bytes", stats.memory_bytes as f64)
            .set("archive_map_bytes", stats.archive_map_bytes as f64)
            .set("table_index_bytes", stats.table_index_bytes as f64)
            .set("wal_index_bytes", stats.wal_index_bytes as f64)
            .set("lru_bytes", stats.lru_bytes as f64)
            .set("resident_bytes", stats.resident_bytes as f64);
        // each table's own rows and bytes
        for table in &stats.tables {
            record
                .set(&format!("partitions:{}", table.table), table.partitions as f64)
                .set(&format!("bytes:{}", table.table), table.bytes as f64);
        }
        // and what its directories hold on disk
        for (place, bytes) in disk {
            if let Some(dir) = place.strip_prefix(&format!("{} ", member.name)) {
                record.set(&format!("du:{dir}"), *bytes as f64);
            }
        }
        records.push(record);
    }
    ctx.emit(&records)
}

/// The cold commit's phases, on the rows a restart left cold
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `settled` - Every table's groups after the restart, settled
/// * `counts` - How many rows were loaded
///
/// # Errors
///
/// When the cluster cannot be driven or read.
async fn cold_commits(
    ctx: &mut Ctx,
    settled: &BTreeMap<String, Vec<crate::cluster::Group>>,
    counts: &Counts,
) -> color_eyre::Result<()> {
    let clients = ctx.clients();
    // the stripe table's groups to aim at, and the loaded keys in each
    let groups = &settled[Kind::Stripe.table()];
    let route = Arc::new(Route::of(groups));
    let targets = Targets::choose(&ctx.lab, groups, &ctx.fast)?;
    let aims = [&targets.slow, &targets.fast, &targets.control];
    let tablets: Vec<Tablets> = aims.iter().map(|group| Tablets::of(&group.tablets)).collect();
    let mut pools: Vec<VecDeque<Key>> = stripes_by_group(LOAD_STRIPE, counts.stripe, &tablets)
        .into_iter()
        .map(|keys| keys.into_iter().map(Key::Stripe).collect())
        .collect();
    for (group, pool) in aims.iter().zip(&pools) {
        println!(
            "rows | group {} led by {} holds {} cold stripe rows",
            group.id,
            ctx.lab.members[group.leader].name,
            pool.len()
        );
    }
    let one = ctx.scale(1_000, 50) as usize;
    let deep = ctx.scale(40_000, 1_000) as usize;
    let beside = ctx.scale(30_000, 500) as usize;
    // the slow group first in odd rounds, the fast one first in even
    let order: Vec<(usize, &str)> = if ctx.reversed() {
        vec![(1, "fast"), (0, "slow")]
    } else {
        vec![(0, "slow"), (1, "fast")]
    };
    for (index, aim) in order {
        let leader = ctx.lab.members[aims[index].leader].name.clone();
        let cell = format!("stripe commit aim={aim}");
        // a: cold rows at depth one, then b: the same rows again, now resident
        let cold_keys = take(&mut pools[index], one)?;
        run_keys(ctx, &cell, "cold", &leader, ReadFirst::No, Keys::at(cold_keys.clone(), 0, false), &route, 1).await?;
        run_keys(ctx, &cell, "warm", &leader, ReadFirst::No, Keys::at(cold_keys.clone(), 1, false), &route, 1).await?;
        // c: the commit after a read, through the leader, a follower and every member
        for (side, first) in [
            ("read-leader", ReadFirst::Leader),
            ("read-follower", ReadFirst::Follower),
            ("read-every", ReadFirst::Every),
        ] {
            let keys = take(&mut pools[index], one)?;
            run_keys(ctx, &cell, side, &leader, first, Keys::at(keys.clone(), 0, false), &route, 1).await?;
            // a commit through a follower waits on that follower's own apply as well as the
            // leader's, cold or not, so the same rows, now resident, are committed through it
            // again with no read: the path's own cost, apart from the row's
            if first == ReadFirst::Follower {
                run_keys(ctx, &cell, "warm-follower", &leader, ReadFirst::Through, Keys::at(keys, 1, false), &route, 1)
                    .await?;
            }
        }
        // d: cold against warm at depth thirty-two
        let keys = take(&mut pools[index], deep)?;
        let hands = deal(&keys, 32).into_iter().map(|hand| Keys::at(hand, 0, false)).collect();
        run_hands(ctx, &format!("{cell} depth=32"), "cold", &leader, hands, &route).await?;
        let hands = deal(&cold_keys, 32).into_iter().map(|hand| Keys::at(hand, 2, true)).collect();
        run_hands(ctx, &format!("{cell} depth=32"), "warm", &leader, hands, &route).await?;
    }
    // e: a writer of resident rows in the slow group, alone and beside others
    let leader = ctx.lab.members[targets.slow.leader].name.clone();
    let mut sets = Vec::new();
    for _ in 0..4 {
        // its own rows, made resident by one commit each first
        let keys = take(&mut pools[0], ctx.scale(300, 30) as usize)?;
        let hands = deal(&keys, 8).into_iter().map(|hand| Keys::at(hand, 0, false)).collect();
        let workers = writers(hands, &route, &clients);
        drive::run(Plan { warmup: Duration::ZERO, window: Duration::from_secs(300) }, workers).await;
        sets.push(keys);
    }
    let warm_beside: Vec<Key> = take(&mut pools[0], ctx.scale(3_000, 100) as usize)?;
    // and the warm writer beside it, made resident the same way
    let hands = deal(&warm_beside, 8).into_iter().map(|hand| Keys::at(hand, 0, false)).collect();
    drive::run(Plan { warmup: Duration::ZERO, window: Duration::from_secs(300) }, writers(hands, &route, &clients)).await;
    for (set, side) in sets.into_iter().zip(["alone", "cold-same", "warm-same", "cold-other"]) {
        // the measured writer: one at a time over its resident rows
        let writer = writers(vec![Keys::at(set, 1, true)], &route, &clients);
        // and the writer beside it, eight deep
        let other: Vec<Box<dyn Work>> = match side {
            "cold-same" => {
                let keys = take(&mut pools[0], beside)?;
                writers(deal(&keys, 8).into_iter().map(|hand| Keys::at(hand, 0, false)).collect(), &route, &clients)
            }
            "warm-same" => writers(
                deal(&warm_beside, 8).into_iter().map(|hand| Keys::at(hand, 1, true)).collect(),
                &route,
                &clients,
            ),
            "cold-other" => {
                let keys = take(&mut pools[2], beside)?;
                writers(deal(&keys, 8).into_iter().map(|hand| Keys::at(hand, 0, false)).collect(), &route, &clients)
            }
            _ => Vec::new(),
        };
        let before = snap(&ctx.lab).await?;
        let mut cells = drive::run_beside(ctx.plan(10), vec![writer, other]).await;
        let after = snap(&ctx.lab).await?;
        let other = cells.pop().unwrap_or_default();
        let measured = cells.pop().unwrap_or_default();
        let mut record = ctx.record("cold", "neighbour aim=slow", side);
        record.label("leader", leader.clone());
        record.label(
            "control_leader",
            format!("{} shard {}", ctx.lab.members[targets.control.leader].name, targets.control.shard),
        );
        fill(&mut record, &measured, &ctx.lab, &before, &after);
        record
            .set("beside_per_sec", other.per_sec())
            .set("beside_p50_us", other.latency.summary().p50)
            .set("beside_failed", other.failed as f64)
            .set("beside_refused", other.refused as f64)
            .set("beside_exhausted", if other.exhausted { 1.0 } else { 0.0 });
        ctx.emit(&[record])?;
    }
    // the object table's commit, cold then warm, in a group a Zen1 host leads
    let groups = &settled[Kind::Object(0).table()];
    let object_route = Arc::new(Route::of(groups));
    let object_targets = Targets::choose(&ctx.lab, groups, &ctx.fast)?;
    let tablets = [Tablets::of(&object_targets.slow.tablets)];
    let mut objects: VecDeque<Key> = objects_by_group(LOAD_OBJECT, counts.object, &tablets)
        .remove(0)
        .into_iter()
        .map(Key::Object)
        .collect();
    let keys = take(&mut objects, one)?;
    let leader = ctx.lab.members[object_targets.slow.leader].name.clone();
    for (side, seq) in [("cold", 0), ("warm", 1)] {
        let worker = Writer::new(
            Kind::Object(0),
            Write::Commit(ReadFirst::No),
            Keys::at(keys.clone(), seq, false),
            object_route.clone(),
            clients.clone(),
            0,
        );
        let mut record = ctx.record("cold", "object commit aim=slow", side);
        record.label("leader", leader.clone());
        let (record, _) = crate::measure::measured(ctx, record, budgeted(), vec![worker]).await?;
        ctx.emit(&[record])?;
    }
    // what the rows the phases touched now cost in memory
    let touched = ctx.lab.node_stats(now_ms()).await?;
    emit_nodes(ctx, "touched", &touched, &BTreeMap::new())?;
    Ok(())
}

/// Take keys off the front of a group's pool of cold rows
///
/// # Arguments
///
/// * `pool` - The pool
/// * `count` - How many
///
/// # Errors
///
/// When the pool holds fewer.
pub(crate) fn take(pool: &mut VecDeque<Key>, count: usize) -> color_eyre::Result<Vec<Key>> {
    if pool.len() < count {
        bail!("a group has {} cold rows left and a phase needs {count}", pool.len());
    }
    Ok(pool.drain(..count).collect())
}

/// A plan for a cell that ends when its keys do: no warm-up, and a window long enough
pub(crate) fn budgeted() -> Plan {
    Plan {
        warmup: Duration::ZERO,
        window: Duration::from_secs(600),
    }
}

/// Committers over hands of keys, each to its key's group's leader
///
/// # Arguments
///
/// * `hands` - Each worker's keys
/// * `route` - Which member leads each tablet
/// * `clients` - Every member's client
fn writers(hands: Vec<Keys>, route: &Arc<Route>, clients: &Arc<Vec<crate::cluster::Client>>) -> Vec<Box<dyn Work>> {
    writers_reading(ReadFirst::No, hands, route, clients)
}

/// Committers over hands of keys that read each row first as they are told, each commit to its
/// key's group's leader unless the read says otherwise
///
/// # Arguments
///
/// * `first` - How each commit reads its row first
/// * `hands` - Each worker's keys
/// * `route` - Which member leads each tablet
/// * `clients` - Every member's client
pub(crate) fn writers_reading(
    first: ReadFirst,
    hands: Vec<Keys>,
    route: &Arc<Route>,
    clients: &Arc<Vec<crate::cluster::Client>>,
) -> Vec<Box<dyn Work>> {
    hands
        .into_iter()
        .enumerate()
        .map(|(worker, keys)| {
            Writer::new(
                Kind::Stripe,
                Write::Commit(first),
                keys,
                route.clone(),
                clients.clone(),
                worker % clients.len(),
            )
        })
        .collect()
}

/// Run one worker's commits over a list of keys, to its end, and record them
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `cell` - The cell
/// * `side` - The side
/// * `leader` - The leader of the group the keys are in, by name
/// * `first` - How the commit reads its row first
/// * `keys` - The keys
/// * `route` - Which member leads each tablet
/// * `depth` - How many workers
#[allow(clippy::too_many_arguments)]
async fn run_keys(
    ctx: &Ctx,
    cell: &str,
    side: &str,
    leader: &str,
    first: ReadFirst,
    keys: Keys,
    route: &Arc<Route>,
    depth: usize,
) -> color_eyre::Result<()> {
    let clients = ctx.clients();
    let worker = Writer::new(Kind::Stripe, Write::Commit(first), keys, route.clone(), clients.clone(), 1 % clients.len());
    let mut record = ctx.record("cold", &format!("{cell} depth={depth}"), side);
    record.label("leader", leader.to_string());
    let (record, _) = crate::measure::measured(ctx, record, budgeted(), vec![worker]).await?;
    ctx.emit(&[record])?;
    Ok(())
}

/// Run committers over hands of keys for a window, or until their keys run out, and record them
///
/// # Arguments
///
/// * `ctx` - The leg
/// * `cell` - The cell
/// * `side` - The side
/// * `leader` - The leader of the group the keys are in, by name
/// * `hands` - Each worker's keys
/// * `route` - Which member leads each tablet
async fn run_hands(
    ctx: &Ctx,
    cell: &str,
    side: &str,
    leader: &str,
    hands: Vec<Keys>,
    route: &Arc<Route>,
) -> color_eyre::Result<()> {
    let workers = writers(hands, route, &ctx.clients());
    let mut record = ctx.record("cold", cell, side);
    record.label("leader", leader.to_string());
    let plan = Plan {
        warmup: Duration::ZERO,
        window: ctx.plan(15).window,
    };
    let (record, _) = crate::measure::measured(ctx, record, plan, workers).await?;
    ctx.emit(&[record])?;
    Ok(())
}
