//! Leg `remedy`: whether the read S7's coordinator makes removes a cold commit's stall under load
//!
//! A supplement to the rounds, added once round one's `rows` leg showed what T1 measures: at depth
//! one a cold commit costs what a warm one does, but a group taking cold commits at depth thirty
//! two commits fewer, and a writer beside eight cold writers in its group waits longer than beside
//! eight warm ones. The rule written before the run chooses the remedy from depth one alone; this
//! asks it under the load the stall appeared under.
//!
//! On a group a Zen1 host leads, with every row cold after a restart:
//!
//! - thirty-two writers commit cold rows: with no read, after a `Quorum` read through the
//!   group's leader, and after a `One` read through every member; then thirty-two commit rows
//!   already resident;
//! - one writer of resident rows runs alone, then beside eight writers of cold rows with no read,
//!   beside eight that read through the leader first, and beside eight writers of resident rows.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use color_eyre::eyre::bail;

use crate::cluster::{Group, Lab};
use crate::drive::{self, Plan};
use crate::keys::{stripes_by_group, Tablets};
use crate::measure::rate::{QUIET, SETTLE_LIMIT};
use crate::measure::rows::{budgeted, take, write_filler, writers_reading};
use crate::measure::{deal, fill, keys_of, preload, snap, Ctx, Key, Keys, Kind, ReadFirst, Route, Targets};

/// The space the leg's stripe rows are numbered in
const REMEDY: u64 = 20;

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
    // conditional writes need the newest wire version, and the leaders their settling
    ctx.lab.activate().await?;
    let stripe = Kind::Stripe.table();
    let filler = <crate::Filler as shoal::shared::traits::PartitionKeySupport>::name();
    let settled = ctx.lab.settle(&[stripe, filler], QUIET, SETTLE_LIMIT).await?;
    // the stripe rows, then filler until they are archived and checkpointed, then a restart
    let count = ctx.scale(3_000_000, 80_000);
    let route = Arc::new(Route::of(&settled[stripe]));
    let rate = preload(ctx, Kind::Stripe, keys_of(Kind::Stripe, REMEDY, count, None), &route, 96).await?;
    println!("remedy | loaded {count} stripe rows at {rate:.0}/s");
    let filler_route = Arc::new(Route::of(&settled[filler]));
    let mut filler_next = 0;
    let mut rounds = 0;
    loop {
        write_filler(ctx, &filler_route, &mut filler_next, ctx.scale(4_096, 1_024)).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        if Lab::archived(&ctx.lab.replication().await?, &[stripe]) {
            break;
        }
        rounds += 1;
        if rounds > 400 {
            bail!("the stripe rows were never archived and checkpointed");
        }
    }
    ctx.lab.restart().await?;
    let settled = ctx.lab.settle(&[stripe, filler], QUIET, SETTLE_LIMIT).await?;
    // the group a Zen1 host leads, and the cold rows in it
    let groups: &Vec<Group> = &settled[stripe];
    let route = Arc::new(Route::of(groups));
    let targets = Targets::choose(&ctx.lab, groups, &ctx.fast)?;
    let leader = ctx.lab.members[targets.slow.leader].name.clone();
    let mut pool: VecDeque<Key> = stripes_by_group(REMEDY, count, &[Tablets::of(&targets.slow.tablets)])
        .remove(0)
        .into_iter()
        .map(Key::Stripe)
        .collect();
    println!("remedy | group {} led by {leader} holds {} cold stripe rows", targets.slow.id, pool.len());
    let clients = ctx.clients();
    let deep = ctx.scale(30_000, 800) as usize;
    let beside = ctx.scale(20_000, 400) as usize;
    let window = ctx.plan(15).window;
    // thirty-two committers of cold rows: no read, a read at the leader, a read through every member
    let mut sides = vec![
        ("cold", ReadFirst::No),
        ("read-leader", ReadFirst::Leader),
        ("read-every", ReadFirst::Every),
    ];
    if ctx.reversed() {
        sides.reverse();
    }
    for (side, first) in sides {
        let keys = take(&mut pool, deep)?;
        let hands = deal(&keys, 32).into_iter().map(|hand| Keys::at(hand, 0, false)).collect();
        let workers = writers_reading(first, hands, &route, &clients);
        let mut record = ctx.record("remedy", "stripe commit aim=slow depth=32", side);
        record.label("leader", leader.clone());
        let plan = Plan { warmup: Duration::ZERO, window };
        let (record, _) = crate::measure::measured(ctx, record, plan, workers).await?;
        ctx.emit(&[record])?;
    }
    // rows made resident by one commit each: for the depth 32 warm cell, for the warm writers
    // beside the measured one, and for the measured writer itself, each set its own, since a set
    // a cell has committed to holds sequences only that cell's writers know
    let warm_keys = take(&mut pool, ctx.scale(6_000, 300) as usize)?;
    let beside_warm = take(&mut pool, ctx.scale(3_000, 100) as usize)?;
    let mut sets = Vec::new();
    for _ in 0..4 {
        sets.push(take(&mut pool, ctx.scale(300, 30) as usize)?);
    }
    let mut touched: Vec<Key> = warm_keys.clone();
    touched.extend(beside_warm.iter().copied());
    for set in &sets {
        touched.extend(set.iter().copied());
    }
    let hands = deal(&touched, 8).into_iter().map(|hand| Keys::at(hand, 0, false)).collect();
    drive::run(budgeted(), writers_reading(ReadFirst::No, hands, &route, &clients)).await;
    // thirty-two committers of the resident rows
    let hands = deal(&warm_keys, 32).into_iter().map(|hand| Keys::at(hand, 1, true)).collect();
    let mut record = ctx.record("remedy", "stripe commit aim=slow depth=32", "warm");
    record.label("leader", leader.clone());
    let plan = Plan { warmup: Duration::ZERO, window };
    let (record, _) = crate::measure::measured(ctx, record, plan, writers_reading(ReadFirst::No, hands, &route, &clients)).await?;
    ctx.emit(&[record])?;
    // one writer of resident rows, alone and beside eight others
    let mut besides = vec!["alone", "cold", "cold-read-leader", "warm"];
    if ctx.reversed() {
        besides.reverse();
    }
    for (set, side) in sets.into_iter().zip(besides) {
        let writer = writers_reading(ReadFirst::No, vec![Keys::at(set, 1, true)], &route, &clients);
        let other = match side {
            "cold" => {
                let keys = take(&mut pool, beside)?;
                writers_reading(ReadFirst::No, deal(&keys, 8).into_iter().map(|hand| Keys::at(hand, 0, false)).collect(), &route, &clients)
            }
            "cold-read-leader" => {
                let keys = take(&mut pool, beside)?;
                writers_reading(ReadFirst::Leader, deal(&keys, 8).into_iter().map(|hand| Keys::at(hand, 0, false)).collect(), &route, &clients)
            }
            "warm" => writers_reading(
                ReadFirst::No,
                deal(&beside_warm, 8).into_iter().map(|hand| Keys::at(hand, 1, true)).collect(),
                &route,
                &clients,
            ),
            _ => Vec::new(),
        };
        let before = snap(&ctx.lab).await?;
        let mut cells = drive::run_beside(ctx.plan(10), vec![writer, other]).await;
        let after = snap(&ctx.lab).await?;
        let other = cells.pop().unwrap_or_default();
        let measured = cells.pop().unwrap_or_default();
        let mut record = ctx.record("remedy", "neighbour aim=slow", side);
        record.label("leader", leader.clone());
        fill(&mut record, &measured, &ctx.lab, &before, &after);
        record
            .set("beside_per_sec", other.per_sec())
            .set("beside_p50_us", other.latency.summary().p50)
            .set("beside_rest_p50_us", other.rest.summary().p50)
            .set("beside_failed", other.failed as f64)
            .set("beside_refused", other.refused as f64)
            .set("beside_exhausted", if other.exhausted { 1.0 } else { 0.0 });
        ctx.emit(&[record])?;
    }
    Ok(())
}
