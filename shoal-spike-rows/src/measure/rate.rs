//! Leg `rate`: rows a second a tablet group commits, and one row rewritten (Q17)
//!
//! Each table is aimed three ways: at a group a Zen1 host leads, at one europa leads, and at
//! every group. Each aim has a pool of resident rows written before any cell, a quarter for each
//! depth's conditional updates and half for the overwrites, so a conditional update always knows
//! the sequence its row holds. Each cell is one of:
//!
//! - `insert`: a row under a key nobody wrote, which reads nothing;
//! - `overwrite`: a whole row over a resident one, which reads nothing either;
//! - `commit`: S7's commit, an update conditional on the sequence the writer read, of a resident
//!   row, which judges its condition against the row in memory.
//!
//! at depth one, which is what one write costs, and thirty-two, which is what a group can take.
//! Every write is sent to its group's leader. Then one object row is rewritten in a loop, a
//! conditional update each time, at four sizes: what a group pays to carry a record that changes
//! at its commit rate, which is the half of Q17 a table can answer.

use std::sync::Arc;
use std::time::Duration;

use crate::keys::Tablets;
use crate::measure::{
    deal, fresh, keys_of, measured, preload, Ctx, Key, Keys, Kind, ReadFirst, Route, Targets, Write,
    Writer,
};

/// How long the leaders must not move before a leg's cells start
pub const QUIET: Duration = Duration::from_secs(30);

/// How long a leg waits at most for its leaders to settle
pub const SETTLE_LIMIT: Duration = Duration::from_secs(600);

/// The inline sizes the one rewritten row is measured at
pub const REWRITE_SIZES: [usize; 4] = [0, 4 << 10, 64 << 10, 1 << 20];

/// One table aimed one way, with its resident pool
struct Aim {
    /// The table
    kind: Kind,
    /// `slow`, `fast` or `all`
    name: &'static str,
    /// The tablets of the group aimed at, or none for every group
    tablets: Option<Arc<Tablets>>,
    /// The member leading the group aimed at, by name, or `every` for every group
    leader: String,
    /// How many groups the cell's rows land in
    groups: usize,
    /// The resident rows written before any cell
    pool: Vec<Key>,
    /// Which member leads each tablet
    route: Arc<Route>,
    /// The space fresh keys are numbered in
    space: u64,
}

/// One cell: an aim, what is written, and how deep
struct Plan {
    /// The aim's place in the list
    aim: usize,
    /// `insert`, `overwrite` or `commit`
    op: &'static str,
    /// How many workers
    depth: usize,
}

/// Run the leg
///
/// # Arguments
///
/// * `ctx` - The leg
///
/// # Errors
///
/// When the cluster cannot be driven or read.
pub async fn run(ctx: &mut Ctx) -> color_eyre::Result<()> {
    // conditional writes need the newest wire version, and the leaders their settling
    ctx.lab.activate().await?;
    let kinds = [Kind::Stripe, Kind::Object(0)];
    let tables: Vec<&str> = kinds.iter().map(|kind| kind.table()).collect();
    let settled = ctx.lab.settle(&tables, QUIET, SETTLE_LIMIT).await?;
    // every table aimed three ways, each aim's pool written first
    let mut aims = Vec::new();
    for (index, kind) in kinds.iter().enumerate() {
        let groups = &settled[kind.table()];
        let route = Arc::new(Route::of(groups));
        let targets = Targets::choose(&ctx.lab, groups, &ctx.fast)?;
        for (offset, (name, group)) in [("slow", Some(&targets.slow)), ("fast", Some(&targets.fast)), ("all", None)]
            .into_iter()
            .enumerate()
        {
            let tablets = group.map(|group| Arc::new(Tablets::of(&group.tablets)));
            let space = 100 + (index as u64) * 10 + offset as u64;
            let pool = keys_of(*kind, space, ctx.scale(4096, 256), tablets.as_ref());
            let rate = preload(ctx, *kind, pool.clone(), &route, 64).await?;
            println!("rate | preloaded {} {name}: {} rows at {rate:.0}/s", kind.short(), pool.len());
            aims.push(Aim {
                kind: *kind,
                name,
                tablets,
                leader: group.map_or("every".to_string(), |group| ctx.lab.members[group.leader].name.clone()),
                groups: if group.is_some() { 1 } else { groups.len() },
                pool,
                route: route.clone(),
                space: 1_000 + (index as u64) * 100 + (offset as u64) * 10,
            });
        }
    }
    // every cell, reversed in even rounds
    let mut plans = Vec::new();
    for aim in 0..aims.len() {
        for op in ["insert", "overwrite", "commit"] {
            for depth in [1, 32] {
                plans.push(Plan { aim, op, depth });
            }
        }
    }
    if ctx.reversed() {
        plans.reverse();
    }
    let clients = ctx.clients();
    for plan in &plans {
        let aim = &aims[plan.aim];
        let quarter = aim.pool.len() / 4;
        // each kind of cell has keys of its own: fresh ones, half the pool, or a quarter of it
        let (write, hands): (Write, Vec<Keys>) = match plan.op {
            "insert" => (
                Write::Insert,
                fresh(aim.space + plan.depth as u64, plan.depth, aim.tablets.as_ref()),
            ),
            "overwrite" => (
                Write::Insert,
                deal(&aim.pool[..2 * quarter], plan.depth)
                    .into_iter()
                    .map(|hand| Keys::list(hand, true))
                    .collect(),
            ),
            _ => {
                let keys = if plan.depth == 1 {
                    &aim.pool[2 * quarter..3 * quarter]
                } else {
                    &aim.pool[3 * quarter..]
                };
                (
                    Write::Commit(ReadFirst::No),
                    deal(keys, plan.depth).into_iter().map(|hand| Keys::list(hand, true)).collect(),
                )
            }
        };
        let workers = hands
            .into_iter()
            .enumerate()
            .map(|(worker, keys)| {
                Writer::new(aim.kind, write, keys, aim.route.clone(), clients.clone(), worker % clients.len())
            })
            .collect();
        let cell = format!("{} {} aim={} depth={}", aim.kind.short(), plan.op, aim.name, plan.depth);
        let mut record = ctx.record("rate", &cell, "-");
        record.label("leader", aim.leader.clone());
        let (mut record, done) = measured(ctx, record, ctx.plan(15), workers).await?;
        record.set("groups", aim.groups as f64).set("per_group", done.per_sec() / aim.groups as f64);
        ctx.emit(&[record])?;
    }
    // one row rewritten in a loop, at four sizes, in a group a Zen1 host leads
    let groups = &settled[Kind::Object(0).table()];
    let route = Arc::new(Route::of(groups));
    let targets = Targets::choose(&ctx.lab, groups, &ctx.fast)?;
    let tablets = Arc::new(Tablets::of(&targets.slow.tablets));
    let mut sizes = REWRITE_SIZES.to_vec();
    if ctx.reversed() {
        sizes.reverse();
    }
    for (index, inline) in sizes.into_iter().enumerate() {
        // a row of its own at this size, written once
        let kind = Kind::Object(inline);
        let key = keys_of(kind, 900 + index as u64, 1, Some(&tablets));
        preload(ctx, kind, key.clone(), &route, 1).await?;
        // then rewritten, each change conditional on the version before it
        let worker = Writer::new(
            kind,
            Write::Commit(ReadFirst::No),
            Keys::list(key, true),
            route.clone(),
            clients.clone(),
            0,
        );
        let cell = format!("rewrite inline={inline}");
        let mut record = ctx.record("rate", &cell, "-");
        record.label("leader", ctx.lab.members[targets.slow.leader].name.clone());
        let (mut record, _) = measured(ctx, record, ctx.plan(10), vec![worker]).await?;
        record.set("inline", inline as f64);
        ctx.emit(&[record])?;
    }
    Ok(())
}
