//! Leg `size`: an object held inline in its row, from 1 KiB to 1 MiB
//!
//! An object at or under its pool's inline threshold is its `ObjectMeta` entry and nothing else
//! ([S3](../../../docs/src/object-storage/objects.md#small-objects-stay-inline)). Each size is
//! eleven octaves from 1 KiB, ascending in odd rounds and descending in even, and each is driven
//! three ways at depth thirty-two: puts of new objects, gets of objects written before the cell,
//! and an even mixture of the two, which is the shape the benchmark host's row size capture took
//! (`f22-row-size`, persistent unsorted, half reads, depth 32). Between sizes the compactors are
//! let catch up, so one size's merges are not charged to the next.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cluster::Lab;
use crate::measure::rate::{QUIET, SETTLE_LIMIT};
use crate::measure::{deal, keys_of, measured, preload, Ctx, Keys, Kind, Mixer, Route, Write, Writer};
use crate::keys::Cursor;
use crate::stats::Rng;

/// The inline sizes swept, by octave from 1 KiB to 1 MiB
pub const SIZES: [usize; 11] = [
    1 << 10,
    2 << 10,
    4 << 10,
    8 << 10,
    16 << 10,
    32 << 10,
    64 << 10,
    128 << 10,
    256 << 10,
    512 << 10,
    1 << 20,
];

/// The mixtures each size is driven with, by the share of operations that read
pub const MIXES: [(&str, u64); 3] = [("r0", 0), ("r50", 50), ("r100", 100)];

/// How many workers each cell drives
const DEPTH: usize = 32;

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
    // the object table's groups, settled, and who leads each tablet
    ctx.lab.activate().await?;
    let table = Kind::Object(0).table();
    let settled = ctx.lab.settle(&[table], QUIET, SETTLE_LIMIT).await?;
    let route = Arc::new(Route::of(&settled[table]));
    let clients = ctx.clients();
    // ascending in odd rounds, descending in even
    let mut sizes: Vec<(usize, usize)> = SIZES.iter().copied().enumerate().collect();
    let mut mixes = MIXES.to_vec();
    if ctx.reversed() {
        sizes.reverse();
        mixes.reverse();
    }
    for (index, size) in sizes {
        let kind = Kind::Object(size);
        // the objects every read is of, written first
        let readable = keys_of(kind, 2_000 + index as u64, ctx.scale(512, 32), None);
        let rate = preload(ctx, kind, readable.clone(), &route, DEPTH).await?;
        println!("size | preloaded {} objects of {size} bytes at {rate:.0}/s", readable.len());
        let hands = deal(&readable, DEPTH);
        for (offset, (mix, read_percent)) in mixes.iter().enumerate() {
            // every worker a mixture of fresh puts and gets of the preloaded objects
            let space = 3_000 + (index as u64) * 10 + offset as u64;
            let workers = hands
                .iter()
                .enumerate()
                .map(|(worker, hand)| {
                    let insert = Writer::build(
                        kind,
                        Write::Insert,
                        Keys::Fresh(Cursor::new(space, worker, DEPTH, None)),
                        route.clone(),
                        clients.clone(),
                        worker % clients.len(),
                    );
                    let read = Writer::build(
                        kind,
                        Write::Get,
                        Keys::list(hand.clone(), true),
                        route.clone(),
                        clients.clone(),
                        worker % clients.len(),
                    );
                    Box::new(Mixer {
                        insert,
                        read,
                        rng: Rng::new(space ^ ((worker as u64) << 32)),
                        read_percent: *read_percent,
                    }) as Box<dyn crate::drive::Work>
                })
                .collect();
            let cell = format!("inline={size} mix={mix}");
            let record = ctx.record("size", &cell, "-");
            let (mut record, done) = measured(ctx, record, ctx.plan(15), workers).await?;
            record
                .set("inline", size as f64)
                .set("read_percent", *read_percent as f64)
                .set("payload_mib_per_sec", done.per_sec() * size as f64 / f64::from(1 << 20));
            ctx.emit(&[record])?;
        }
        // the compactors catch up before the next size
        drain(&ctx.lab, Duration::from_secs(ctx.scale(300, 30))).await?;
    }
    Ok(())
}

/// Wait until no member's compactors hold a backlog, or a limit passes
///
/// # Arguments
///
/// * `lab` - The cluster
/// * `limit` - How long to wait at most
///
/// # Errors
///
/// When a member cannot be read.
async fn drain(lab: &Lab, limit: Duration) -> color_eyre::Result<()> {
    let started = Instant::now();
    loop {
        // a backlog of sealed segments on any shard is still being merged
        let reports = lab.replication().await?;
        if Lab::drained(&reports) {
            return Ok(());
        }
        if started.elapsed() > limit {
            println!("size | the compactors still held a backlog after {limit:?}; going on");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
