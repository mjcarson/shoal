//! X12: what a rebuild's and a deep scrub's cpu costs one core, with no device in the way
//!
//! Every figure is one pinned core working on stripes held in memory, sixteen chunks or more, so
//! the bytes come from memory as a read's would and not from cache. The checksum is CRC-64/NVME a
//! 64 KiB unit (X5); the fold is the XOR of a chunk's units S11's summary check needs, alone and
//! fused with the checksum while a unit is in cache; a decode rebuilds a data or a parity chunk of
//! a 2+1 and a 4+2 on a kept plan (X4's `rusty_erasure`); a pipeline is everything a rebuild's cpu
//! does for one chunk: its survivors verified, the chunk made, and its own checksums taken. P1
//! reads the fold against the checksum. The planted faults are checked here too, on every leg, so
//! a disk's leg proves the check finds them though its scrub may never reach them in a window.

use std::time::{Duration, Instant};

use glommio::io::DmaBuffer;

use super::stats::fmt;
use super::stripes::{self, make_stripe, planted, Codec, ChunkHeader, HeldChunk, Layout, Steps, CHUNK, UNIT, UNITS};
use super::table::Table;
use super::{on_core, Ctx, SideOut};

/// Stripes held for each layout: enough that every op's bytes come from memory
const STRIPES: usize = 4;

/// How long each op is timed
const TIMED: Duration = Duration::from_secs(1);

/// Stripes held for a layout, every chunk with its header
///
/// # Arguments
///
/// * `layout` - The layout
/// * `stripes` - The stripes, numbered as in a population of `of`
/// * `of` - Stripes in that population, which says where the faults were planted
fn hold(layout: Layout, stripes: &[usize], of: usize) -> Vec<Vec<HeldChunk>> {
    let coder = stripes::coder(layout).expect("a coded layout");
    stripes
        .iter()
        .map(|&stripe| {
            let (chunks, tables) = make_stripe(&coder, layout, of, stripe);
            chunks
                .into_iter()
                .enumerate()
                .map(|(position, bytes)| HeldChunk {
                    header: ChunkHeader { layout, stripe: stripe as u64, position: position as u8, crcs: tables[position] },
                    bytes,
                })
                .collect()
        })
        .collect()
}

/// Time an op over and over for the timed length, and say how many times it ran
///
/// # Arguments
///
/// * `timed` - How long
/// * `op` - The op, given its number
async fn time<F: AsyncFnMut(usize)>(timed: Duration, mut op: F) -> (usize, f64) {
    let began = Instant::now();
    let mut count = 0;
    while began.elapsed() < timed {
        op(count).await;
        count += 1;
    }
    (count, began.elapsed().as_secs_f64())
}

/// One op's figures
///
/// # Arguments
///
/// * `name` - The op
/// * `count` - Chunks it ran over
/// * `secs` - How long it took
/// * `bytes` - Bytes a chunk counts as
fn figure(name: &str, count: usize, secs: f64, bytes: u64) -> SideOut {
    let gib_s = count as f64 * bytes as f64 / secs / f64::from(1 << 30);
    let us = secs * 1e6 / count.max(1) as f64;
    SideOut::new(format!("op={name}"), "cpu", &[("gib_s", gib_s), ("us_per_chunk", us), ("count", count as f64)])
}

/// Run the codec measurement for one round
///
/// # Arguments
///
/// * `ctx` - The run
/// * `round` - The round
pub fn run(ctx: &Ctx, round: u32) {
    let timed = ctx.window(TIMED);
    let (outs, kernels) = on_core(ctx.core, ctx.sibling, move || async move {
        let mut outs = Vec::new();
        // the stripes held: 4+2 and 2+1, clean, from populations of full size
        let rs42 = hold(Layout::Rs42, &(0..STRIPES).collect::<Vec<_>>(), 192);
        let rs21 = hold(Layout::Rs21, &(0..STRIPES * 2).collect::<Vec<_>>(), 256);
        let chunks: Vec<&HeldChunk> = rs42.iter().flatten().collect();
        let (codec42, codec21, copy) = (Codec::new(Layout::Rs42), Codec::new(Layout::Rs21), Codec::new(Layout::Copy));
        let kernels = codec42.kernels();
        let mut summary = vec![0_u8; UNIT as usize];
        // the checksum of every unit of a chunk
        let (count, secs) = time(timed, async |nth| {
            let chunk = chunks[nth % chunks.len()];
            for unit in 0..UNITS {
                std::hint::black_box(stripes::crc(chunk.unit(unit)));
            }
        })
        .await;
        outs.push(figure("crc", count, secs, CHUNK));
        // the fold alone
        let (count, secs) = time(timed, async |nth| {
            let chunk = chunks[nth % chunks.len()];
            for unit in 0..UNITS {
                stripes::fold_into(&mut summary, chunk.unit(unit));
            }
            std::hint::black_box(&summary);
        })
        .await;
        outs.push(figure("fold", count, secs, CHUNK));
        // the two fused, a unit's fold while it is in cache from its checksum
        let (count, secs) = time(timed, async |nth| {
            let chunk = chunks[nth % chunks.len()];
            for unit in 0..UNITS {
                std::hint::black_box(stripes::crc(chunk.unit(unit)));
                stripes::fold_into(&mut summary, chunk.unit(unit));
            }
            std::hint::black_box(&summary);
        })
        .await;
        outs.push(figure("crc+fold", count, secs, CHUNK));
        // the supplement: a summary one block long, its fold alone and in the checksum's pass, so
        // the summary stays in the first cache while the unit streams through
        let mut block = vec![0_u8; stripes::BLOCK];
        let (count, secs) = time(timed, async |nth| {
            let chunk = chunks[nth % chunks.len()];
            for unit in 0..UNITS {
                stripes::fold_block(&mut block, chunk.unit(unit));
            }
            std::hint::black_box(&block);
        })
        .await;
        outs.push(figure("fold-4k", count, secs, CHUNK));
        let (count, secs) = time(timed, async |nth| {
            let chunk = chunks[nth % chunks.len()];
            for unit in 0..UNITS {
                std::hint::black_box(stripes::crc(chunk.unit(unit)));
                stripes::fold_block(&mut block, chunk.unit(unit));
            }
            std::hint::black_box(&block);
        })
        .await;
        outs.push(figure("crc+fold-4k", count, secs, CHUNK));
        // a chunk copied, which is a copy's rebuild in memory
        let mut out: Vec<DmaBuffer> = (0..4).map(|_| glommio::allocate_dma_buffer(1 << 20)).collect();
        let mut steps = Steps::default();
        let (count, secs) = time(timed, async |nth| {
            let chunk = chunks[nth % chunks.len()];
            copy.rebuild(&[chunk], 0, &mut out, 1 << 20, &mut steps).await;
        })
        .await;
        outs.push(figure("copy", count, secs, CHUNK));
        // a decode of a data chunk and of a parity chunk, for each layout
        for (name, codec, held, lost) in [
            ("decode-21-data", &codec21, &rs21, 0),
            ("decode-21-parity", &codec21, &rs21, 2),
            ("decode-42-data", &codec42, &rs42, 0),
            ("decode-42-parity", &codec42, &rs42, 4),
        ] {
            let (count, secs) = time(timed, async |nth| {
                let stripe = &held[nth % held.len()];
                let sources: Vec<&HeldChunk> = codec.survivors(lost).iter().map(|&position| &stripe[position]).collect();
                codec.rebuild(&sources, lost, &mut out, 1 << 20, &mut steps).await;
            })
            .await;
            outs.push(figure(name, count, secs, CHUNK));
        }
        // a rebuild's whole cpu for one chunk: survivors verified, the chunk made, its checksums
        for (name, codec, held, lost) in [
            ("pipeline-copy", &copy, &rs42, 0),
            ("pipeline-21", &codec21, &rs21, 0),
            ("pipeline-42", &codec42, &rs42, 0),
        ] {
            let (count, secs) = time(timed, async |nth| {
                let stripe = &held[nth % held.len()];
                let survivors = codec.survivors(lost);
                let sources: Vec<&HeldChunk> = survivors.iter().map(|&position| &stripe[position]).collect();
                for (source, &position) in sources.iter().zip(&survivors) {
                    stripes::verify(*source, &stripe[position].header.crcs, None, &mut steps).await;
                }
                codec.rebuild(&sources, lost, &mut out, 1 << 20, &mut steps).await;
                for unit in 0..UNITS {
                    let at = unit as u64 * UNIT;
                    std::hint::black_box(stripes::crc(&out[(at >> 20) as usize].as_bytes()[(at & ((1 << 20) - 1)) as usize..][..UNIT as usize]));
                }
            })
            .await;
            outs.push(figure(name, count, secs, CHUNK));
        }
        // the summary check of one 4+2 stripe: four summaries encoded and two compared
        let summaries: Vec<Vec<u8>> = rs42[0]
            .iter()
            .map(|chunk| {
                let mut summary = vec![0_u8; UNIT as usize];
                for unit in 0..UNITS {
                    stripes::fold_into(&mut summary, chunk.unit(unit));
                }
                summary
            })
            .collect();
        let mut scratch = vec![vec![0_u8; UNIT as usize]; 2];
        let (count, secs) = time(timed, async |_| {
            assert!(codec42.summaries_match(&summaries, &mut scratch, &mut steps), "a clean stripe's summaries match");
        })
        .await;
        outs.push(figure("summary-42", count, secs, CHUNK * 6));
        // and one 4+2 stripe's block summaries encoded and compared
        let blocks: Vec<Vec<u8>> = rs42[0]
            .iter()
            .map(|chunk| {
                let mut summary = vec![0_u8; stripes::BLOCK];
                for unit in 0..UNITS {
                    stripes::fold_block(&mut summary, chunk.unit(unit));
                }
                summary
            })
            .collect();
        let mut block_scratch = vec![vec![0_u8; stripes::BLOCK]; 2];
        let (count, secs) = time(timed, async |_| {
            assert!(codec42.summaries_match(&blocks, &mut block_scratch, &mut steps), "a clean stripe's block summaries match");
        })
        .await;
        outs.push(figure("summary-42-4k", count, secs, CHUNK * 6));
        // the planted faults, held and checked as a deep scrub checks them
        let of = 192;
        let checked: Vec<usize> = (0..of).filter(|&stripe| planted(Layout::Rs42, of, stripe).is_some()).chain([0, 1]).collect();
        let held = hold(Layout::Rs42, &checked, of);
        let (mut found, mut undetected, mut false_alarms) = (0, 0, 0);
        for (stripe, chunks) in checked.iter().zip(&held) {
            let mut failed = 0;
            let mut summaries = vec![vec![0_u8; UNIT as usize]; 6];
            for (position, chunk) in chunks.iter().enumerate() {
                failed += stripes::verify(chunk, &chunk.header.crcs, Some(&mut summaries[position]), &mut steps).await;
            }
            let matched = failed > 0 || codec42.summaries_match(&summaries, &mut scratch, &mut steps);
            match planted(Layout::Rs42, of, *stripe) {
                Some(stripes::Fault::Data) if failed > 0 => found += 1,
                Some(stripes::Fault::Parity) if failed == 0 && !matched => found += 1,
                Some(_) => undetected += 1,
                None if failed > 0 || !matched => false_alarms += 1,
                None => {}
            }
        }
        outs.push(SideOut::new(
            "op=detect",
            "cpu",
            &[("planted_found", f64::from(found)), ("undetected", f64::from(undetected)), ("false_alarms", f64::from(false_alarms))],
        ));
        (outs, kernels)
    });
    let mut table = Table::new(&["op", "GiB/s", "µs a chunk", "planted found", "undetected", "false alarms"]);
    let mut records = Vec::new();
    for out in outs {
        let detect = out.cell == "op=detect";
        table.row(vec![
            out.cell.trim_start_matches("op=").to_string(),
            if detect { "-".into() } else { fmt(out.get("gib_s")) },
            if detect { "-".into() } else { fmt(out.get("us_per_chunk")) },
            if detect { fmt(out.get("planted_found")) } else { "-".into() },
            if detect { fmt(out.get("undetected")) } else { "-".into() },
            if detect { fmt(out.get("false_alarms")) } else { "-".into() },
        ]);
        records.push(ctx.record("codec", round, out));
    }
    print!(
        "{}",
        table.render(
            &format!("X12 · A rebuild's and a scrub's cpu on one core, round {round} (4 MiB chunks of 64 KiB units, out of cache; {kernels}; a summary is a 4+2 stripe's six)"),
            &ctx.label()
        )
    );
    ctx.emit(&records);
}
