//! X6's device store figures retaken from the store M14 built
//!
//! X6 measured S6's device store with a harness of its own (`shoal-spike device`). F85 built the
//! store, so this times the product's code on one pinned executor the way a node's object executor
//! drives it: `Slice::open`, `stage_units`, `stage_whole`, `apply`, nothing of the harness's.
//! Three sections ([F85](../../docs/src/features/devices-and-pools.md#performance)):
//!
//! - `small`: one unit staged over a chunk and applied, at each apply sync, rested (one write every
//!   100 ms) and loaded (back to back), at each depth. X6 found 2 ms and 6 ms on the 970 EVO and
//!   81 µs on the Optane.
//! - `whole`: 1 MiB whole chunks staged from the pool written ahead and applied, back to back, beside
//!   the same bytes written at successive offsets of one file and synced each, which X6 put at 0.71.
//! - `listing`: a placement group of chunk files made in the product's layout, the caches dropped
//!   when the run may, and the group listed. X6 listed a million on titan's XFS in 37.5 s.
//! - `previous`: what keeping a chunk's previous state costs an apply
//!   ([F86](../../docs/src/features/replicated-pools.md)): one unit written in place over a chunk,
//!   and a whole 1 MiB chunk over one, each applied back to back with the state kept and with it
//!   not, the store's shared flush.
//!
//! It refuses tmpfs and btrfs as the store does, and writes a record a run with `--out`.

use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use glommio::{LocalExecutorBuilder, Placement};
use shoal::server::devices::probe::{self, Facts};
use shoal::server::devices::DeviceState;
use shoal::server::objects::store::{ChunkId, Slice, SliceOptions, StageUnits, StageWhole};
use shoal::shared::devices::{ApplySync, DeviceId, DeviceSpec, SliceId};
use shoal::shared::objects::Label;
use shoal::shared::poolmap::PlacementGroup;
use shoal::shared::rendezvous::mix64;

/// The sections, in the order they run
const SECTIONS: [&str; 4] = ["small", "whole", "listing", "previous"];
/// The whole chunk's bytes
const WHOLE: u64 = 1 << 20;
/// The whole chunk's unit
const WHOLE_UNIT: u32 = 64 << 10;
/// The chunks a small write is spread over
const SMALL_CHUNKS: u64 = 16;
/// The pause between two rested writes
const REST: Duration = Duration::from_millis(100);

/// What a run was asked for
#[derive(Debug, Clone)]
struct Args {
    /// The scratch directory, on the device measured
    dir: PathBuf,
    /// The cpu the executor is pinned to
    core: usize,
    /// The sections to run
    only: Vec<String>,
    /// The small write's unit
    unit: u32,
    /// The depths the small write runs at
    depths: Vec<usize>,
    /// Writes a side of the small section
    small_ops: usize,
    /// Chunks a side of the whole section
    whole_ops: usize,
    /// Chunks the listing makes
    listing_chunks: u64,
    /// Stripes an object of the listing has
    stripes_per_object: u64,
    /// A label for the record
    leg: String,
    /// Where the record goes
    out: Option<PathBuf>,
}

/// The value after a flag
///
/// # Arguments
///
/// * `args` - The arguments
/// * `flag` - The flag
fn value_of<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.iter()
        .position(|arg| arg == flag)
        .and_then(|at| args.get(at + 1))
        .map(String::as_str)
}

impl Args {
    /// Read the arguments
    ///
    /// # Arguments
    ///
    /// * `args` - What followed `store`
    fn parse(args: &[String]) -> Self {
        let quick = args.iter().any(|arg| arg == "--quick" || arg == "quick");
        let number = |flag: &str, default: u64| -> u64 {
            value_of(args, flag).map_or(default, |value| value.parse().expect("a number"))
        };
        Args {
            dir: PathBuf::from(value_of(args, "--dir").expect("--dir names the scratch directory")),
            core: usize::try_from(number("--core", 2)).expect("a cpu"),
            only: value_of(args, "--only").map_or_else(
                || SECTIONS.iter().map(ToString::to_string).collect(),
                |only| only.split(',').map(str::to_string).collect(),
            ),
            unit: u32::try_from(number("--unit", 4096)).expect("a unit"),
            depths: value_of(args, "--depths").map_or_else(
                || vec![1, 8],
                |depths| depths.split(',').map(|depth| depth.parse().expect("a depth")).collect(),
            ),
            small_ops: usize::try_from(number("--small-ops", if quick { 20 } else { 400 })).expect("a count"),
            whole_ops: usize::try_from(number("--whole-ops", if quick { 32 } else { 1024 })).expect("a count"),
            listing_chunks: number("--listing-chunks", if quick { 10_000 } else { 1_000_000 }),
            stripes_per_object: number("--stripes-per-object", 16),
            leg: value_of(args, "--leg").unwrap_or("store").to_string(),
            out: value_of(args, "--out").map(PathBuf::from),
        }
    }
}

/// Run `shoal-spike store`
///
/// # Arguments
///
/// * `args` - What followed `store`
pub fn main(args: &[String]) {
    if args.is_empty() || args.iter().any(|arg| arg == "--help" || arg == "help") {
        println!(
            "shoal-spike store <all|quick> --dir <scratch> [--core N] [--only small,whole,listing,previous] \
             [--unit 4096] [--depths 1,8] [--small-ops N] [--whole-ops N] [--listing-chunks N] \
             [--stripes-per-object 16] [--leg <name>] [--out f.json]"
        );
        return;
    }
    let args = Args::parse(args);
    std::fs::create_dir_all(&args.dir).expect("the scratch directory is made");
    // the filesystem the store would refuse is refused here too
    let facts = probe::probe(&args.dir).expect("the scratch directory is probed");
    if let Some(refusal) = facts.refusal() {
        panic!("{}: {refusal}", args.dir.display());
    }
    println!(
        "# shoal-spike store on {} · cpu {} · governor {} · {} on {} ({}, write cache {})\n",
        super::hostname(),
        args.core,
        super::governor(),
        args.dir.display(),
        facts.filesystem,
        if facts.rotational { "rotational" } else { "solid state" },
        facts.write_cache.as_deref().unwrap_or("unknown")
    );
    let core = args.core;
    let run = args.clone();
    let records = LocalExecutorBuilder::new(Placement::Fixed(core))
        .name("shoal-spike-store")
        .spawn(move || async move { sections(&run, facts).await })
        .expect("the executor starts")
        .join()
        .expect("the executor ran");
    // the record, one line a figure
    if let Some(out) = &args.out {
        let json = shoal::serde_json::to_vec_pretty(&records).expect("the record encodes");
        std::fs::write(out, json).expect("the record is written");
    }
}

/// Run every section asked for, on the executor
///
/// # Arguments
///
/// * `args` - What was asked for
/// * `facts` - What the scratch directory is on
async fn sections(args: &Args, facts: Facts) -> Vec<shoal::serde_json::Value> {
    let mut records = Vec::new();
    let base = shoal::serde_json::json!({
        "host": super::hostname(),
        "governor": super::governor(),
        "core": args.core,
        "leg": args.leg,
        "filesystem": facts.filesystem,
        "rotational": facts.rotational,
    });
    for section in &args.only {
        let found = match section.as_str() {
            "small" => small(args, &facts).await,
            "whole" => whole(args, &facts).await,
            "listing" => listing(args).await,
            "previous" => previous(args, &facts).await,
            other => panic!("{other} is not a section: {}", SECTIONS.join(", ")),
        };
        for mut record in found {
            if let (Some(record), Some(base)) = (record.as_object_mut(), base.as_object()) {
                record.extend(base.clone());
            }
            records.push(record);
        }
    }
    records
}

/// A fresh slice of a device under the scratch directory
///
/// # Arguments
///
/// * `dir` - The scratch directory
/// * `name` - The device's directory under it
/// * `sync` - How its applies are made durable
/// * `chunk` - The chunk length its pool keeps files for
/// * `facts` - What it is on
async fn fresh_slice(dir: &Path, name: &str, sync: ApplySync, chunk: u64, facts: &Facts) -> Rc<Slice> {
    fresh_slice_keeping(dir, name, sync, chunk, facts, true).await
}

/// A fresh slice of a device under the scratch directory, keeping a chunk's previous state or not
///
/// # Arguments
///
/// * `dir` - The scratch directory
/// * `name` - The device's directory under it
/// * `sync` - How its applies are made durable
/// * `chunk` - The chunk length its pool keeps files for
/// * `facts` - What it is on
/// * `keep` - Whether it keeps a chunk's previous state
async fn fresh_slice_keeping(
    dir: &Path,
    name: &str,
    sync: ApplySync,
    chunk: u64,
    facts: &Facts,
    keep: bool,
) -> Rc<Slice> {
    // the device's directory, emptied
    let root = dir.join(name);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("slice-0")).expect("the slice's directory");
    let device = Arc::new(DeviceState::new(
        DeviceId::mint(),
        DeviceSpec::new(&root, "ssd").apply_sync(sync),
        facts.clone(),
        0,
        sync,
        vec![(SliceId::mint(), root.join("slice-0"))],
        Vec::new(),
    ));
    let spec = device.spec.clone();
    Slice::open(
        device.clone(),
        device.slices[0].clone(),
        SliceOptions {
            segment_bytes: spec.segment_bytes(),
            free_files: spec.free_files_kept(),
            chunk_lengths: vec![chunk],
            budget: None,
            keep_previous: keep,
        },
        None,
    )
    .await
    .expect("the slice opens")
}

/// The chunk a write lands in
///
/// # Arguments
///
/// * `object` - Its object
fn chunk_of(object: u64) -> ChunkId {
    ChunkId {
        consumer: 7,
        pg: PlacementGroup { tablet: 1, sub: 0 },
        object: u128::from(object),
        stripe: 0,
        position: 0,
    }
}

/// Seeded bytes, so no two writes are alike
///
/// # Arguments
///
/// * `seed` - The seed
/// * `len` - How many
fn bytes(seed: u64, len: usize) -> Vec<u8> {
    (0..len.div_ceil(8) as u64)
        .flat_map(|index| mix64(index ^ seed.rotate_left(32)).to_le_bytes())
        .take(len)
        .collect()
}

/// Points of a set of samples, in microseconds
///
/// # Arguments
///
/// * `samples` - The samples
fn points(samples: &mut [Duration]) -> shoal::serde_json::Value {
    samples.sort_unstable();
    let at = |quantile: f64| -> f64 {
        if samples.is_empty() {
            return 0.0;
        }
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
        let index = ((samples.len() as f64 * quantile) as usize).min(samples.len() - 1);
        samples[index].as_secs_f64() * 1e6
    };
    shoal::serde_json::json!({ "n": samples.len(), "p50_us": at(0.5), "p99_us": at(0.99), "max_us": at(1.0) })
}

/// One unit staged over a chunk and applied, at each sync, regime and depth
///
/// # Arguments
///
/// * `args` - What was asked for
/// * `facts` - What the scratch directory is on
async fn small(args: &Args, facts: &Facts) -> Vec<shoal::serde_json::Value> {
    let mut records = Vec::new();
    // a chunk of so many units as fit a header, at most 64 of them
    let units = 64u32;
    let chunk = u64::from(args.unit) * u64::from(units);
    println!("## A {} byte unit staged and applied, a chunk of {units}\n", args.unit);
    println!("| sync | regime | depth | writes | stage p50 µs | stage p99 µs | apply p50 µs | apply p99 µs | write p50 µs | write p99 µs | flushes |");
    println!("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    for sync in [ApplySync::Each, ApplySync::Shared] {
        for rested in [true, false] {
            for &depth in &args.depths {
                // rested is one writer by definition
                if rested && depth > 1 {
                    continue;
                }
                let slice = fresh_slice(&args.dir, "small", sync, chunk, facts).await;
                // the chunks every write lands over, each applied once whole
                for object in 0..SMALL_CHUNKS {
                    let id = chunk_of(object);
                    slice
                        .stage_whole(StageWhole {
                            id,
                            label: Label { sequence: 1, tag: 0 },
                            chunk_bytes: chunk,
                            unit_bytes: args.unit,
                            length: chunk,
                            bytes: bytes(object, chunk as usize),
                        })
                        .await
                        .expect("the base is staged");
                    slice.apply(id, Label { sequence: 1, tag: 0 }).await.expect("the base is applied");
                }
                let flushes_before = slice.flushes().0;
                // each writer its own chunks, so two never wait on one chunk's lock
                let per = args.small_ops / depth;
                let writers = (0..depth).map(|writer| {
                    let slice = slice.clone();
                    let unit = args.unit;
                    async move {
                        let mut stage = Vec::with_capacity(per);
                        let mut apply = Vec::with_capacity(per);
                        let mut write = Vec::with_capacity(per);
                        for op in 0..per {
                            let object = (writer + op * depth) as u64 % SMALL_CHUNKS;
                            let id = chunk_of(object);
                            // the chunk's label now, and the next one
                            let current = slice.chunk_label(&id).await.expect("a label").expect("a chunk");
                            let next = Label { sequence: current.sequence + 1, tag: 0 };
                            let began = Instant::now();
                            slice
                                .stage_units(StageUnits {
                                    id,
                                    makes: next,
                                    expects: current,
                                    also: Vec::new(),
                                    length: chunk,
                                    units: vec![(((op * 7) as u32) % units, bytes(op as u64 ^ 0xabc, unit as usize))],
                                })
                                .await
                                .expect("staged");
                            let staged = Instant::now();
                            slice.apply(id, next).await.expect("applied");
                            let applied = Instant::now();
                            stage.push(staged - began);
                            apply.push(applied - staged);
                            write.push(applied - began);
                            if rested {
                                glommio::timer::sleep(REST).await;
                            }
                        }
                        (stage, apply, write)
                    }
                });
                let results = futures::future::join_all(writers).await;
                let flushes = slice.flushes().0 - flushes_before;
                slice.close().await;
                let (mut stage, mut apply, mut write) = (Vec::new(), Vec::new(), Vec::new());
                for (s, a, w) in results {
                    stage.extend(s);
                    apply.extend(a);
                    write.extend(w);
                }
                let (stage, apply, write) = (points(&mut stage), points(&mut apply), points(&mut write));
                let sync_name = format!("{sync:?}").to_lowercase();
                let regime = if rested { "rested" } else { "loaded" };
                println!(
                    "| {sync_name} | {regime} | {depth} | {} | {:.0} | {:.0} | {:.0} | {:.0} | {:.0} | {:.0} | {flushes} |",
                    write["n"], stage["p50_us"].as_f64().unwrap_or(0.0), stage["p99_us"].as_f64().unwrap_or(0.0),
                    apply["p50_us"].as_f64().unwrap_or(0.0), apply["p99_us"].as_f64().unwrap_or(0.0),
                    write["p50_us"].as_f64().unwrap_or(0.0), write["p99_us"].as_f64().unwrap_or(0.0)
                );
                records.push(shoal::serde_json::json!({
                    "section": "small", "sync": sync_name, "regime": regime, "depth": depth, "unit": args.unit,
                    "stage": stage, "apply": apply, "write": write, "flushes": flushes,
                }));
            }
        }
    }
    println!();
    records
}

/// Whole 1 MiB chunks staged from the pool and applied, beside one file written and synced
///
/// # Arguments
///
/// * `args` - What was asked for
/// * `facts` - What the scratch directory is on
async fn whole(args: &Args, facts: &Facts) -> Vec<shoal::serde_json::Value> {
    let mut records = Vec::new();
    println!("## 1 MiB whole chunks, back to back\n");
    println!("| side | chunks | MiB/s | write p50 µs | write p99 µs | of the file's rate |");
    println!("| --- | --- | --- | --- | --- | --- |");
    // the file first: the same bytes at successive offsets, synced each
    let file_rate = {
        let path = args.dir.join("whole-file");
        let _ = std::fs::remove_file(&path);
        let file = glommio::io::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .dma_open(&path)
            .await
            .expect("the file opens");
        let mut samples = Vec::with_capacity(args.whole_ops);
        let started = Instant::now();
        for op in 0..args.whole_ops {
            let mut buffer = file.alloc_dma_buffer(WHOLE as usize);
            buffer.as_bytes_mut().copy_from_slice(&bytes(op as u64, WHOLE as usize));
            let began = Instant::now();
            file.write_at(buffer, op as u64 * WHOLE).await.expect("written");
            file.fdatasync().await.expect("synced");
            samples.push(began.elapsed());
        }
        let secs = started.elapsed().as_secs_f64();
        file.close().await.expect("closed");
        let _ = std::fs::remove_file(&path);
        #[allow(clippy::cast_precision_loss)]
        let rate = args.whole_ops as f64 / secs;
        let point = points(&mut samples);
        println!(
            "| one file, synced each | {} | {rate:.0} | {:.0} | {:.0} | 1.00 |",
            args.whole_ops,
            point["p50_us"].as_f64().unwrap_or(0.0),
            point["p99_us"].as_f64().unwrap_or(0.0)
        );
        records.push(shoal::serde_json::json!({ "section": "whole", "side": "file", "chunks": args.whole_ops, "mib_per_sec": rate, "write": point }));
        rate
    };
    // then the store, each sync
    for sync in [ApplySync::Each, ApplySync::Shared] {
        let slice = fresh_slice(&args.dir, "whole", sync, WHOLE, facts).await;
        // the pool written ahead before the clock starts, as a node's is
        glommio::timer::sleep(Duration::from_millis(500)).await;
        let mut samples = Vec::with_capacity(args.whole_ops);
        let started = Instant::now();
        for op in 0..args.whole_ops {
            let id = chunk_of(op as u64);
            let label = Label { sequence: 1, tag: 0 };
            let data = bytes(op as u64, WHOLE as usize);
            let began = Instant::now();
            slice
                .stage_whole(StageWhole {
                    id,
                    label,
                    chunk_bytes: WHOLE,
                    unit_bytes: WHOLE_UNIT,
                    length: WHOLE,
                    bytes: data,
                })
                .await
                .expect("staged");
            slice.apply(id, label).await.expect("applied");
            samples.push(began.elapsed());
        }
        let secs = started.elapsed().as_secs_f64();
        slice.close().await;
        #[allow(clippy::cast_precision_loss)]
        let rate = args.whole_ops as f64 / secs;
        let point = points(&mut samples);
        let sync_name = format!("{sync:?}").to_lowercase();
        println!(
            "| the store, {sync_name} | {} | {rate:.0} | {:.0} | {:.0} | {:.2} |",
            args.whole_ops,
            point["p50_us"].as_f64().unwrap_or(0.0),
            point["p99_us"].as_f64().unwrap_or(0.0),
            rate / file_rate
        );
        records.push(shoal::serde_json::json!({ "section": "whole", "side": sync_name, "chunks": args.whole_ops, "mib_per_sec": rate, "write": point, "of_file": rate / file_rate }));
    }
    let _ = std::fs::remove_dir_all(args.dir.join("whole"));
    println!();
    records
}

/// What keeping a chunk's previous state costs an apply: one unit in place, and a whole chunk over
/// one, back to back under the shared flush, with the state kept and with it not
///
/// # Arguments
///
/// * `args` - What was asked for
/// * `facts` - What the scratch directory is on
async fn previous(args: &Args, facts: &Facts) -> Vec<shoal::serde_json::Value> {
    let mut records = Vec::new();
    let units = 64u32;
    let chunk = u64::from(args.unit) * u64::from(units);
    println!("## A chunk's previous state kept and not: applies back to back, the shared flush
");
    println!("| write | kept | writes | apply p50 µs | apply p99 µs |");
    println!("| --- | --- | --- | --- | --- |");
    for (write, chunk_bytes, unit_bytes) in [("unit", chunk, args.unit), ("whole", WHOLE, WHOLE_UNIT)] {
        for keep in [false, true] {
            let slice = fresh_slice_keeping(&args.dir, "previous", ApplySync::Shared, chunk_bytes, facts, keep).await;
            // every chunk applied once whole, so each write after lands over one
            for object in 0..SMALL_CHUNKS {
                let id = chunk_of(object);
                let label = Label { sequence: 1, tag: 0 };
                slice
                    .stage_whole(StageWhole {
                        id,
                        label,
                        chunk_bytes,
                        unit_bytes,
                        length: chunk_bytes,
                        bytes: bytes(object, chunk_bytes as usize),
                    })
                    .await
                    .expect("the base is staged");
                slice.apply(id, label).await.expect("the base is applied");
            }
            let ops = if write == "unit" { args.small_ops } else { args.whole_ops };
            let mut apply = Vec::with_capacity(ops);
            for op in 0..ops {
                let id = chunk_of(op as u64 % SMALL_CHUNKS);
                let current = slice.chunk_label(&id).await.expect("a label").expect("a chunk");
                let next = Label { sequence: current.sequence + 1, tag: 0 };
                if write == "unit" {
                    slice
                        .stage_units(StageUnits {
                            id,
                            makes: next,
                            expects: current,
                            also: Vec::new(),
                            length: chunk_bytes,
                            units: vec![(((op * 7) as u32) % units, bytes(op as u64 ^ 0xabc, unit_bytes as usize))],
                        })
                        .await
                        .expect("staged");
                } else {
                    slice
                        .stage_whole(StageWhole {
                            id,
                            label: next,
                            chunk_bytes,
                            unit_bytes,
                            length: chunk_bytes,
                            bytes: bytes(op as u64 ^ 0xdef, chunk_bytes as usize),
                        })
                        .await
                        .expect("staged");
                }
                let began = Instant::now();
                slice.apply(id, next).await.expect("applied");
                apply.push(began.elapsed());
            }
            slice.close().await;
            let apply = points(&mut apply);
            println!(
                "| {write} | {keep} | {} | {:.0} | {:.0} |",
                apply["n"],
                apply["p50_us"].as_f64().unwrap_or(0.0),
                apply["p99_us"].as_f64().unwrap_or(0.0)
            );
            records.push(shoal::serde_json::json!({
                "section": "previous", "write": write, "kept": keep, "unit": unit_bytes, "apply": apply,
            }));
        }
    }
    let _ = std::fs::remove_dir_all(args.dir.join("previous"));
    println!();
    records
}

/// Drop the page, dentry and inode caches, if this run may
fn drop_caches() -> bool {
    // the dirty pages first, so the drop leaves nothing to write back
    unsafe { libc::sync() };
    std::fs::write("/proc/sys/vm/drop_caches", "3").is_ok()
}

/// A placement group's chunk files made in the product's layout, and listed cold
///
/// # Arguments
///
/// * `args` - What was asked for
async fn listing(args: &Args) -> Vec<shoal::serde_json::Value> {
    let root = args.dir.join("listing").join("chunks");
    let _ = std::fs::remove_dir_all(args.dir.join("listing"));
    let chunks = args.listing_chunks;
    let stripes = args.stripes_per_object.max(1);
    // made on a thread of the executor's blocking pool would hold it; a plain thread does
    let made = {
        let root = root.clone();
        std::thread::spawn(move || {
            let started = Instant::now();
            for index in 0..chunks {
                let id = ChunkId {
                    consumer: 7,
                    pg: PlacementGroup { tablet: 1, sub: 0 },
                    object: u128::from(index / stripes),
                    stripe: index % stripes,
                    position: 0,
                };
                let path = root.join(id.relative());
                if index % stripes == 0 {
                    std::fs::create_dir_all(path.parent().expect("an object's directory")).expect("made");
                }
                std::fs::File::create(&path).expect("a chunk file");
            }
            started.elapsed()
        })
        .join()
        .expect("the files are made")
    };
    // cold, if the caches can be dropped
    let cold = drop_caches();
    let group = root.join(format!("{:016x}", 7u64)).join(format!("{:03x}-{:04x}", 1, 0));
    let (listed, took) = std::thread::spawn(move || {
        let started = Instant::now();
        let mut listed = 0u64;
        for object in std::fs::read_dir(&group).expect("the group's directory").flatten() {
            for _chunk in std::fs::read_dir(object.path()).expect("an object's directory").flatten() {
                listed += 1;
            }
        }
        (listed, started.elapsed())
    })
    .join()
    .expect("the group is listed");
    println!("## A placement group of {chunks} chunks, {stripes} a stripe directory\n");
    println!("| chunks | objects | made s | listed s | caches dropped |");
    println!("| --- | --- | --- | --- | --- |");
    println!(
        "| {listed} | {} | {:.1} | {:.2} | {} |\n",
        chunks.div_ceil(stripes),
        made.as_secs_f64(),
        took.as_secs_f64(),
        if cold { "yes" } else { "no: not root, so the listing is warm" }
    );
    // the files go again, as a node's chunks would on a wipe
    let _ = std::fs::remove_dir_all(args.dir.join("listing"));
    vec![shoal::serde_json::json!({
        "section": "listing", "chunks": listed, "objects": chunks.div_ceil(stripes),
        "made_secs": made.as_secs_f64(), "listed_secs": took.as_secs_f64(), "cold": cold,
    })]
}
