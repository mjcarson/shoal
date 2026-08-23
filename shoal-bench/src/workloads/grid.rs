//! `macro/grid/...` and `macro/skew/...` - how Shoal behaves, rather than why
//!
//! # What this is for, and what it is not for
//!
//! Every other workload in this crate isolates one path, because
//! [F8](../../../docs/src/features/purpose-built-workloads.md) is right that a blended number
//! cannot attribute anything: if reads and writes move one figure between them, a change to either
//! moves it and neither can be credited. That argument is about **attribution**, and it is not the
//! only question worth asking.
//!
//! The question this family answers is the other one. A caller choosing a store does not want to
//! know what the write path costs in isolation; they want to know what *their* workload costs -
//! some ratio of reads to writes, over rows of some width, against some kind of table. Nothing in
//! this repository could answer that, because there was no read/write mixture anywhere and the row
//! width axis existed only inside the encryption sweep.
//!
//! **A regression is never attributed to a grid arm.** The grid says a mixture got slower; the
//! isolating workloads say which half. Reading it the other way around is the failure F8 describes.
//!
//! # The cross, not the cube
//!
//! Four table kinds, six mixtures and eleven widths is two hundred and sixty four arms, and a
//! capture that took a working day. This sweeps each axis fully against a fixed reference of the
//! others instead:
//!
//! - the **width sweep** runs all eleven widths, on all four tables, at the reference mixture
//! - the **mixture sweep** runs all six mixtures, on all four tables, at the reference width
//! - the **skew sweep** runs the three key distributions on the two persistent tables, at both
//!   references
//! - the **depth ladder** runs four load depths on one table, at both references
//!
//! The two sweeps share their four `r50/1024` cells, so it is seventy four arms rather than
//! seventy eight. What the cross cannot see is an *interaction* - a cost that appears only at a
//! wide row under a write-heavy mixture would be missed by both sweeps. That is recorded in
//! `docs/src/appendix/todos.md` rather than papered over.
//!
//! # YCSB, borrowed exactly where it is borrowed
//!
//! `r50` is YCSB's workload **A**, `r95` is **B** and `r100` is **C**, the reference width is
//! YCSB's 1 KiB record, and [`Keys`] is YCSB's own generator rather than one shaped like it. Two
//! things are deliberately not YCSB's, and both are on the feature page rather than left to be
//! discovered: the record is one payload field instead of ten, and **a write is an insert into a
//! key range the reads never touch** rather than an update in place. The second is what keeps every
//! read a hit and keeps the payload on the write path - an `#[shoal(update)]` write carries only
//! `label`, so under one the row width axis would not reach the write path at all.
//!
//! # What the depth is, and why it does not move
//!
//! Every arm runs at [`DEPTH`] outstanding queries on one client, at every width. Scaling the depth
//! down as the rows get wider is the confound [`encryption`](super::encryption) objects to in the
//! transport pair: the two axes would move together and neither could be plotted against the other.
//! Thirty two is the deepest value that is safe at a four megabyte row and deep enough to keep
//! twelve shards busy at sixty four bytes.
//!
//! The cost of holding it fixed is that **a grid arm's throughput is its throughput at that depth**,
//! not the throughput the server is capable of, and that
//! [O31](../../../docs/src/appendix/optimizations.md) applies: past the knee of the curve a p50
//! stops being a service time and becomes a measure of how long the queue is. The depth ladder is
//! what says where that knee is.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;

use crate::model::macro_layer::{ScaleFacts, Timing};
use crate::workloads::harness::driver::{self, Batch, StreamMode};
use crate::workloads::harness::keys::{KeyDistribution, Keys};
use crate::workloads::harness::rows::RowProfile;
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::schema::{
    BenchClient, BenchQueryKinds, Event, EventGet, Item, ItemGet, MemEvent, MemEventGet, MemItem,
    MemItemGet, sort_key,
};
use crate::workloads::workload::{
    BoxFuture, ConfOverrides, Context, Measurement, ServerNeed, Workload, WorkloadPlan,
};

/// The row widths the width sweep covers
///
/// Eight fixed widths spanning sixteen doublings, and three declared mixtures. The fixed points
/// are where a cliff would show up; the mixtures are where a caller actually lives.
pub const WIDTHS: [RowProfile; 11] = [
    RowProfile::Fixed(64),
    RowProfile::Fixed(128),
    RowProfile::Fixed(512),
    RowProfile::Fixed(1024),
    RowProfile::Fixed(8 * 1024),
    RowProfile::Fixed(512 * 1024),
    RowProfile::Fixed(1024 * 1024),
    RowProfile::Fixed(4 * 1024 * 1024),
    RowProfile::small(),
    RowProfile::mid(),
    RowProfile::large(),
];

/// The row widths that close the gap the first sweep left between 8 KiB and 512 KiB
///
/// Declared apart from [`WIDTHS`] rather than spliced into it, and the split is bookkeeping rather
/// than meaning: a workload's position in [`crate::workload_ids::IDS`] decides the port a capture
/// gives it, so inserting 16 KiB between `8 * 1024` and `512 * 1024` would move every grid arm
/// after it onto a different port. These are minted in their own pass at the end of [`Grid::all`],
/// which leaves every arm that existed before them exactly where it was.
///
/// What they buy is the knee. The axis went 8 KiB to 512 KiB with nothing in between, so a step at
/// the staging buffer and a slope that starts near it were indistinguishable, and everything the
/// row size page said about *where* the curve bends was an inference from two points either side of
/// a 64x hole.
pub const INFILL_WIDTHS: [RowProfile; 5] = [
    RowProfile::Fixed(16 * 1024),
    RowProfile::Fixed(32 * 1024),
    RowProfile::Fixed(64 * 1024),
    RowProfile::Fixed(128 * 1024),
    RowProfile::Fixed(256 * 1024),
];

/// The mixtures the width axis is repeated at, beyond the reference one
///
/// The two ends, and `r0` is the one that matters. Over 1 KiB to 8 KiB the persistent arms lose
/// far more throughput than the ephemeral ones, which is a *write* path effect being measured under
/// a mixture that is half reads - so the reference sweep asks it half a question. `r100` is its
/// control: a pure read sweep has no intent log on the path at all, so the per-byte cost it shows
/// is the read path's alone and the difference between the two is the write path's.
pub const WIDE_MIXES: [u32; 2] = [0, 100];

/// The arms the stage layer profiles, which is the width axis seen three times
///
/// A stage breakdown at one width cannot say which of the nineteen stages grows with bytes, and one
/// at every width would cost more than the rest of a capture. Three points answer the question: the
/// reference cell, the first width above the 4096 byte staging buffer, and one well past the knee.
pub const STAGED_ARMS: [&str; 3] = [
    "macro/grid/unsorted/r50/1024",
    "macro/grid/unsorted/r50/8192",
    "macro/grid/unsorted/r50/524288",
];

/// Every width the grid sweeps, in the order arms are minted in
///
/// [`WIDTHS`] then [`INFILL_WIDTHS`], which is mint order rather than ascending width. A page that
/// wants the axis in width order sorts by the width each arm *recorded*, because a mixture's place
/// on a numeric axis is its mean and only the artifact knows what that came out at.
fn every_width() -> impl Iterator<Item = RowProfile> {
    WIDTHS.into_iter().chain(INFILL_WIDTHS)
}

/// The read shares the mixture sweep covers, as percentages
///
/// The two ends, the even split, both seventy/thirty leanings, and YCSB's ninety five. Six points
/// rather than five because `r95` is what makes a number here readable next to a published YCSB
/// **B** figure, and it costs one arm per table.
pub const MIXES: [u32; 6] = [0, 30, 50, 70, 95, 100];

/// The mixture every sweep but the mixture sweep holds fixed
pub const REFERENCE_MIX: u32 = 50;

/// The row width every sweep but the width sweep holds fixed
///
/// YCSB's record size, so the reference cell of the whole grid is the cell a published number is
/// most likely to be comparable with.
pub const REFERENCE_WIDTH: RowProfile = RowProfile::Fixed(1024);

/// How many queries every grid arm keeps outstanding at once
///
/// See the module header for why this does not move with the row width.
pub const DEPTH: u32 = 32;

/// The load depths the depth ladder covers
///
/// One is the floor, where a service time is the whole round trip and nothing queues. 128 is deep
/// enough to be past the knee on this hardware, which is the point of measuring it.
pub const DEPTHS: [u32; 4] = [1, 8, 32, 128];

/// The tables the grid sweeps
///
/// Declared in this order, which is the order the arms are minted in and therefore the order their
/// ports are assigned in.
pub const TABLES: [Table; 4] = [
    Table::Unsorted,
    Table::Sorted,
    Table::UnsortedMem,
    Table::SortedMem,
];

/// The tables the skew sweep covers
///
/// The persistent pair only. Skew is about what stays resident, and an ephemeral table holds
/// everything it was given for as long as the process lives, so there is nothing for a skewed
/// access pattern to be warmer than.
pub const SKEW_TABLES: [Table; 2] = [Table::Unsorted, Table::Sorted];

/// The key distributions the skew sweep covers
pub const SKEWS: [KeyDistribution; 3] = [
    KeyDistribution::Uniform,
    KeyDistribution::Zipfian,
    KeyDistribution::Latest,
];

/// How many bytes of payload one full run of one arm moves
///
/// The budget that flattens the cost across the width axis, the same trick
/// [`encryption`](super::encryption) uses and with the same consequence: **the widest arms have the
/// fewest samples**, so their p99 is the worst few of a few hundred rather than a percentile. Read
/// the p50 at the wide end.
const BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// The fewest queries any arm measures
///
/// Below a couple of hundred a p50 stops being worth reading. A four megabyte arm hits this floor
/// and therefore moves more than [`BUDGET_BYTES`], which is the floor winning on purpose.
const MIN_QUERIES: u64 = 200;

/// The most queries any arm measures
///
/// The narrow arms would otherwise run millions of queries to spend their byte budget, buying
/// precision nobody reads at a cost the capture notices.
const MAX_QUERIES: u64 = 20_000;

/// How many bytes of rows an arm seeds before it is measured
const SEED_BYTES: u64 = 256 * 1024 * 1024;

/// The fewest rows any arm seeds
///
/// A four megabyte arm lands here, so its reads walk sixty four partitions. That is a small key
/// space, and it is the honest one at that width: two hundred and fifty six of those rows is a
/// gigabyte, and an arm that seeded a gigabyte would be measuring eviction rather than the wire.
const MIN_ROWS: u64 = 64;

/// The most rows any arm seeds
const MAX_ROWS: u64 = 20_000;

/// The share of a frame one seed bundle is allowed to fill
///
/// A bundle is refused outright at the frame bound, so sizing to exactly the bound would fail on
/// the archive's own overhead. The same quarter [`transport`](super::transport) uses.
const SEED_FRAME_SHARE: u64 = 4;

/// How many distinct payloads are built per width before a run starts
///
/// Payloads are built up front and cloned per query, because generating a four megabyte string
/// inside the run loop would put the cost of building it in the arm's wall clock and therefore in
/// its throughput. Eight rather than one so that no two consecutive rows carry identical bytes,
/// and eight rather than a thousand because eight four megabyte payloads is thirty two megabytes.
const PAYLOAD_VARIANTS: u64 = 8;

/// Which table an arm drives
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {
    /// The persistent unsorted table, one row per partition
    Unsorted,
    /// The persistent sorted table, one row per partition
    Sorted,
    /// The ephemeral unsorted table, which is the same table with a storage engine that stores
    /// nothing
    UnsortedMem,
    /// The ephemeral sorted table
    SortedMem,
}

impl Table {
    /// The segment this table contributes to an identifier
    pub fn as_str(self) -> &'static str {
        match self {
            Table::Unsorted => "unsorted",
            Table::Sorted => "sorted",
            Table::UnsortedMem => "unsorted_mem",
            Table::SortedMem => "sorted_mem",
        }
    }

    /// What this table is called in the artifact
    ///
    /// Spelled out rather than abbreviated, because this is the field a reader groups by and
    /// `unsorted_mem` does not say which of the two axes it differs on.
    pub fn artifact_name(self) -> String {
        match self {
            Table::Unsorted => "persistent_unsorted",
            Table::Sorted => "persistent_sorted",
            Table::UnsortedMem => "ephemeral_unsorted",
            Table::SortedMem => "ephemeral_sorted",
        }
        .to_string()
    }

    /// Builds a get of one row in this table
    ///
    /// Every table holds one row per partition, so a get names one key and comes back with one
    /// row. The sorted tables name the single sort key their rows were written at, so a get there
    /// is a keyed lookup and not a range scan - the two kinds of table then differ in how they
    /// store a row rather than in how much of it is being asked for.
    ///
    /// # Arguments
    ///
    /// * `key` - The partition to read
    fn get(self, key: u64) -> BenchQueryKinds {
        match self {
            Table::Unsorted => ItemGet::new(vec![key]).into(),
            Table::Sorted => EventGet::new(vec![key]).sort_keys(vec![sort_key(0)]).into(),
            Table::UnsortedMem => MemItemGet::new(vec![key]).into(),
            Table::SortedMem => MemEventGet::new(vec![key])
                .sort_keys(vec![sort_key(0)])
                .into(),
        }
    }

    /// Builds an insert of one row into this table
    ///
    /// # Arguments
    ///
    /// * `key` - The partition to write into
    /// * `filter` - The value of the filterable field, which no grid arm queries on
    /// * `payload` - The row's payload, already built
    fn insert(self, key: u64, filter: u64, payload: String) -> BenchQueryKinds {
        match self {
            Table::Unsorted => Item {
                id: key,
                bucket: filter,
                // narrow and fixed, so the width axis is the payload and nothing else
                label: String::new(),
                payload,
            }
            .into(),
            Table::Sorted => Event {
                stream: key,
                at: sort_key(0),
                kind: filter,
                payload,
            }
            .into(),
            Table::UnsortedMem => MemItem {
                id: key,
                bucket: filter,
                label: String::new(),
                payload,
            }
            .into(),
            Table::SortedMem => MemEvent {
                stream: key,
                at: sort_key(0),
                kind: filter,
                payload,
            }
            .into(),
        }
    }
}

/// Which of the family's four sweeps an arm belongs to
///
/// Only the identifier depends on this. Two arms of different sweeps that describe the same
/// measurement are the same measurement, which is what the duplicate-arm tests below pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sweep {
    /// A cell of the width or mixture sweep
    Cell,
    /// A point of the key distribution sweep
    Skew,
    /// A rung of the load depth ladder
    Depth,
    /// A rung of the width axis measured with one query outstanding
    ///
    /// Its own variant rather than a [`Sweep::Depth`] rung that happens to carry a width, because
    /// the two ladders are named apart and drawn apart: `Depth` walks the depth at the reference
    /// width, this walks the width at depth one. They cross at `macro/grid/depth/1`, which is
    /// minted by the ladder and not here.
    WidthDepth,
    /// A point of a configuration sweep, holding the name of the setting that moved
    ///
    /// Minted by [`conf_sweep`](super::conf_sweep) rather than by [`Grid::all`], because the two
    /// answer different questions: the grid asks what a mixture costs against the configuration
    /// this repository benchmarks under, and a configuration arm asks what one setting of that
    /// configuration is worth. They share a driver because the second is the first with one field
    /// moved, and sharing it is what makes them comparable.
    Conf {
        /// Which setting this arm moved
        knob: &'static str,
    },
}

/// One arm of the grid
pub struct Grid {
    /// Which sweep this arm belongs to, which decides how it is named
    pub sweep: Sweep,
    /// Which table it drives
    pub table: Table,
    /// What share of its queries are reads
    pub read_pct: u32,
    /// How wide the rows it moves are
    pub rows: RowProfile,
    /// Which keys its reads ask for
    pub distribution: KeyDistribution,
    /// How many queries it keeps outstanding at once
    pub depth: u32,
    /// What this arm asks of its server
    ///
    /// Empty for every arm of the grid proper, which is the point: those arms are about what a
    /// caller's workload costs against the committed `shoal.yml`, so an arm that pinned a setting
    /// would be holding still something it is not about. A configuration arm names exactly one
    /// field here and nothing else, which is what makes the difference between it and the arm
    /// beside it attributable to that field.
    pub conf: ConfOverrides,
    /// This arm's identifier, built once because the trait hands back a `&'static str`
    pub id: &'static str,
    /// One line saying what this arm measures, built once for the same reason
    pub summary: &'static str,
}

impl Grid {
    /// Every arm of every sweep
    ///
    /// **Sweep outermost, then table, then the swept axis.** A workload's position in
    /// `workload_ids::IDS` decides the port a capture gives it, so this order is the order that
    /// list declares and neither may be reshuffled to read better.
    pub fn all() -> Vec<Grid> {
        // every width but the reference one, which is what the two passes added after the first
        // capture sweep - the reference width is already minted for them by the sweeps above
        let widened = every_width().count() - 1;
        let mut built = Vec::with_capacity(
            TABLES.len() * WIDTHS.len()
                + TABLES.len() * (MIXES.len() - 1)
                + SKEWS.len() * SKEW_TABLES.len()
                + DEPTHS.len()
                + TABLES.len() * INFILL_WIDTHS.len()
                + WIDE_MIXES.len() * TABLES.len() * widened
                + widened,
        );
        // the width sweep, at the reference mixture
        for table in TABLES {
            for width in WIDTHS {
                built.push(Grid::cell(table, REFERENCE_MIX, width));
            }
        }
        // the mixture sweep, at the reference width. the reference mixture is skipped because the
        // width sweep already minted it for every table, and a duplicate identifier is not a
        // second measurement - it is a collision
        for table in TABLES {
            for mix in MIXES {
                if mix != REFERENCE_MIX {
                    built.push(Grid::cell(table, mix, REFERENCE_WIDTH));
                }
            }
        }
        // the skew sweep, at both references, on the persistent pair
        for skew in SKEWS {
            for table in SKEW_TABLES {
                built.push(Grid::skew(table, skew));
            }
        }
        // the depth ladder, at both references, on one table
        for depth in DEPTHS {
            built.push(Grid::depth(depth));
        }
        // everything below this line was added after the first capture and is minted in its own
        // pass for one reason: an arm's position here decides its port, so extending a sweep in
        // place would move every arm after it. see `INFILL_WIDTHS`.
        //
        // the widths that close the 64x hole, at the reference mixture, on every table
        for table in TABLES {
            for width in INFILL_WIDTHS {
                built.push(Grid::cell(table, REFERENCE_MIX, width));
            }
        }
        // the whole width axis again at each end of the mixture, on every table. the reference
        // width is skipped at both ends because the mixture sweep already minted it there, and a
        // duplicate identifier is a collision rather than a second measurement
        for mix in WIDE_MIXES {
            for table in TABLES {
                for width in every_width() {
                    if width != REFERENCE_WIDTH {
                        built.push(Grid::cell(table, mix, width));
                    }
                }
            }
        }
        // and the width axis at one query outstanding, on one table, which is what separates a
        // service time from a queue length along it. the reference width is skipped for the same
        // reason as above: `macro/grid/depth/1` already is that arm
        for width in every_width() {
            if width != REFERENCE_WIDTH {
                built.push(Grid::width_depth(width));
            }
        }
        built
    }

    /// Builds one cell of the width or mixture sweep
    ///
    /// # Arguments
    ///
    /// * `table` - Which table this cell drives
    /// * `read_pct` - What share of its queries are reads
    /// * `rows` - How wide its rows are
    fn cell(table: Table, read_pct: u32, rows: RowProfile) -> Self {
        let id: &'static str = Box::leak(
            format!(
                "macro/grid/{}/r{read_pct}/{}",
                table.as_str(),
                rows.segment()
            )
            .into_boxed_str(),
        );
        Grid {
            sweep: Sweep::Cell,
            table,
            read_pct,
            rows,
            distribution: KeyDistribution::Uniform,
            depth: DEPTH,
            conf: ConfOverrides::default(),
            id,
            summary: "a read/write mixture at one row width against one kind of table",
        }
    }

    /// Builds one point of the skew sweep
    ///
    /// # Arguments
    ///
    /// * `table` - Which table this point drives
    /// * `distribution` - Which keys its reads ask for
    fn skew(table: Table, distribution: KeyDistribution) -> Self {
        let id: &'static str = Box::leak(
            format!(
                "macro/skew/{}/{}",
                distribution.as_str(),
                table.as_str()
            )
            .into_boxed_str(),
        );
        Grid {
            sweep: Sweep::Skew,
            table,
            read_pct: REFERENCE_MIX,
            rows: REFERENCE_WIDTH,
            distribution,
            depth: DEPTH,
            conf: ConfOverrides::default(),
            id,
            summary: "the reference mixture with the reads drawn from a skewed key space",
        }
    }

    /// Builds one rung of the depth ladder
    ///
    /// # Arguments
    ///
    /// * `depth` - How many queries this rung keeps outstanding
    fn depth(depth: u32) -> Self {
        let id: &'static str =
            Box::leak(format!("macro/grid/depth/{depth}").into_boxed_str());
        Grid {
            sweep: Sweep::Depth,
            table: Table::Unsorted,
            read_pct: REFERENCE_MIX,
            rows: REFERENCE_WIDTH,
            distribution: KeyDistribution::Uniform,
            depth,
            conf: ConfOverrides::default(),
            id,
            summary: "the reference mixture at one load depth",
        }
    }

    /// Builds one rung of the width axis at a single outstanding query
    ///
    /// Every other arm in the grid runs at [`DEPTH`], at every width. At four megabytes that is 128
    /// MiB outstanding on one client against a key space of sixty four partitions, so those arms
    /// measure queueing and partition contention as much as service time - and a latency past the
    /// knee of a throughput curve is a measure of how long the queue is rather than of how long the
    /// work took. This ladder is the same axis with nothing queued, so the difference between the
    /// two at one width is what the depth was costing there.
    ///
    /// One table, because the point is the depth and not the table, and the persistent unsorted
    /// table is the one every other page's reference cell drives.
    ///
    /// # Arguments
    ///
    /// * `rows` - How wide this rung's rows are
    fn width_depth(rows: RowProfile) -> Self {
        let id: &'static str =
            Box::leak(format!("macro/grid/depth/1/{}", rows.segment()).into_boxed_str());
        Grid {
            sweep: Sweep::WidthDepth,
            table: Table::Unsorted,
            read_pct: REFERENCE_MIX,
            rows,
            distribution: KeyDistribution::Uniform,
            // the whole point of the ladder, and the one field that separates it from a cell
            depth: 1,
            conf: ConfOverrides::default(),
            id,
            summary: "the reference mixture at one row width, with one query outstanding",
        }
    }

    /// The scale a context was built at
    ///
    /// # Arguments
    ///
    /// * `ctx` - The run this workload was given
    fn scale_of(ctx: &Context) -> Scale {
        // anything that is not a smoke run is a full one, which is what every other workload does
        if ctx.scale.scale == "smoke" {
            Scale::Smoke
        } else {
            Scale::Full
        }
    }
}

/// How many queries one arm measures at a mean row width
///
/// # Arguments
///
/// * `row_bytes` - The mean width of one row
/// * `scale` - How large a run was asked for
///
/// # Examples
///
/// ```
/// use shoal_bench::workloads::grid::queries_for;
/// use shoal_bench::workloads::harness::seed::Scale;
///
/// // a narrow row is capped rather than allowed to spend its whole byte budget
/// assert_eq!(queries_for(64, Scale::Full), 20_000);
/// // and a four megabyte row is floored, so its p50 still has samples behind it
/// assert_eq!(queries_for(4 * 1024 * 1024, Scale::Full), 200);
/// ```
pub fn queries_for(row_bytes: u64, scale: Scale) -> u64 {
    // a fixed budget of bytes moved, floored so the widest arm still has enough samples for a
    // median and capped so the narrowest does not run all day
    let budget = (BUDGET_BYTES / row_bytes.max(1)).clamp(MIN_QUERIES, MAX_QUERIES);
    match scale {
        Scale::Smoke => (budget / 100).max(20),
        Scale::Full => budget,
    }
}

/// How many rows one arm seeds at a mean row width
///
/// # Arguments
///
/// * `row_bytes` - The mean width of one row
/// * `scale` - How large a run was asked for
pub fn rows_for(row_bytes: u64, scale: Scale) -> u64 {
    // the same shape as the query budget, and for the same reason
    let rows = (SEED_BYTES / row_bytes.max(1)).clamp(MIN_ROWS, MAX_ROWS);
    match scale {
        Scale::Smoke => (rows / 100).max(MIN_ROWS),
        Scale::Full => rows,
    }
}

/// Whether the query at an index is a read
///
/// The mixture is a function of the index rather than a counter, for the reason
/// [`Seeded::at`](crate::workloads::harness::seed::Seeded::at) gives: several slots pull from one
/// cursor, so anything decided in sequence would depend on which slot got there first.
///
/// # Arguments
///
/// * `seed` - The mixture stream's seed
/// * `index` - Which query is being decided
/// * `read_pct` - What share of queries should be reads
fn is_read(seed: u64, index: u64, read_pct: u32) -> bool {
    // the two ends are exact rather than probabilistic: an `r100` arm must issue no writes at all,
    // or its read latencies are contaminated by the handful a draw happened to produce
    match read_pct {
        0 => false,
        100 => true,
        share => Seeded::at(seed, index).below(100) < u64::from(share),
    }
}

/// The payloads one arm writes, built before anything is timed
///
/// Keyed by width, because a mixture writes rows of several widths and each needs its own set.
struct Payloads {
    /// The built payloads, keyed by their width
    by_width: BTreeMap<u64, Vec<String>>,
}

impl Payloads {
    /// Builds every payload an arm will need
    ///
    /// # Arguments
    ///
    /// * `profile` - The width distribution the arm writes
    /// * `seed` - The seed the whole run derives from
    fn build(profile: RowProfile, seed: u64) -> Self {
        let mut payloads = Seeded::stream(seed, "grid/payloads");
        let mut by_width = BTreeMap::new();
        // every width the profile can produce needs its own set, since a payload of the wrong
        // width would make the row a different size than the arm says it is
        for width in profile.widths() {
            let built: Vec<String> = (0..PAYLOAD_VARIANTS)
                .map(|_| payloads.string(width as usize))
                .collect();
            by_width.insert(width, built);
        }
        Payloads { by_width }
    }

    /// The payload the row at an index carries
    ///
    /// Cloned rather than borrowed, because a query owns the row it carries. That clone is inside
    /// the arm's wall clock and outside its samples, which is the same place the query building
    /// itself sits.
    ///
    /// # Arguments
    ///
    /// * `width` - How wide this row is
    /// * `index` - Which row is being built
    fn at(&self, width: u64, index: u64) -> String {
        // a width the arm never declared is a bug in the caller rather than something to invent a
        // payload for
        let built = self
            .by_width
            .get(&width)
            .unwrap_or_else(|| panic!("no payload was built for a {width} byte row"));
        built[(index % PAYLOAD_VARIANTS) as usize].clone()
    }
}

impl Workload for Grid {
    /// What this workload is called
    fn id(&self) -> &'static str {
        self.id
    }

    /// What this workload measures
    fn summary(&self) -> &'static str {
        // built when the arm was minted rather than matched here, because a configuration arm's
        // summary names the setting it moved and there is no `&'static str` to match onto
        self.summary
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        // one query per slot, each stamped on its own, so a sample is a service time. a mixture
        // measured per batch would charge a read for the writes batched beside it
        Timing::PerQuery
    }

    /// Whether the hotpath layer may run this workload
    fn profiles(&self) -> bool {
        // two hundred and twenty nine workloads under an attribution layer would cost more than the
        // rest of a capture put together, for profiles that would mostly repeat each other
        false
    }

    /// Whether the stage layer may run this workload
    ///
    /// Three arms of the width axis say yes, and they are the reason the two instrumented layers
    /// stopped sharing one list. A hotpath profile attributes time to scopes and one arm's mostly
    /// repeats another's; a stage breakdown attributes one query's latency to nineteen points on
    /// its path, and the question worth asking of it is which of the nineteen grows with the row
    /// width - which is a question about several widths of the same workload and about nothing
    /// else. See [`STAGED_ARMS`].
    fn stage_profiles(&self) -> bool {
        STAGED_ARMS.contains(&self.id)
    }

    /// What this workload needs before it can run
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        let row_bytes = self.rows.mean();
        let rows = rows_for(row_bytes, scale);
        WorkloadPlan {
            // a grid arm pins nothing about the server: it is about what a caller's workload costs
            // against the configuration this repository benchmarks under, so an arm that named a
            // shard count or a memory limit would be holding still something it is not about - the
            // same choice `transport` and `encryption` make. a configuration arm carries exactly
            // one field here, which is the only difference between the two.
            server: ServerNeed::Fresh(self.conf.clone()),
            scale: ScaleFacts {
                scale: scale.as_str().to_string(),
                rows,
                // the mean for a mixture, which `row_profile` beside it says is a mean
                row_bytes,
                // one row per partition, so the key count is the row count
                keys: rows,
                concurrency: self.depth,
                clients: None,
                read_pct: Some(self.read_pct),
                row_profile: self.rows.artifact_name(),
                distribution: self.distribution.artifact_name(),
                table_kind: Some(self.table.artifact_name()),
            },
            // a twentieth of the run, so connection establishment and the first cold partitions are
            // behind the arm before it samples anything
            warmup: (queries_for(row_bytes, scale) / 20).max(10),
        }
    }

    /// Writes the rows this workload's reads will find, without timing any of it
    ///
    /// Every arm seeds, including the ones that never read. An `r0` arm has no use for these rows,
    /// but it starts from the same table every other arm starts from, and an arm whose starting
    /// state depended on its mixture would be measuring the mixture twice over.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let client = ctx.client().await?;
            let payloads = Payloads::build(self.rows, ctx.seed);
            let mut filters = Seeded::stream(ctx.seed, "grid/filters");
            let profile = self.rows;
            let table = self.table;
            let seed = ctx.seed;
            let total = ctx.scale.rows;
            // sized from the widest row this arm can produce rather than from the mean, since a
            // bundle of the widest rows is the one that has to fit inside a frame
            let batch = seed_batch(profile.widest(), frame_bytes(ctx));
            let mut built = 0u64;
            let batches = move || {
                if built >= total {
                    return None;
                }
                let mut queries = shoal::shared::queries::Queries::<BenchClient>::default();
                for _ in 0..(batch as u64).min(total - built) as usize {
                    let width = profile.width(seed, built);
                    queries.add_mut(table.insert(
                        built,
                        filters.below(16),
                        payloads.at(width, built),
                    ));
                    built += 1;
                }
                Some(Batch { queries })
            };
            // the gate is bounded in bytes rather than queries, since a bundle of wide rows holds
            // as much memory as a get's responses do
            let gate = (BUDGET_BYTES / profile.widest().max(1)).clamp(8, 1024) as usize;
            driver::drive_with(&client, batches, "seed", 0, StreamMode::Unordered, gate).await?;
            Ok(())
        })
    }

    /// Runs the mixture, keeping the reads' latencies apart from the writes'
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        Box::pin(async move {
            let clients = vec![Arc::new(ctx.client().await?)];
            let rows = ctx.scale.rows;
            let queries = queries_for(self.rows.mean(), Self::scale_of(ctx));
            // built before the run so that neither the payloads nor the Zipfian constant is paid
            // for inside the wall clock this arm reports as a throughput
            let payloads = Payloads::build(self.rows, ctx.seed);
            let reads = Keys::new(self.distribution, rows, ctx.seed, "grid/reads");
            let mix_seed = Seeded::stream(ctx.seed, "grid/mix").next_u64();
            let profile = self.rows;
            let table = self.table;
            let read_pct = self.read_pct;
            let seed = ctx.seed;
            driver::drive_mixed_per_query(&clients, self.depth, queries, ctx.warmup, move |index| {
                if is_read(mix_seed, index, read_pct) {
                    // a read asks for a key inside the seeded range, so every read is a hit and
                    // the arm measures a lookup rather than a miss
                    ("read", table.get(reads.at(index)))
                } else {
                    // a write lands past the seeded range, so it collides with nothing a read is
                    // asking for and the read hit rate does not drift as the arm runs
                    let key = rows + index;
                    let width = profile.width(seed, key);
                    (
                        "write",
                        table.insert(key, index % 16, payloads.at(width, index)),
                    )
                }
            })
            .await
        })
    }
}

/// How many rows of a given width fit in one seed bundle
///
/// Sized against the frame bound the server was **actually started with** rather than against
/// [`DEFAULT_MAX_FRAME_BYTES`](shoal::shared::protocol::DEFAULT_MAX_FRAME_BYTES). The two are the
/// same for every arm that does not sweep the bound, and for the arms that do, the constant is the
/// wrong number: a bundle sized against 64 MiB and sent to a server that will accept 1 MiB is
/// refused outright, which is a workload that cannot run rather than one that runs slowly.
///
/// # Arguments
///
/// * `row_bytes` - How wide one row is
/// * `frame_bytes` - The largest frame the server will accept
fn seed_batch(row_bytes: u64, frame_bytes: u64) -> usize {
    // what a quarter of a frame holds at this width, and never fewer than one row
    let budget = frame_bytes / SEED_FRAME_SHARE / row_bytes.max(1);
    (budget.max(1) as usize).min(driver::BATCH)
}

/// The largest frame the server this run was given will accept
///
/// Falls back to the protocol default, which is what a workload running without a server would see
/// and what every arm that does not move the bound resolves to anyway.
///
/// # Arguments
///
/// * `ctx` - The run this workload was given
fn frame_bytes(ctx: &Context) -> u64 {
    // the resolved configuration records it, so this is reading back what the server was started
    // with rather than assuming what it was started with
    ctx.conf
        .as_ref()
        .and_then(|conf| conf.max_frame_bytes)
        .unwrap_or_else(|| u64::from(shoal::shared::protocol::DEFAULT_MAX_FRAME_BYTES))
}

#[cfg(test)]
mod tests {
    use super::{
        DEPTH, DEPTHS, Grid, INFILL_WIDTHS, MIXES, REFERENCE_MIX, REFERENCE_WIDTH, SKEWS,
        SKEW_TABLES, STAGED_ARMS, Sweep, TABLES, WIDE_MIXES, WIDTHS, every_width, is_read,
        queries_for, rows_for, seed_batch,
    };
    use crate::model::macro_layer::Timing;
    use crate::workloads::harness::keys::KeyDistribution;
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::Workload;

    /// Every arm of every sweep is minted once, and nothing is minted twice
    ///
    /// A duplicate identifier is not a second measurement of anything - it is two workloads
    /// claiming one artifact key and one port, and the second one silently wins.
    #[test]
    fn every_arm_is_minted_exactly_once() {
        let all = Grid::all();
        let widened = every_width().count() - 1;
        let expected = TABLES.len() * WIDTHS.len()
            + TABLES.len() * (MIXES.len() - 1)
            + SKEWS.len() * SKEW_TABLES.len()
            + DEPTHS.len()
            + TABLES.len() * INFILL_WIDTHS.len()
            + WIDE_MIXES.len() * TABLES.len() * widened
            + widened;
        assert_eq!(all.len(), expected);
        assert_eq!(all.len(), 229, "the capture's cost changed");
        let mut ids: Vec<&str> = all.iter().map(|arm| arm.id()).collect();
        ids.sort_unstable();
        let before = ids.len();
        ids.dedup();
        assert_eq!(before, ids.len(), "a grid id is minted twice");
    }

    /// An identifier names every axis that varies within its sweep
    ///
    /// The identifier is the join key of every comparison, so an axis missing from it is an axis
    /// two different measurements would be joined across.
    #[test]
    fn an_id_names_the_axes_of_its_sweep() {
        for arm in Grid::all() {
            let expected = match arm.sweep {
                Sweep::Cell => format!(
                    "macro/grid/{}/r{}/{}",
                    arm.table.as_str(),
                    arm.read_pct,
                    arm.rows.segment()
                ),
                Sweep::Skew => format!(
                    "macro/skew/{}/{}",
                    arm.distribution.as_str(),
                    arm.table.as_str()
                ),
                Sweep::Depth => format!("macro/grid/depth/{}", arm.depth),
                Sweep::WidthDepth => format!("macro/grid/depth/1/{}", arm.rows.segment()),
                // the configuration sweep is minted by `conf_sweep`, which names its own arms and
                // tests them there. `Grid::all` producing one would mean a sweep had moved house.
                Sweep::Conf { knob } => {
                    unreachable!("Grid::all minted a configuration arm for {knob}")
                }
            };
            assert_eq!(arm.id(), expected);
        }
    }

    /// The two sweeps cross at the reference cell rather than measuring it twice
    #[test]
    fn the_sweeps_share_their_reference_cells() {
        let all = Grid::all();
        // one cell per table at the reference mixture and the reference width, not two
        let shared: Vec<&str> = all
            .iter()
            .filter(|arm| {
                arm.sweep == Sweep::Cell
                    && arm.read_pct == REFERENCE_MIX
                    && arm.rows == REFERENCE_WIDTH
            })
            .map(|arm| arm.id())
            .collect();
        assert_eq!(shared.len(), TABLES.len(), "{shared:?}");
    }

    /// The uniform arm of the skew sweep is the reference cell, measured the same way
    ///
    /// The duplication is the point: the two arms differ in their identifier and in nothing else,
    /// so a capture where they disagree has something moving that neither of them names. This is
    /// the control-pair shape [F9] established for storage, applied to the grid's own reference.
    #[test]
    fn the_uniform_skew_arm_matches_the_reference_cell() {
        let all = Grid::all();
        for table in SKEW_TABLES {
            let skew = all
                .iter()
                .find(|arm| {
                    arm.sweep == Sweep::Skew
                        && arm.table == table
                        && arm.distribution == KeyDistribution::Uniform
                })
                .expect("the uniform skew arm");
            let cell = all
                .iter()
                .find(|arm| {
                    arm.sweep == Sweep::Cell
                        && arm.table == table
                        && arm.read_pct == REFERENCE_MIX
                        && arm.rows == REFERENCE_WIDTH
                })
                .expect("the reference cell");
            for scale in [Scale::Smoke, Scale::Full] {
                let left = skew.plan(scale);
                let right = cell.plan(scale);
                assert_eq!(left.scale, right.scale, "{} against {}", skew.id(), cell.id());
                assert_eq!(left.warmup, right.warmup);
                assert_eq!(left.server, right.server);
            }
            assert_eq!(skew.timing(), cell.timing());
        }
    }

    /// The depth ladder's middle rung is the reference cell with the depth left alone
    ///
    /// Which is what lets the ladder place the whole grid on its curve: one rung of it *is* the
    /// grid's operating point rather than something near it.
    #[test]
    fn the_ladder_passes_through_the_grids_own_depth() {
        let all = Grid::all();
        let rung = all
            .iter()
            .find(|arm| arm.sweep == Sweep::Depth && arm.depth == DEPTH)
            .expect("the ladder rung at the grid's depth");
        let cell = all
            .iter()
            .find(|arm| {
                arm.sweep == Sweep::Cell
                    && arm.table == rung.table
                    && arm.read_pct == REFERENCE_MIX
                    && arm.rows == REFERENCE_WIDTH
            })
            .expect("the reference cell");
        for scale in [Scale::Smoke, Scale::Full] {
            assert_eq!(rung.plan(scale).scale, cell.plan(scale).scale);
        }
    }

    /// Two cells that differ in one axis differ in that axis alone
    ///
    /// The property that makes a difference between two cells attributable to the axis they were
    /// swept along. Without it a width sweep would also be sweeping the query count, the warmup and
    /// the key space at once, which is what the byte budget is careful to keep out of the plan.
    #[test]
    fn two_cells_of_one_sweep_differ_in_one_axis() {
        let all = Grid::all();
        // two mixtures at one width and one table: everything but the read share is held
        let cells: Vec<&Grid> = all
            .iter()
            .filter(|arm| {
                arm.sweep == Sweep::Cell
                    && arm.table == super::Table::Unsorted
                    && arm.rows == REFERENCE_WIDTH
            })
            .collect();
        assert_eq!(cells.len(), MIXES.len());
        let first = cells[0].plan(Scale::Full);
        for cell in &cells[1..] {
            let plan = cell.plan(Scale::Full);
            assert_eq!(plan.scale.rows, first.scale.rows);
            assert_eq!(plan.scale.row_bytes, first.scale.row_bytes);
            assert_eq!(plan.scale.keys, first.scale.keys);
            assert_eq!(plan.scale.concurrency, first.scale.concurrency);
            assert_eq!(plan.warmup, first.warmup);
            // and the read share is the one thing that moved
            assert_ne!(plan.scale.read_pct, first.scale.read_pct);
        }
    }

    /// Every arm records what it was, so a reader is never parsing the identifier back
    #[test]
    fn every_arm_records_its_own_axes() {
        for arm in Grid::all() {
            let plan = arm.plan(Scale::Full);
            assert_eq!(plan.scale.read_pct, Some(arm.read_pct));
            assert_eq!(plan.scale.table_kind, Some(arm.table.artifact_name()));
            assert_eq!(plan.scale.distribution, arm.distribution.artifact_name());
            assert_eq!(plan.scale.row_profile, arm.rows.artifact_name());
            assert_eq!(plan.scale.concurrency, arm.depth);
            // a mixture's recorded width is the mean of what it writes
            assert_eq!(plan.scale.row_bytes, arm.rows.mean());
        }
    }

    /// The mixture a read share asks for is the mixture it gets
    ///
    /// A share that drifted would make an `r70` arm a different measurement than it says it is,
    /// and the artifact would report the intent rather than what happened.
    #[test]
    fn a_read_share_is_the_share_that_is_issued() {
        for share in MIXES {
            let reads = (0..20_000u64)
                .filter(|index| is_read(42, *index, share))
                .count();
            let observed = reads * 100 / 20_000;
            assert!(
                observed.abs_diff(share as usize) <= 2,
                "asked for {share}% reads and issued {observed}%"
            );
        }
    }

    /// The two ends of the mixture are exact rather than nearly exact
    ///
    /// An `r100` arm that issued one write in twenty thousand would put a write's service time in
    /// its read distribution, and at a four megabyte row that single sample is the p99.
    #[test]
    fn the_ends_of_the_mixture_are_absolute() {
        for index in 0..50_000u64 {
            assert!(!is_read(42, index, 0), "an r0 arm issued a read");
            assert!(is_read(42, index, 100), "an r100 arm issued a write");
        }
    }

    /// Every arm measures enough queries for a median and few enough to finish
    #[test]
    fn every_arm_is_bounded_at_both_ends() {
        for arm in Grid::all() {
            for scale in [Scale::Smoke, Scale::Full] {
                let queries = queries_for(arm.rows.mean(), scale);
                assert!(queries >= 20, "{} runs {queries} queries", arm.id());
                assert!(queries <= 20_000, "{} runs {queries} queries", arm.id());
                // and the warmup is a share of the run rather than a share of nothing
                let plan = arm.plan(scale);
                assert!(plan.warmup < queries, "{} warms up its whole run", arm.id());
            }
        }
    }

    /// Every arm seeds rows a read can find
    #[test]
    fn every_arm_seeds_a_key_space() {
        for arm in Grid::all() {
            for scale in [Scale::Smoke, Scale::Full] {
                let rows = rows_for(arm.rows.mean(), scale);
                assert!(rows >= super::MIN_ROWS, "{} seeds {rows} rows", arm.id());
            }
        }
    }

    /// A seed bundle fits inside a frame at every width the grid writes
    ///
    /// A bundle over the frame bound is refused outright, which is a workload that cannot run at
    /// all rather than one that runs slowly.
    #[test]
    fn a_seed_bundle_fits_in_a_frame_at_every_width() {
        let frame = u64::from(shoal::shared::protocol::DEFAULT_MAX_FRAME_BYTES);
        for profile in every_width() {
            let widest = profile.widest();
            let bundle = seed_batch(widest, frame) as u64 * widest;
            assert!(
                bundle * super::SEED_FRAME_SHARE <= frame,
                "{} byte rows bundle to {bundle} bytes",
                widest
            );
        }
    }

    /// A bundle fits inside a frame the configuration sweep narrowed, too
    ///
    /// The reason [`seed_batch`] takes the bound rather than reading the constant. Sized against
    /// the constant, the reference width would bundle a hundred rows regardless, and the arm that
    /// narrows the bound to a megabyte would be seeding into a server that refuses its frames.
    #[test]
    fn a_seed_bundle_fits_inside_a_narrowed_frame() {
        for frame in [1u64 << 20, 8 << 20, u64::from(shoal::shared::protocol::DEFAULT_MAX_FRAME_BYTES)] {
            for profile in every_width() {
                let widest = profile.widest();
                let bundle = seed_batch(widest, frame) as u64 * widest;
                assert!(
                    bundle * super::SEED_FRAME_SHARE <= frame || bundle == widest,
                    "{widest} byte rows bundle to {bundle} bytes under a {frame} byte frame"
                );
            }
        }
    }

    /// Nothing in the family opts into the hotpath layer
    #[test]
    fn no_arm_asks_to_be_profiled() {
        assert!(Grid::all().iter().all(|arm| !arm.profiles()));
    }

    /// Exactly the three declared arms opt into the stage layer, and all three exist
    ///
    /// The second half is the one worth having. [`STAGED_ARMS`] is a list of identifier strings, so
    /// a width renamed or a sweep reordered would leave it naming an arm nobody mints - and the
    /// stage layer would quietly profile two widths instead of three.
    #[test]
    fn the_staged_arms_are_the_three_declared_ones() {
        let all = Grid::all();
        let staged: Vec<&str> = all
            .iter()
            .filter(|arm| arm.stage_profiles())
            .map(|arm| arm.id())
            .collect();
        assert_eq!(staged, STAGED_ARMS, "the stage layer's width axis moved");
    }

    /// The two width arrays are one axis rather than two overlapping ones
    ///
    /// They are declared apart only so that ports do not move. A width in both would be minted
    /// twice at the reference mixture, which is the collision `every_arm_is_minted_exactly_once`
    /// catches - this says which array to look in when it fires.
    #[test]
    fn the_width_arrays_do_not_overlap() {
        let mut widths: Vec<u64> = every_width().map(|profile| profile.mean()).collect();
        let before = widths.len();
        widths.sort_unstable();
        widths.dedup();
        assert_eq!(before, widths.len(), "a width is declared in both arrays");
    }

    /// The width ladder at depth one crosses the depth ladder at the reference width
    ///
    /// The two ladders share `macro/grid/depth/1`, and sharing it is what lets either be read
    /// against the other: without a common point they are two curves with no origin in common.
    #[test]
    fn the_two_ladders_cross_at_one_arm() {
        let all = Grid::all();
        let shared = all
            .iter()
            .find(|arm| arm.sweep == Sweep::Depth && arm.depth == 1)
            .expect("the ladder rung at one query outstanding");
        assert_eq!(shared.rows, REFERENCE_WIDTH);
        // and no rung of the width ladder claims that cell a second time
        assert!(
            all.iter()
                .filter(|arm| arm.sweep == Sweep::WidthDepth)
                .all(|arm| arm.rows != REFERENCE_WIDTH),
            "the width ladder minted the reference width the depth ladder already holds"
        );
        for scale in [Scale::Smoke, Scale::Full] {
            for arm in all.iter().filter(|arm| arm.sweep == Sweep::WidthDepth) {
                // every rung of the width ladder differs from the shared cell in the width alone
                let plan = arm.plan(scale);
                assert_eq!(plan.scale.concurrency, 1, "{}", arm.id());
                assert_eq!(plan.scale.read_pct, Some(REFERENCE_MIX), "{}", arm.id());
                assert_eq!(plan.scale.table_kind, shared.plan(scale).scale.table_kind);
            }
        }
    }

    /// The width axis is swept at all three mixtures, on every table
    ///
    /// The reference sweep measures a write path effect under a mixture that is half reads. This
    /// asserts the other two ends exist to compare it against, and that neither is missing a table
    /// - a sweep short of one table is a pair that cannot be subtracted.
    #[test]
    fn the_width_axis_is_swept_at_every_declared_mixture() {
        let all = Grid::all();
        for mix in [REFERENCE_MIX, WIDE_MIXES[0], WIDE_MIXES[1]] {
            for table in TABLES {
                let widths = all
                    .iter()
                    .filter(|arm| {
                        arm.sweep == Sweep::Cell && arm.read_pct == mix && arm.table == table
                    })
                    .count();
                assert_eq!(
                    widths,
                    every_width().count(),
                    "r{mix} on {} is short a width",
                    table.as_str()
                );
            }
        }
    }

    /// Every arm is timed per query, since every one of them reports a latency
    #[test]
    fn every_arm_is_timed_per_query() {
        assert!(
            Grid::all()
                .iter()
                .all(|arm| arm.timing() == Timing::PerQuery)
        );
    }
}
