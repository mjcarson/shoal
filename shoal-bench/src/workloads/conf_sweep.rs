//! `macro/conf/...` - what one setting of the configuration is worth
//!
//! # The question nothing could answer
//!
//! `shoal.yml` carries a dozen tuning knobs and `docs/src/getting-started/configuration.md`
//! describes every one of them. It describes what each setting *is*. It has never said what value
//! to pick, because nothing measured one: every capture in this repository ran against the same
//! committed configuration, so the configuration was a constant rather than an axis.
//!
//! That leaves two holes. A caller deploying Shoal has no basis for choosing a value beyond the
//! defaults somebody once wrote down. And a bottleneck that lives in a *setting* rather than in the
//! code is invisible to every other layer of this harness - `docs/src/appendix/todos.md` has been
//! asking for an `Async` against `Fsync` capture since [F8], calling it "the cheapest remaining item
//! on this page", and it was never run because there was nowhere to put the answer.
//!
//! # An arm is the reference cell with one field moved
//!
//! Every arm here is [`grid`](super::grid)'s reference cell - the persistent unsorted table, a 1 KiB
//! row, an even read/write mixture, thirty two outstanding queries - with exactly one field of the
//! server configuration changed. That is deliberate and it is the whole design:
//!
//! - **It reuses the grid's driver rather than a second one.** Two drivers measuring "the same
//!   thing under two configurations" would differ in the driver as well as the configuration, and
//!   nothing would be attributable to either.
//! - **It makes an arm comparable to `macro/grid/unsorted/r50/1024`.** The grid cell is the same
//!   measurement against the committed `shoal.yml`, so the sweep is anchored to a number that
//!   already appears on another page.
//! - **One field, never two.** The [F9](../../../docs/src/features/ephemeral-tables.md) control-pair
//!   shape applied to the configuration: a pair of arms differs in one setting, so the difference
//!   between them is what that setting costs. A test below pins it.
//!
//! # Every sweep brackets the shipped default
//!
//! Each knob's value list contains the value the committed `shoal.yml` actually resolves to -
//! `durability/r50/fsync`, `latency_buffer/r50/4Ki`, `shards/r50/12`. That arm is *measured* rather
//! than borrowed from the grid cell it duplicates, because it has its own identifier, its own
//! storage subdirectory and its own port, and a sweep read against an arm that ran somewhere else
//! is not a sweep. A test asserts the bracketing, so the day somebody retunes `shoal.yml` the sweep
//! fails rather than quietly stopping short of the value in use.
//!
//! # What this cannot see
//!
//! One knob moves at a time, against a fixed reference of every other. That is the same cross, not
//! cube argument [`grid`](super::grid) makes, and it has the same consequence: **an interaction
//! between two settings is invisible here.** A write-behind depth that only pays off at a large
//! buffer would show as two flat sweeps. Recorded in `docs/src/appendix/todos.md` rather than
//! papered over.
//!
//! [F8]: ../../../docs/src/features/purpose-built-workloads.md

use shoal::server::tables::storage::fs::conf::Durability;

use crate::workloads::grid::{DEPTH, Grid, REFERENCE_MIX, REFERENCE_WIDTH, Sweep, Table};
use crate::workloads::harness::conf::binary_size;
use crate::workloads::harness::keys::KeyDistribution;
use crate::workloads::harness::rows::RowProfile;
use crate::workloads::workload::ConfOverrides;

/// The mixture a sweep runs at when one mixture is enough
///
/// The grid's reference share, so an arm is comparable to `macro/grid/unsorted/r50/1024` without
/// anything having to be adjusted for.
const AT_REFERENCE: &[u32] = &[REFERENCE_MIX];

/// The mixtures the resource sweeps run at
///
/// The reference share and a pure read share. Cores and memory are the two knobs whose effect
/// plausibly differs between the two halves of a mixture - more shards is more parallelism for
/// reads and more fsync contention for writes, and a memory limit only bites on the read path -
/// so sweeping them at a mixture alone would blend the two effects the sweep exists to separate.
/// The writer knobs get no read-heavy repeat because at `r100` they are not on the path at all.
const AT_BOTH_ENDS: &[u32] = &[REFERENCE_MIX, 100];

/// Which half of the configuration a knob belongs to
///
/// This is a segment of every arm's identifier rather than something derived from it, so that
/// `groups.rs` and `render/family.rs` - neither of which can link the engine or see this enum -
/// can both split the sweep with a prefix check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Section {
    /// The filesystem writer settings: how a write is buffered, queued and made durable
    Storage,
    /// What the server is given to run on: cores, memory, and the frame bound
    Resources,
}

impl Section {
    /// The lowercase name of this section, which is the identifier segment it contributes
    pub fn as_str(&self) -> &'static str {
        // these strings are inside a workload identifier, so they are a join key and not cosmetic
        match self {
            Section::Storage => "storage",
            Section::Resources => "resources",
        }
    }
}

/// One value of one setting, and how it reaches the server
///
/// An enum rather than a function pointer so that the sweep table below is a plain `const` a test
/// can walk, and so that a value carries its own type - a memory limit is written the way
/// `shoal.yml` writes it and a write-behind depth is a count, and neither should be spellable as
/// the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Setting {
    /// Which barrier a write waits on before its response is released
    Durability(Durability),
    /// How many bytes the intent log buffers before it flushes
    LatencyBuffer(usize),
    /// How many intent log writes may be in flight at once
    LatencyWriteBehind(usize),
    /// How large the intent log may grow before compaction is due
    IntentLog(u64),
    /// How many bytes the throughput sensitive writer buffers before it flushes
    ThroughputBuffer(usize),
    /// How many throughput sensitive writes may be in flight at once
    ThroughputWriteBehind(usize),
    /// How many shards the server runs
    Shards(usize),
    /// The memory limit each shard is held to, written the way `shoal.yml` writes it
    Memory(&'static str),
    /// The largest frame the server will accept, in bytes
    Frame(u32),
}

impl Setting {
    /// Which knob this value belongs to
    ///
    /// The identifier segment naming the sweep, so every value of one sweep answers the same
    /// string. A test asserts that against the table below.
    pub fn knob(&self) -> &'static str {
        // one name per variant, and the name is what the artifact is keyed under
        match self {
            Setting::Durability(_) => "durability",
            Setting::LatencyBuffer(_) => "latency_buffer",
            Setting::LatencyWriteBehind(_) => "latency_write_behind",
            Setting::IntentLog(_) => "intent_log",
            Setting::ThroughputBuffer(_) => "throughput_buffer",
            Setting::ThroughputWriteBehind(_) => "throughput_write_behind",
            Setting::Shards(_) => "shards",
            Setting::Memory(_) => "memory",
            Setting::Frame(_) => "frame",
        }
    }

    /// How this value is written in an identifier and read in a table
    ///
    /// A byte count goes through [`binary_size`] so that an arm reads as `64Ki` rather than as
    /// `65536`, which is the same thing [`ConfFacts`](crate::model::macro_layer::ConfFacts) does
    /// with the memory limit and for the same reason.
    pub fn label(&self) -> String {
        // counts stay counts, sizes are rendered in the units they were written in
        match self {
            Setting::Durability(Durability::Fsync) => "fsync".to_string(),
            Setting::Durability(Durability::Async) => "async".to_string(),
            Setting::LatencyBuffer(bytes) => binary_size(*bytes as u64),
            Setting::LatencyWriteBehind(count) => count.to_string(),
            Setting::IntentLog(bytes) => binary_size(*bytes),
            Setting::ThroughputBuffer(bytes) => binary_size(*bytes as u64),
            Setting::ThroughputWriteBehind(count) => count.to_string(),
            Setting::Shards(count) => count.to_string(),
            Setting::Memory(size) => (*size).to_string(),
            Setting::Frame(bytes) => binary_size(u64::from(*bytes)),
        }
    }

    /// Writes this value into a set of overrides
    ///
    /// Exactly one field, always. The test below asserts that against a defaulted set, because a
    /// value that touched two fields would make its arm's difference from the base unattributable
    /// to either of them.
    ///
    /// # Arguments
    ///
    /// * `overrides` - The overrides to write into
    pub fn apply(&self, overrides: &mut ConfOverrides) {
        // one arm, one field
        match self {
            Setting::Durability(durability) => overrides.durability = Some(*durability),
            Setting::LatencyBuffer(bytes) => overrides.latency_buffer_size = Some(*bytes),
            Setting::LatencyWriteBehind(count) => overrides.latency_write_behind = Some(*count),
            Setting::IntentLog(bytes) => overrides.intent_log_size = Some(*bytes),
            Setting::ThroughputBuffer(bytes) => overrides.throughput_buffer_size = Some(*bytes),
            Setting::ThroughputWriteBehind(count) => {
                overrides.throughput_write_behind = Some(*count)
            }
            Setting::Shards(count) => overrides.shards = Some(*count),
            Setting::Memory(size) => overrides.memory = Some((*size).to_string()),
            Setting::Frame(bytes) => overrides.max_frame_bytes = Some(*bytes),
        }
    }
}

/// One knob, and every value of it the capture measures
pub struct KnobSweep {
    /// The identifier segment naming this sweep
    pub knob: &'static str,
    /// One line saying what moving this knob is expected to change
    pub summary: &'static str,
    /// Which half of the configuration it belongs to
    pub section: Section,
    /// Which read shares it is swept at
    pub mixes: &'static [u32],
    /// The values it takes, in the order they are minted and therefore ported
    pub values: &'static [Setting],
}

/// Every configuration sweep, in the order a capture runs them
///
/// **Append, never interleave.** An arm's position here decides the port it binds, the same rule
/// [`crate::workload_ids::IDS`] states, and inserting a value into the middle of a sweep moves every
/// arm after it onto a different port.
pub const SWEEPS: &[KnobSweep] = &[
    KnobSweep {
        knob: "durability",
        summary: "what the fdatasync barrier on the write path costs",
        section: Section::Storage,
        mixes: AT_REFERENCE,
        values: &[
            Setting::Durability(Durability::Fsync),
            Setting::Durability(Durability::Async),
        ],
    },
    KnobSweep {
        knob: "latency_buffer",
        summary: "how much the intent log's write size matters, against the device's alignment",
        section: Section::Storage,
        mixes: AT_REFERENCE,
        values: &[
            Setting::LatencyBuffer(512),
            Setting::LatencyBuffer(4 * 1024),
            Setting::LatencyBuffer(16 * 1024),
            Setting::LatencyBuffer(64 * 1024),
            Setting::LatencyBuffer(256 * 1024),
        ],
    },
    KnobSweep {
        knob: "latency_write_behind",
        summary: "how deep the write path's io_uring queue has to be before it stops stalling",
        section: Section::Storage,
        mixes: AT_REFERENCE,
        values: &[
            Setting::LatencyWriteBehind(1),
            Setting::LatencyWriteBehind(8),
            Setting::LatencyWriteBehind(32),
            Setting::LatencyWriteBehind(128),
            Setting::LatencyWriteBehind(512),
        ],
    },
    KnobSweep {
        knob: "intent_log",
        summary: "how often compaction runs, traded against how much the log holds",
        section: Section::Storage,
        mixes: AT_REFERENCE,
        values: &[
            Setting::IntentLog(1 << 20),
            Setting::IntentLog(10 << 20),
            Setting::IntentLog(100 << 20),
            Setting::IntentLog(1 << 30),
        ],
    },
    KnobSweep {
        knob: "throughput_buffer",
        summary: "what the archive writer's buffer size is worth, which is item 71's evidence",
        section: Section::Storage,
        mixes: AT_REFERENCE,
        values: &[
            Setting::ThroughputBuffer(32 * 1024),
            Setting::ThroughputBuffer(128 * 1024),
            Setting::ThroughputBuffer(512 * 1024),
            Setting::ThroughputBuffer(1024 * 1024),
        ],
    },
    KnobSweep {
        knob: "throughput_write_behind",
        summary: "what the archive writer's queue depth is worth, which is item 71's evidence",
        section: Section::Storage,
        mixes: AT_REFERENCE,
        values: &[
            Setting::ThroughputWriteBehind(1),
            Setting::ThroughputWriteBehind(4),
            Setting::ThroughputWriteBehind(16),
        ],
    },
    KnobSweep {
        knob: "shards",
        summary: "how the server scales with the cores it is given",
        section: Section::Resources,
        mixes: AT_BOTH_ENDS,
        values: &[
            Setting::Shards(1),
            Setting::Shards(2),
            Setting::Shards(4),
            Setting::Shards(8),
            Setting::Shards(12),
        ],
    },
    KnobSweep {
        knob: "memory",
        // the rungs are chosen against the reference cell's actual working set rather than against
        // round numbers, and the difference matters: `resources.memory` is a **per shard** budget,
        // and the reference cell seeds `rows_for(1024, Full)` = 20,000 rows of a kilobyte, which is
        // 19.5 MiB over twelve shards - about 1.6 MiB each, plus roughly half as much again from
        // the writes the run itself issues. So the interesting region is somewhere around two or
        // three mebibytes a shard, and a sweep whose smallest rung was 64Mi would be thirty-eight
        // times the working set at its *tightest* point: ten arms, all on the flat, measuring the
        // same thing. `1Mi` and `4Mi` are what put the cliff inside the sweep.
        summary: "where the working set stops fitting and reads start reaching disk",
        section: Section::Resources,
        mixes: AT_BOTH_ENDS,
        values: &[
            Setting::Memory("1Mi"),
            Setting::Memory("4Mi"),
            Setting::Memory("16Mi"),
            Setting::Memory("64Mi"),
            Setting::Memory("1Gi"),
            Setting::Memory("4Gi"),
        ],
    },
    KnobSweep {
        knob: "frame",
        summary: "what bounding the largest acceptable frame costs a batching client",
        section: Section::Resources,
        mixes: AT_REFERENCE,
        values: &[
            Setting::Frame(1 << 20),
            Setting::Frame(8 << 20),
            Setting::Frame(64 << 20),
        ],
    },
];

/// One knob repeated at a row width other than the reference one
///
/// A sweep runs at the grid's reference cell, which is 1 KiB rows. For most knobs that is the right
/// place to ask the question. For `latency_buffer` it is the one width whose answer is no: the
/// intent log stages records into a 4096 byte buffer and flushes when the next record will not fit,
/// so at 1 KiB three records already share an aligned write and the sweep is measuring the flat side
/// of a step. `StreamWriter::prep` stops batching *entirely* once a record exceeds the buffer, which
/// is where the whole effect lives and where the reference cell never goes.
///
/// So the same rungs are run again above the buffer. Nothing else about an arm changes.
pub struct WideRepeat {
    /// Which sweep is being repeated, named the same way [`KnobSweep::knob`] names it
    pub knob: &'static str,
    /// The widths to repeat it at, none of which may be the reference width
    pub widths: &'static [RowProfile],
}

/// Every knob repeated above the reference width, in the order the repeats are minted
///
/// A second table rather than a `widths` field on [`KnobSweep`], and the reason is ports: `all()`
/// mints this table in a pass of its own after the whole of [`SWEEPS`], so every arm that existed
/// before these do keeps the port it has always had. Folding the widths into the sweep would have
/// interleaved ten arms into the middle of the storage half.
pub const WIDE_REPEATS: &[WideRepeat] = &[WideRepeat {
    knob: "latency_buffer",
    // the first width in the grid above the 4096 byte buffer, and one well above it. two points
    // rather than one because a single width above the step says the step exists and not whether
    // the setting still does anything once a record is far larger than any value of it
    widths: &[RowProfile::Fixed(8 * 1024), RowProfile::Fixed(64 * 1024)],
}];

/// The sweep a knob names
///
/// Panics rather than returning an option, because [`WIDE_REPEATS`] naming a knob that no sweep
/// declares is a table that has drifted from the one beside it rather than a condition to handle.
/// A test walks the same path, so it fails there before it can panic in a capture.
///
/// # Arguments
///
/// * `knob` - The identifier segment naming the sweep
fn sweep_of(knob: &str) -> &'static KnobSweep {
    SWEEPS
        .iter()
        .find(|sweep| sweep.knob == knob)
        .expect("a width repeat names a sweep that does not exist")
}

/// Every arm of every configuration sweep
///
/// **Sweep outermost, then mixture, then value**, and then the width repeats in a pass of their
/// own. That order is the order [`crate::workload_ids::IDS`] declares them and therefore the order
/// their ports are assigned in, so neither may be reshuffled to read better.
pub fn all() -> Vec<Grid> {
    // one arm per (sweep, mixture, value), which is the whole table flattened
    let mut built = Vec::with_capacity(
        SWEEPS
            .iter()
            .map(|sweep| sweep.mixes.len() * sweep.values.len())
            .sum::<usize>()
            + WIDE_REPEATS
                .iter()
                .map(|repeat| repeat.widths.len() * sweep_of(repeat.knob).mixes.len()
                    * sweep_of(repeat.knob).values.len())
                .sum::<usize>(),
    );
    for sweep in SWEEPS {
        for mix in sweep.mixes {
            for value in sweep.values {
                built.push(arm(sweep, *value, *mix, REFERENCE_WIDTH));
            }
        }
    }
    // and then the same rungs again at the widths a knob's effect actually lives at, minted last so
    // that no arm above keeps a different port than it had. see `WIDE_REPEATS`
    for repeat in WIDE_REPEATS {
        let sweep = sweep_of(repeat.knob);
        for width in repeat.widths {
            for mix in sweep.mixes {
                for value in sweep.values {
                    built.push(arm(sweep, *value, *mix, *width));
                }
            }
        }
    }
    built
}

/// Builds one arm of one sweep
///
/// # Arguments
///
/// * `sweep` - The knob being swept
/// * `value` - The value this arm sets it to
/// * `read_pct` - What share of this arm's queries are reads
/// * `rows` - How wide this arm's rows are, which is the reference width unless it is a repeat
fn arm(sweep: &'static KnobSweep, value: Setting, read_pct: u32, rows: RowProfile) -> Grid {
    let label = value.label();
    // the width is in the identifier only when it is not the reference one. that asymmetry is
    // deliberate: an identifier is the join key of every comparison, so adding a segment to the
    // forty eight arms that already exist would orphan every capture taken before this
    let width = if rows == REFERENCE_WIDTH {
        String::new()
    } else {
        format!("w{}/", rows.segment())
    };
    // the section is a segment rather than something to be derived, so that the two halves of the
    // sweep are separable by anything holding only the identifier
    let id: &'static str = Box::leak(
        format!(
            "macro/conf/{}/{}/r{read_pct}/{width}{label}",
            sweep.section.as_str(),
            sweep.knob
        )
        .into_boxed_str(),
    );
    let summary: &'static str = Box::leak(
        if rows == REFERENCE_WIDTH {
            format!("the reference mixture with {} set to {label}", sweep.knob)
        } else {
            format!(
                "the reference mixture at {} rows with {} set to {label}",
                crate::fmt::bytes(rows.mean()),
                sweep.knob
            )
        }
        .into_boxed_str(),
    );
    // exactly one field of the base configuration moves, which is what the arm is about
    let mut conf = ConfOverrides::default();
    value.apply(&mut conf);
    Grid {
        sweep: Sweep::Conf { knob: sweep.knob },
        // the persistent unsorted table, which is the table the reference cell drives and the one
        // the filesystem writers are actually exercised through
        table: Table::Unsorted,
        read_pct,
        rows,
        distribution: KeyDistribution::Uniform,
        depth: DEPTH,
        conf,
        id,
        summary,
    }
}

#[cfg(test)]
mod tests {
    use super::{SWEEPS, Section, Setting, WIDE_REPEATS, all, sweep_of};
    use crate::workloads::grid::{DEPTH, REFERENCE_MIX, REFERENCE_WIDTH, Sweep, Table};
    use crate::workloads::harness::keys::KeyDistribution;
    use crate::workloads::harness::rows::RowProfile;
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::workload::{ConfOverrides, Workload};

    /// How many fields of a set of overrides differ from the base
    ///
    /// # Arguments
    ///
    /// * `overrides` - The overrides to count against a defaulted set
    fn fields_moved(overrides: &ConfOverrides) -> usize {
        let base = ConfOverrides::default();
        // one term per field, because a struct cannot be walked and a field added without a term
        // here would silently stop being checked
        usize::from(overrides.shards != base.shards)
            + usize::from(overrides.memory != base.memory)
            + usize::from(overrides.tls != base.tls)
            + usize::from(overrides.durability != base.durability)
            + usize::from(overrides.latency_buffer_size != base.latency_buffer_size)
            + usize::from(overrides.latency_write_behind != base.latency_write_behind)
            + usize::from(overrides.intent_log_size != base.intent_log_size)
            + usize::from(overrides.throughput_buffer_size != base.throughput_buffer_size)
            + usize::from(overrides.throughput_write_behind != base.throughput_write_behind)
            + usize::from(overrides.max_frame_bytes != base.max_frame_bytes)
    }

    /// Every arm is minted once, and the capture's cost is what it says it is
    #[test]
    fn every_arm_is_minted_exactly_once() {
        let arms = all();
        let expected: usize = SWEEPS
            .iter()
            .map(|sweep| sweep.mixes.len() * sweep.values.len())
            .sum::<usize>()
            + WIDE_REPEATS
                .iter()
                .map(|repeat| {
                    let sweep = sweep_of(repeat.knob);
                    repeat.widths.len() * sweep.mixes.len() * sweep.values.len()
                })
                .sum::<usize>();
        assert_eq!(arms.len(), expected);
        assert_eq!(arms.len(), 58, "the capture's cost changed");
        let mut ids: Vec<&str> = arms.iter().map(|arm| arm.id()).collect();
        ids.sort_unstable();
        let before = ids.len();
        ids.dedup();
        assert_eq!(before, ids.len(), "a configuration arm is minted twice");
    }

    /// Every value of a sweep belongs to the knob the sweep declares
    ///
    /// The table names the knob once and each value names it again. Without this they could drift,
    /// and an arm would be filed under a sweep it does not belong to.
    #[test]
    fn every_value_names_its_own_knob() {
        for sweep in SWEEPS {
            for value in sweep.values {
                assert_eq!(value.knob(), sweep.knob, "{value:?}");
            }
        }
    }

    /// An arm moves exactly one field of the configuration, and never two
    ///
    /// This is the whole basis for attributing a difference to a setting. Two arms of one sweep
    /// differ in one field; two arms of different sweeps differ in two, and are never compared.
    #[test]
    fn an_arm_moves_one_field() {
        for arm in all() {
            assert_eq!(
                fields_moved(&arm.conf),
                1,
                "{} moves {} fields",
                arm.id(),
                fields_moved(&arm.conf)
            );
        }
    }

    /// Every arm holds the reference cell still in every respect but its own knob and its mixture
    ///
    /// The reason an arm is comparable to `macro/grid/unsorted/r50/1024` at all.
    #[test]
    fn an_arm_is_the_reference_cell_with_one_setting_moved() {
        // the widths a repeat is allowed to hold instead of the reference one, which is the only
        // respect in which an arm may differ from the cell beyond its own knob and its mixture
        let repeated: Vec<RowProfile> = WIDE_REPEATS
            .iter()
            .flat_map(|repeat| repeat.widths.iter().copied())
            .collect();
        for arm in all() {
            assert_eq!(arm.table, Table::Unsorted, "{}", arm.id());
            assert!(
                arm.rows == REFERENCE_WIDTH || repeated.contains(&arm.rows),
                "{} runs at a width nothing declared",
                arm.id()
            );
            assert_eq!(arm.distribution, KeyDistribution::Uniform, "{}", arm.id());
            assert_eq!(arm.depth, DEPTH, "{}", arm.id());
            assert!(matches!(arm.sweep, Sweep::Conf { .. }), "{}", arm.id());
        }
    }

    /// A width repeat names a sweep that exists, and never the reference width
    ///
    /// Two ways the two tables drift apart. A knob nobody declares would panic inside `all()`
    /// during a capture rather than here; a repeat at the reference width would mint an identifier
    /// the sweep proper already holds, which is a collision and not a second measurement.
    #[test]
    fn a_width_repeat_names_a_real_sweep_at_a_new_width() {
        for repeat in WIDE_REPEATS {
            let sweep = sweep_of(repeat.knob);
            assert_eq!(sweep.knob, repeat.knob);
            assert!(!repeat.widths.is_empty(), "{} repeats at no width", repeat.knob);
            for width in repeat.widths {
                assert_ne!(
                    *width, REFERENCE_WIDTH,
                    "{} is repeated at the width it already runs at",
                    repeat.knob
                );
            }
        }
    }

    /// A repeat runs every rung its sweep runs, so the two are the same ladder at two widths
    ///
    /// The point of the repeat is a comparison between one width and another at every value of the
    /// knob. A repeat short of a rung would be a ladder with a missing step, and the two widths
    /// would only be comparable where they happened to overlap.
    #[test]
    fn a_repeat_runs_every_rung_its_sweep_does() {
        let arms = all();
        for repeat in WIDE_REPEATS {
            let sweep = sweep_of(repeat.knob);
            for width in repeat.widths {
                for mix in sweep.mixes {
                    for value in sweep.values {
                        let expected = format!(
                            "macro/conf/{}/{}/r{mix}/w{}/{}",
                            sweep.section.as_str(),
                            sweep.knob,
                            width.segment(),
                            value.label()
                        );
                        assert!(
                            arms.iter().any(|arm| arm.id() == expected),
                            "{expected} was never minted"
                        );
                    }
                }
            }
        }
    }

    /// An identifier names its section, its knob, its mixture and its value
    ///
    /// The section is in there so that anything holding only the identifier - the group table and
    /// the page families, neither of which can see this module - can split the sweep in two.
    #[test]
    fn an_id_names_its_section_and_its_value() {
        for sweep in SWEEPS {
            for mix in sweep.mixes {
                for value in sweep.values {
                    let expected = format!(
                        "macro/conf/{}/{}/r{mix}/{}",
                        sweep.section.as_str(),
                        sweep.knob,
                        value.label()
                    );
                    assert!(
                        all().iter().any(|arm| arm.id() == expected),
                        "{expected} was never minted"
                    );
                }
            }
        }
    }

    /// Both sections are swept, so neither half of the configuration is unmeasured
    #[test]
    fn both_sections_are_swept() {
        for section in [Section::Storage, Section::Resources] {
            assert!(
                SWEEPS.iter().any(|sweep| sweep.section == section),
                "{section:?} has no sweep"
            );
        }
    }

    /// Every sweep brackets the value the committed `shoal.yml` resolves to
    ///
    /// Without this a sweep can quietly stop covering the configuration everything else in the
    /// harness is measured under, and the page would recommend a value against a reference that is
    /// not on the chart. Retuning `shoal.yml` is supposed to fail this test.
    #[test]
    fn every_sweep_covers_the_shipped_default() {
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("shoal-bench has a parent")
            .join("shoal.yml");
        let conf = crate::workloads::harness::conf::resolve(
            &base,
            "macro/conf/test",
            &ConfOverrides::default(),
            12_000,
        )
        .expect("the committed config resolves");
        let filesystem = &conf.storage.default.filesystem;
        // what the committed file actually says, per knob, in the same spelling an arm uses
        let shipped = |knob: &str| -> String {
            match knob {
                "durability" => match filesystem.latency_sensitive.durability {
                    shoal::server::tables::storage::fs::conf::Durability::Fsync => "fsync".into(),
                    shoal::server::tables::storage::fs::conf::Durability::Async => "async".into(),
                },
                "latency_buffer" => {
                    Setting::LatencyBuffer(filesystem.latency_sensitive.buffer_size).label()
                }
                "latency_write_behind" => {
                    filesystem.latency_sensitive.write_behind.to_string()
                }
                "intent_log" => {
                    Setting::IntentLog(filesystem.latency_sensitive.intent_log_size).label()
                }
                "throughput_buffer" => {
                    Setting::ThroughputBuffer(filesystem.throughput_sensitive.buffer_size).label()
                }
                "throughput_write_behind" => {
                    filesystem.throughput_sensitive.write_behind.to_string()
                }
                "shards" => conf.resources.cores.expect("shoal.yml pins a core count").to_string(),
                "memory" => crate::workloads::harness::conf::binary_size(
                    conf.resources.memory as u64,
                ),
                "frame" => Setting::Frame(conf.networking.max_frame_bytes).label(),
                other => panic!("no shipped value is known for {other}"),
            }
        };
        for sweep in SWEEPS {
            let want = shipped(sweep.knob);
            let covered: Vec<String> = sweep.values.iter().map(Setting::label).collect();
            assert!(
                covered.contains(&want),
                "the {} sweep covers {covered:?} and shoal.yml is set to {want}",
                sweep.knob
            );
        }
    }

    /// Every arm plans a server, since a configuration arm is about the server
    #[test]
    fn every_arm_asks_for_a_configured_server() {
        for arm in all() {
            let plan = arm.plan(Scale::Full);
            let overrides = plan.server.overrides().expect("a configuration arm needs a server");
            assert_eq!(fields_moved(overrides), 1, "{}", arm.id());
        }
    }

    /// The reference mixture is swept by every knob, so every sweep crosses the grid's own cell
    #[test]
    fn every_sweep_runs_at_the_reference_mixture() {
        for sweep in SWEEPS {
            assert!(
                sweep.mixes.contains(&REFERENCE_MIX),
                "the {} sweep never runs at the reference mixture",
                sweep.knob
            );
        }
    }
}
