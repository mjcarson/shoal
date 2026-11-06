//! `macro/sort_key/{padded,u64}/{get,range,walk}/{resident,evicted}` - a sorted table's reads with its
//! sort key a padded string and with it an integer
//!
//! # What this is for
//!
//! O156 in `docs/src/appendix/optimizations.md`. A sort key could only be a `String` until Resolved
//! #207, so [`Event`] orders its rows by a number padded to twenty digits; an integer is eight bytes
//! inline in its archived row and one comparison, where a padded key is an archived string's header
//! and twenty bytes beside it, compared byte by byte at every step of a seek. These workloads read
//! the same rows both ways - [`Event`] and its twin [`NumberedEvent`], the same partitions,
//! positions, filters and payloads - so each pair differs in the sort key's type and nothing else.
//!
//! # The three reads
//!
//! - `get`: one partition, eight sort keys named, spread over it
//! - `range`: one partition, the 64 rows between two keys
//! - `walk`: one page of 64 rows after a cursor, as a paged walk of a partition asks one page at a time
//!
//! Each `resident`, every partition in memory, and `evicted`, the server restarted after seeding so
//! every read is of an archive, as [`crate::workloads::fanout`]'s arms are. A read loads its
//! partition, which stays resident after it, so an evicted arm seeds as many partitions as it reads
//! ([`EVICTED_READS`]) and the coprime stride gives every read, the warmup's too, a partition no read
//! before it loaded; with the resident arms' 64 the warmup alone loaded them all, and every read it
//! timed was resident (O156's first run on titan, 2026-11-05).

use std::ops::Bound;

use anyhow::Result;
use shoal::shared::queries::SortRange;

use crate::model::macro_layer::{ScaleFacts, Timing};
use crate::workloads::fanout::Residency;
use crate::workloads::harness::driver::{self, Batch};
use crate::workloads::harness::seed::{Scale, Seeded};
use crate::workloads::schema::{
    BenchClient, BenchQueryKinds, Event, EventGet, NumberedEvent, NumberedEventGet, sort_key,
};
use crate::workloads::workload::{
    BoxFuture, ConfOverrides, Context, Measurement, ServerNeed, Workload, WorkloadPlan,
};

/// How many partitions a full resident run seeds
const PARTITIONS: u64 = 64;

/// How many reads a full evicted run sends, its warmup's among them, and so how many partitions it
/// seeds: one a read, so that no read finds its partition loaded by one before it
const EVICTED_READS: u64 = 1_024;

/// How many rows each partition holds
const ROWS: u64 = 1_024;

/// How wide each row's payload is, in bytes: the fanout curve's, so the key is a visible share of a row
const ROW_BYTES: u64 = 256;

/// How many keys a get names
const NAMED: u64 = 8;

/// How many rows a range or a page reads
const SPAN: u64 = 64;

/// How many reads may be outstanding at once
const CONCURRENCY: u32 = 8;

/// The sort key's type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyForm {
    /// [`Event`]'s, a number padded to twenty digits
    Padded,
    /// [`NumberedEvent`]'s, the number itself
    Integer,
}

impl KeyForm {
    /// The name this form appears under in an identifier
    fn as_str(self) -> &'static str {
        match self {
            KeyForm::Padded => "padded",
            KeyForm::Integer => "u64",
        }
    }
}

/// What a read asks of a partition
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Eight sort keys named
    Get,
    /// The rows between two keys
    Range,
    /// One page after a cursor
    Walk,
}

impl Access {
    /// The name this read appears under in an identifier
    fn as_str(self) -> &'static str {
        match self {
            Access::Get => "get",
            Access::Range => "range",
            Access::Walk => "walk",
        }
    }
}

/// One arm: a key form, a read and a residency
pub struct SortKeyArm {
    /// The sort key's type
    pub form: KeyForm,
    /// What each read asks
    pub access: Access,
    /// Whether the partitions are in memory
    pub residency: Residency,
    /// This workload's identifier, built once because the trait hands back a `&'static str`
    pub id: &'static str,
}

impl SortKeyArm {
    /// Every arm, the key form innermost so the two forms of one read and residency are run one
    /// after the other in a capture, as their pair is read
    pub fn all() -> Vec<SortKeyArm> {
        let mut built = Vec::with_capacity(12);
        for residency in [Residency::Resident, Residency::Evicted] {
            for access in [Access::Get, Access::Range, Access::Walk] {
                for form in [KeyForm::Padded, KeyForm::Integer] {
                    // twelve short identifiers built once and read for the life of the process
                    let id: &'static str = Box::leak(
                        format!(
                            "macro/sort_key/{}/{}/{}",
                            form.as_str(),
                            access.as_str(),
                            residency_name(residency)
                        )
                        .into_boxed_str(),
                    );
                    built.push(SortKeyArm {
                        form,
                        access,
                        residency,
                        id,
                    });
                }
            }
        }
        built
    }

    /// How many partitions this arm seeds at a scale
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    /// * `residency` - Whether the partitions are in memory
    fn partitions(scale: Scale, residency: Residency) -> u64 {
        match residency {
            // a few partitions even at a smoke scale, every one of them whole
            Residency::Resident => scale.rows(PARTITIONS).max(4),
            // a partition a read, so every read loads one no read before it did
            Residency::Evicted => SortKeyArm::queries(scale, residency),
        }
    }

    /// How many reads this arm sends at a scale, its warmup's among them
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    /// * `residency` - Whether the partitions are in memory
    fn queries(scale: Scale, residency: Residency) -> u64 {
        match (residency, scale) {
            (Residency::Resident, Scale::Smoke) => 400,
            (Residency::Resident, Scale::Full) => 8_000,
            (Residency::Evicted, Scale::Smoke) => 64,
            (Residency::Evicted, Scale::Full) => EVICTED_READS,
        }
    }
}

/// The name a residency appears under in an identifier, as the fanout curve names it
///
/// # Arguments
///
/// * `residency` - The residency
fn residency_name(residency: Residency) -> &'static str {
    match residency {
        Residency::Resident => "resident",
        Residency::Evicted => "evicted",
    }
}

/// The partition, and the first position, a read at an index asks for
///
/// A coprime stride over the partitions, so consecutive reads do not walk one partition in order,
/// and a position that moves through the partition with the index; the same for both forms.
///
/// # Arguments
///
/// * `index` - The read's index
/// * `partitions` - How many partitions there are
/// * `access` - What the read asks
fn placed(index: u64, partitions: u64, access: Access) -> (u64, u64) {
    let partition = index.wrapping_mul(crate::workloads::keyed_get::stride_for(partitions)) % partitions;
    let first = match access {
        // eight keys from here, spread a stride apart
        Access::Get => index.wrapping_mul(131) % ROWS,
        // a span that fits in the partition
        Access::Range => index.wrapping_mul(97) % (ROWS - SPAN),
        // a page's first position, on a page boundary
        Access::Walk => (index % (ROWS / SPAN)) * SPAN,
    };
    (partition, first)
}

impl Workload for SortKeyArm {
    /// What this workload is called
    fn id(&self) -> &'static str {
        self.id
    }

    /// What path this workload isolates
    fn summary(&self) -> &'static str {
        match (self.form, self.access) {
            (KeyForm::Padded, Access::Get) => "eight named sort keys of a partition keyed by a padded string",
            (KeyForm::Integer, Access::Get) => "eight named sort keys of a partition keyed by an integer",
            (KeyForm::Padded, Access::Range) => "64 rows between two keys of a partition keyed by a padded string",
            (KeyForm::Integer, Access::Range) => "64 rows between two keys of a partition keyed by an integer",
            (KeyForm::Padded, Access::Walk) => "a page of 64 rows after a cursor, keyed by a padded string",
            (KeyForm::Integer, Access::Walk) => "a page of 64 rows after a cursor, keyed by an integer",
        }
    }

    /// How this workload's samples are taken
    fn timing(&self) -> Timing {
        // one read a slot, each stamped on its own
        Timing::PerQuery
    }

    /// Whether the instrumented layers may run this workload
    fn profiles(&self) -> bool {
        // twelve arms under two attribution layers would be twenty four instrumented runs
        false
    }

    /// What this workload needs before it can run
    ///
    /// # Arguments
    ///
    /// * `scale` - How large a run was asked for
    fn plan(&self, scale: Scale) -> WorkloadPlan {
        let partitions = SortKeyArm::partitions(scale, self.residency);
        // one shard, so a partition's placement is fixed, as the fanout curve's is
        let overrides = ConfOverrides {
            shards: Some(1),
            ..ConfOverrides::default()
        };
        let server = match self.residency {
            Residency::Resident => ServerNeed::Fresh(overrides),
            Residency::Evicted => ServerNeed::RestartAfterSeed(overrides),
        };
        WorkloadPlan {
            server,
            scale: ScaleFacts {
                scale: scale.as_str().to_string(),
                rows: partitions * ROWS,
                row_bytes: ROW_BYTES,
                keys: partitions,
                concurrency: CONCURRENCY,
                clients: None,
                ..ScaleFacts::default()
            },
            warmup: (SortKeyArm::queries(scale, self.residency) / 20).max(10),
        }
    }

    /// Writes every partition's rows, without timing any of it, the same rows both ways
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn seed<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let client = ctx.client().await?;
            let mut kinds = Seeded::stream(ctx.seed, "sort_key/kinds");
            let mut payloads = Seeded::stream(ctx.seed, "sort_key/payloads");
            let total = ctx.scale.rows;
            let form = self.form;
            let mut built = 0u64;
            let batches = move || {
                if built >= total {
                    return None;
                }
                let mut queries = shoal::shared::queries::Queries::<BenchClient>::default();
                for _ in 0..driver::BATCH.min((total - built) as usize) {
                    // a partition's rows one after another, each at its position
                    let (stream, at) = (built / ROWS, built % ROWS);
                    let (kind, payload) = (kinds.below(16), payloads.string(ROW_BYTES as usize));
                    match form {
                        KeyForm::Padded => queries.add_mut(Event {
                            stream,
                            at: sort_key(at),
                            kind,
                            payload,
                        }),
                        KeyForm::Integer => queries.add_mut(NumberedEvent {
                            stream,
                            at,
                            kind,
                            payload,
                        }),
                    }
                    built += 1;
                }
                Some(Batch { queries })
            };
            driver::drive(&client, batches, "seed", 0).await?;
            Ok(())
        })
    }

    /// Reads one partition a read, timing each
    ///
    /// # Arguments
    ///
    /// * `ctx` - The server, seed and scale this run was given
    fn run<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<Measurement>> {
        Box::pin(async move {
            let client = std::sync::Arc::new(ctx.client().await?);
            let partitions = (ctx.scale.rows / ROWS).max(1);
            let scale = if ctx.scale.scale == "smoke" { Scale::Smoke } else { Scale::Full };
            let (form, access) = (self.form, self.access);
            driver::drive_per_query(
                client,
                ctx.scale.concurrency,
                SortKeyArm::queries(scale, self.residency),
                ctx.warmup,
                "get",
                move |index| -> BenchQueryKinds {
                    let (partition, first) = placed(index, partitions, access);
                    match form {
                        KeyForm::Padded => {
                            let get = EventGet::new(vec![partition]);
                            match access {
                                Access::Get => get.sort_keys((0..NAMED).map(|at| sort_key((first + at * 127) % ROWS)).collect()),
                                Access::Range => get.sort_range(SortRange::new(
                                    Bound::Included(sort_key(first)),
                                    Bound::Excluded(sort_key(first + SPAN)),
                                )),
                                // a page after its cursor: the key before its first row, or the start
                                Access::Walk if first == 0 => get.limit(SPAN as usize),
                                Access::Walk => get.sort_range(SortRange::after(sort_key(first - 1))).limit(SPAN as usize),
                            }
                            .into()
                        }
                        KeyForm::Integer => {
                            let get = NumberedEventGet::new(vec![partition]);
                            match access {
                                Access::Get => get.sort_keys((0..NAMED).map(|at| (first + at * 127) % ROWS).collect()),
                                Access::Range => get.sort_range(SortRange::new(
                                    Bound::Included(first),
                                    Bound::Excluded(first + SPAN),
                                )),
                                Access::Walk if first == 0 => get.limit(SPAN as usize),
                                Access::Walk => get.sort_range(SortRange::after(first - 1)).limit(SPAN as usize),
                            }
                            .into()
                        }
                    }
                },
            )
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Access, KeyForm, SortKeyArm, ROWS, SPAN};
    use crate::workloads::fanout::Residency;
    use crate::workloads::harness::seed::Scale;
    use crate::workloads::schema::{Event, NumberedEvent, sort_key};
    use crate::workloads::workload::Workload;
    use deepsize2::DeepSizeOf;

    /// Every arm is minted once, and its identifier names its form, read and residency
    #[test]
    fn every_arm_is_minted_once() {
        let all = SortKeyArm::all();
        assert_eq!(all.len(), 12);
        let mut ids: Vec<&str> = all.iter().map(Workload::id).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 12, "an arm is minted twice");
        assert!(ids.contains(&"macro/sort_key/u64/walk/evicted"));
    }

    /// The two forms of one read and residency differ in their key and in nothing else
    #[test]
    fn the_pair_differs_only_in_its_key() {
        let all = SortKeyArm::all();
        for padded in all.iter().filter(|arm| arm.form == KeyForm::Padded) {
            let integer = all
                .iter()
                .find(|arm| {
                    arm.form == KeyForm::Integer && arm.access == padded.access && arm.residency == padded.residency
                })
                .expect("every padded arm has an integer twin");
            for scale in [Scale::Smoke, Scale::Full] {
                let (one, other) = (padded.plan(scale), integer.plan(scale));
                assert_eq!(one.scale, other.scale, "{}", padded.id());
                assert_eq!(one.warmup, other.warmup, "{}", padded.id());
                assert_eq!(one.server.overrides(), other.server.overrides(), "{}", padded.id());
            }
            assert_eq!(padded.timing(), integer.timing());
        }
    }

    /// A range and a page never read past their partition's last row
    #[test]
    fn every_read_stays_inside_its_partition() {
        for index in 0..10_000 {
            for access in [Access::Get, Access::Range, Access::Walk] {
                let (partition, first) = super::placed(index, 64, access);
                assert!(partition < 64);
                assert!(first + if access == Access::Get { 0 } else { SPAN } <= ROWS, "{access:?} {index}");
            }
        }
    }

    /// Every read of an evicted arm, its warmup's among them, is of a partition no read before it loaded
    ///
    /// A read leaves its partition resident, so an evicted arm whose reads came back to a partition
    /// would time resident reads after its first pass over them, as O156's first run on titan did.
    #[test]
    fn every_evicted_read_is_of_a_partition_of_its_own() {
        for arm in SortKeyArm::all().iter().filter(|arm| arm.residency == Residency::Evicted) {
            for scale in [Scale::Smoke, Scale::Full] {
                let plan = arm.plan(scale);
                let partitions = plan.scale.rows / ROWS;
                let reads = SortKeyArm::queries(scale, arm.residency);
                assert!(plan.warmup < reads, "{} reads nothing past its warmup", arm.id());
                let mut seen = std::collections::HashSet::new();
                for index in 0..reads {
                    let (partition, _) = super::placed(index, partitions, arm.access);
                    assert!(seen.insert(partition), "{} reads partition {partition} twice", arm.id());
                }
            }
        }
    }

    /// What a row's sort key costs a resident partition: the padded key's twenty bytes on the heap
    /// beside the row, the integer's none (O156's memory figure)
    #[test]
    fn a_rows_resident_bytes_by_its_key() {
        let padded = Event {
            stream: 1,
            at: sort_key(7),
            kind: 3,
            payload: "x".repeat(256),
        };
        let integer = NumberedEvent {
            stream: 1,
            at: 7,
            kind: 3,
            payload: "x".repeat(256),
        };
        // a `String` in the row where a `u64` was, and the twenty digits it holds on the heap
        let extra = std::mem::size_of::<String>() - std::mem::size_of::<u64>() + padded.at.capacity();
        assert_eq!(padded.deep_size_of() - integer.deep_size_of(), extra);
        assert!(padded.at.capacity() >= 20);
        println!(
            "a resident row: {} bytes keyed by a padded string, {} by an integer",
            padded.deep_size_of(),
            integer.deep_size_of()
        );
    }
}
