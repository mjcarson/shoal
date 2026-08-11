//! Executes Shoal's benchmarks, keeps their results with their provenance, and renders the book
//!
//! This crate replaces `scripts/bench.sh`, `scripts/collect-micro.sh` and `scripts/compare.sh`.
//! Those three captured JSON into `docs/perf/` and stopped there: nothing read the artifacts back,
//! only the micro layer could be compared, a committed number could not say whether it still
//! described the current code, and a capture was all four layers or nothing.
//!
//! # The four layers
//!
//! | Layer | What it measures | Trustworthy as |
//! | --- | --- | --- |
//! | micro | criterion, over the partition internals | a latency, to within the noise band |
//! | macro | the `tmdb` example end to end | a wall clock, to within the observed interval |
//! | hotpath | a separately built instrumented run | attribution only |
//! | stages | a separately built instrumented run | attribution only |
//!
//! The two instrumented layers are never a source of a latency or a throughput number. Both take
//! extra timestamps on the query path, so their wall clocks are not comparable to the
//! uninstrumented build's, and mixing them is the easiest way to produce a confident wrong answer.
//!
//! # What holds the tool together
//!
//! - **Criterion is driven, not replaced.** `shoal/benches/partitions.rs` and criterion's sampling
//!   are untouched, so the frozen baseline `docs/perf/baselines/B1-performance.json` - captured
//!   before this crate existed - is still directly comparable.
//! - **Nothing in this workspace is a dependency.** See `shoal-bench/Cargo.toml`.
//! - **The rendered page is deterministic.** It is committed and verified with
//!   `shoal-bench render --check`, which means no wall clock, no `HashMap` iteration and no
//!   unbounded float formatting may reach it.

pub mod cli;
pub mod clock;
pub mod collect;
pub mod compare;
pub mod fingerprint;
pub mod fmt;
pub mod model;
pub mod promote;
pub mod registry;
pub mod render;
pub mod run;
pub mod stale;
pub mod store;
pub mod workload_ids;
// the workloads link `shoal`, and the half of this crate that judges a capture must not. see
// `shoal-bench/Cargo.toml` for why this is a feature rather than a separate crate.
#[cfg(feature = "workloads")]
pub mod workloads;
