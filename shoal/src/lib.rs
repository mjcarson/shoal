//! Shoal's public surface: everything a user of the database needs, and nothing else
//!
//! This crate is a facade over `shoal-core` and `shoal-derive`. It deliberately holds no code of
//! its own.
//!
//! It used to hold two modules that were not database facilities at all. `bencher` was a latency
//! histogram with a baseline comparison engine attached, and `stages` built a profiling report;
//! between them they meant every user of this crate compiled a percentile calculator and linked a
//! terminal colour library. Both existed to serve the `tmdb` example back when that example was
//! also the benchmark harness. The harness now lives in `shoal-bench`, which is where both of them
//! went - see `docs/src/features/purpose-built-workloads.md`.

pub use shoal_core::ShoalPool;
pub use shoal_core::client::{
    self, Errors, QuerySuceededOpts, Shoal, ShoalResponse, ShoalUnorderedResultStream,
};
pub use shoal_core::server::Conf;
pub use shoal_core::shared::{self, traits};
pub use shoal_core::storage::{self, FileSystem, NoStorage};
pub use shoal_core::tables::{
    self, EphemeralSortedTable, EphemeralUnsortedTable, PersistentSortedTable,
    PersistentUnsortedTable,
};
pub use shoal_derive::{ShoalProjection, ShoalSortedTable, ShoalUnsortedTable, db};
