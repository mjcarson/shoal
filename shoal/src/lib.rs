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

// The crates the generated code names by path.
//
// This facade declares none of them itself. Each is re-exported from the crate whose trait
// signatures it appears in, so a schema built by `#[shoal::db]` cannot resolve a different `rkyv`
// than `RkyvSupport` was compiled against. That is the fix for known issue 54: a crate writing a
// schema used to have to declare `glommio`, `uuid` and `deepsize2` itself, having never heard of
// any of them.
//
// `gxhash` is among them because a client hashes its own partition keys, so it cannot be gated
// behind the engine - the regression crate catches that directly. It resolves to the one
// workspace pin, and `tests/partition_keys.rs` freezes what that pin hashes eight keys to
// (resolved item 65).
#[cfg(feature = "server")]
pub use shoal_core::{glommio, kanal, lru};
pub use shoal_proto::{deepsize2, gxhash, rkyv, serde_json, tracing, uuid};
// never race a bare kanal receive: race a kept one (Resolved #152)
pub use shoal_client::channel;

// The protocol: what both peers see. Present whether or not an engine is linked.
pub use shoal_proto::shared::{self, traits};
/// Loading a table from a dataset file and benchmarking it by name
///
/// A table opts in to being loaded from a dataset and benchmarked by name with `dataset`, and a
/// row that does must also derive `serde::Deserialize`
/// ([F66](../docs/src/features/dataset-benchmarks.md)):
///
/// ```
/// use deepsize2::DeepSizeOf;
/// use rkyv::{Archive, Deserialize, Serialize};
/// use shoal::tables::EphemeralUnsortedTable;
/// use shoal::ShoalUnsortedTable;
///
/// #[derive(Debug, Archive, Serialize, Deserialize, serde::Deserialize, Clone, ShoalUnsortedTable, DeepSizeOf)]
/// #[rkyv(derive(Debug))]
/// #[shoal_table(db = "Shop", dataset)]
/// pub struct Item {
///     #[shoal(partition)]
///     pub id: u64,
/// }
///
/// #[shoal::db]
/// pub struct Shop {
///     pub items: EphemeralUnsortedTable<Item>,
/// }
/// ```
///
/// The same row without `serde::Deserialize` is refused where it opts in, not where a benchmark
/// first reads it:
///
/// ```compile_fail
/// use deepsize2::DeepSizeOf;
/// use rkyv::{Archive, Deserialize, Serialize};
/// use shoal::tables::EphemeralUnsortedTable;
/// use shoal::ShoalUnsortedTable;
///
/// #[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, DeepSizeOf)]
/// #[rkyv(derive(Debug))]
/// #[shoal_table(db = "Shop", dataset)]
/// pub struct Item {
///     #[shoal(partition)]
///     pub id: u64,
/// }
///
/// #[shoal::db]
/// pub struct Shop {
///     pub items: EphemeralUnsortedTable<Item>,
/// }
/// ```
pub use shoal_proto::shared::dataset;

// The client. Its error types come from the protocol crate rather than from here, because
// `QuerySupport` and `shared::responses` both name them.
pub use shoal_client::client::{self, Shoal, ShoalResponse, ShoalUnorderedResultStream};
pub use shoal_proto::client::{ChannelError, ConnectError, Errors, QuerySuceededOpts};
pub use shoal_proto::FromShoal;

// The server, and the engine it runs on. None of this exists without the `server` feature, which
// is the whole point: a `#[shoal::db(client)]` schema names its table and storage types in field
// position only, never in a `use`, so it never reaches for any of these.
#[cfg(feature = "server")]
pub use shoal_core::server::{
    self,
    database::ShoalDatabase,
    routing::{ArchivedShardRouting, ShardRouting},
    Conf,
};
#[cfg(feature = "server")]
pub use shoal_core::storage::{self, FileSystem, NoStorage};
#[cfg(feature = "server")]
pub use shoal_core::tables::{
    self, EphemeralSortedTable, EphemeralUnsortedTable, PersistentSortedTable,
    PersistentUnsortedTable,
};
#[cfg(feature = "server")]
pub use shoal_core::ShoalPool;

pub use shoal_derive::{db, ShoalProjection, ShoalSortedTable, ShoalUnsortedTable};
