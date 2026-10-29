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
pub use shoal_proto::shared;
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
/// The traits a schema's tables and database implement
///
/// A partition key is hashed from a row or from a key's values and from nothing else, so code
/// holding an archived row deserializes it first
/// ([Resolved #93](../docs/src/appendix/resolved/archived-partition-hash.md)):
///
/// ```
/// use deepsize2::DeepSizeOf;
/// use rkyv::{Archive, Deserialize, Serialize};
/// use shoal::tables::EphemeralUnsortedTable;
/// use shoal::traits::{PartitionKeySupport, RkyvSupport};
/// use shoal::ShoalUnsortedTable;
///
/// #[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, DeepSizeOf)]
/// #[rkyv(derive(Debug))]
/// #[shoal_table(db = "Library")]
/// pub struct Book {
///     #[shoal(partition)]
///     pub title: String,
/// }
///
/// #[shoal::db]
/// pub struct Library {
///     pub books: EphemeralUnsortedTable<Book>,
/// }
///
/// let book = Book { title: "Moby Dick".to_owned() };
/// let bytes = RkyvSupport::serialize(&book)?;
/// let archived = Book::access(&bytes)?;
/// let key = <Book as RkyvSupport>::deserialize(archived)?.get_partition_key();
/// assert_eq!(key, Book::get_partition_key_from_values(&book.title));
/// # Ok::<(), rkyv::rancor::Error>(())
/// ```
///
/// The same row hashed where it lies is refused, because there is no archived hash to call. The
/// one there was disagreed with the live hash for a string key:
///
/// ```compile_fail
/// use deepsize2::DeepSizeOf;
/// use rkyv::{Archive, Deserialize, Serialize};
/// use shoal::tables::EphemeralUnsortedTable;
/// use shoal::traits::{PartitionKeySupport, RkyvSupport};
/// use shoal::ShoalUnsortedTable;
///
/// #[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, DeepSizeOf)]
/// #[rkyv(derive(Debug))]
/// #[shoal_table(db = "Library")]
/// pub struct Book {
///     #[shoal(partition)]
///     pub title: String,
/// }
///
/// #[shoal::db]
/// pub struct Library {
///     pub books: EphemeralUnsortedTable<Book>,
/// }
///
/// let book = Book { title: "Moby Dick".to_owned() };
/// let bytes = RkyvSupport::serialize(&book)?;
/// let archived = Book::access(&bytes)?;
/// let key = Book::get_partition_key_from_archived_insert(archived);
/// assert_eq!(key, Book::get_partition_key_from_values(&book.title));
/// # Ok::<(), rkyv::rancor::Error>(())
/// ```
pub use shoal_proto::shared::traits;

// The client. Its error types come from the protocol crate rather than from here, because
// `QuerySupport` and `shared::responses` both name them.
pub use shoal_client::client::{self, Routing, Shoal, ShoalResponse, ShoalUnorderedResultStream};
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
/// A bucket, declared as a field of a schema beside its tables
///
/// The field names a marker, a unit struct whose name is the bucket's, and the macro generates
/// the bucket's two tables and everything else it needs
/// ([F79](../docs/src/features/bucket-schema.md)):
///
/// ```
/// use deepsize2::DeepSizeOf;
/// use rkyv::{Archive, Deserialize, Serialize};
/// use shoal::storage::FileSystem;
/// use shoal::tables::{Bucket, PersistentUnsortedTable};
/// use shoal::ShoalUnsortedTable;
///
/// #[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, DeepSizeOf)]
/// #[rkyv(derive(Debug))]
/// #[shoal_table(db = "Media")]
/// pub struct Movie {
///     #[shoal(partition)]
///     pub id: u64,
/// }
///
/// pub struct Posters;
///
/// #[shoal::db]
/// pub struct Media {
///     pub movies: PersistentUnsortedTable<Movie, FileSystem>,
///     pub posters: Bucket<Posters>,
/// }
///
/// assert_eq!(<MediaClient as shoal::traits::QuerySupport>::bucket_names(), &["Posters"]);
/// ```
///
/// A bucket that names no marker is refused with a span on the field, not a panic of the macro:
///
/// ```compile_fail
/// use deepsize2::DeepSizeOf;
/// use rkyv::{Archive, Deserialize, Serialize};
/// use shoal::storage::FileSystem;
/// use shoal::tables::{Bucket, PersistentUnsortedTable};
/// use shoal::ShoalUnsortedTable;
///
/// #[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, DeepSizeOf)]
/// #[rkyv(derive(Debug))]
/// #[shoal_table(db = "Media")]
/// pub struct Movie {
///     #[shoal(partition)]
///     pub id: u64,
/// }
///
/// #[shoal::db]
/// pub struct Media {
///     pub movies: PersistentUnsortedTable<Movie, FileSystem>,
///     pub posters: Bucket,
/// }
/// ```
///
/// And so is a table whose row has the name a bucket's generated table takes, since a table is
/// named by its row and the two would be one table:
///
/// ```compile_fail
/// use deepsize2::DeepSizeOf;
/// use rkyv::{Archive, Deserialize, Serialize};
/// use shoal::storage::FileSystem;
/// use shoal::tables::{Bucket, PersistentUnsortedTable};
/// use shoal::ShoalUnsortedTable;
///
/// #[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, DeepSizeOf)]
/// #[rkyv(derive(Debug))]
/// #[shoal_table(db = "Media")]
/// pub struct PostersObjectMeta {
///     #[shoal(partition)]
///     pub id: u64,
/// }
///
/// pub struct Posters;
///
/// #[shoal::db]
/// pub struct Media {
///     pub movies: PersistentUnsortedTable<PostersObjectMeta, FileSystem>,
///     pub posters: Bucket<Posters>,
/// }
/// ```
#[cfg(feature = "server")]
pub use shoal_core::tables::Bucket;
#[cfg(feature = "server")]
pub use shoal_core::tables::{
    self, EphemeralSortedTable, EphemeralUnsortedTable, PersistentSortedTable,
    PersistentUnsortedTable,
};
#[cfg(feature = "server")]
pub use shoal_core::ShoalPool;

pub use shoal_derive::{db, ShoalProjection, ShoalSortedTable, ShoalUnsortedTable};
