//! Ephemeral tables are fully in memory and are never persisted to disk
//!
//! This means while they are the fastest when it comes to writes they will
//! not retain data through restarts.
//!
//! # An ephemeral table is not a separate table type
//!
//! Both of these are aliases, not structs. A table is generic over the storage engine beneath it,
//! so an ephemeral table is the same table every persistent database uses with
//! [`NoStorage`](crate::storage::NoStorage) underneath it instead of
//! [`FileSystem`](crate::storage::FileSystem). Every layer above the engine — routing, partitions,
//! filters, projections, the query and response enums the derive macros mint — is shared with the
//! persistent tables and cannot drift from them.
//!
//! That is also what makes an ephemeral benchmark worth reading. `macro/insert_ephemeral` and
//! `macro/insert_unsorted` differ in the storage engine and in nothing else, so the gap between
//! them is the storage layer rather than a difference between two implementations that happen to
//! both be called a table.
//!
//! # What an ephemeral table still costs
//!
//! Taking the disk away does not take away the machinery built around having one. An insert is
//! still wrapped in an intent, still parked, and still released on a shard sweep rather than
//! answered inline, and every partition is still held behind a `MaybeLoaded` that can only ever
//! be the loaded arm. See `docs/src/features/ephemeral-tables.md`.
//!
//! # What it buys
//!
//! No intent log write, no fdatasync, no compaction, no archive read — and no eviction. An
//! ephemeral partition is never marked evictable, because the only two places that send a
//! `MarkEvictable` are the filesystem compactor and the partition load path, and an ephemeral
//! table reaches neither. Memory pressure therefore cannot reclaim it, which is a property in
//! both directions: the data is safe, and it is also the users problem to bound.

use super::persistent::{PersistentSortedTable, PersistentUnsortedTable};
use super::storage::NoStorage;

/// A sorted table that keeps every row in memory and never writes one to disk
///
/// # Generics
///
/// * `R` - The row type this table holds
/// * `D` - The database this table is a field of
/// * `N` - The table name enum for that database
///
/// A schema writes this with one generic — `EphemeralSortedTable<MyRow>` — and the `#[shoal::db]`
/// macro fills the other two in.
pub type EphemeralSortedTable<R, D, N> = PersistentSortedTable<R, NoStorage<D>, N>;

/// An unsorted table that keeps every row in memory and never writes one to disk
///
/// # Generics
///
/// * `R` - The row type this table holds
/// * `D` - The database this table is a field of
/// * `N` - The table name enum for that database
///
/// A schema writes this with one generic — `EphemeralUnsortedTable<MyRow>` — and the
/// `#[shoal::db]` macro fills the other two in.
pub type EphemeralUnsortedTable<R, D, N> = PersistentUnsortedTable<R, NoStorage<D>, N>;
