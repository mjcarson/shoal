//! A schema that compiles against the client alone
//!
//! This crate exists to fail. It declares `shoal` with `default-features = false` and nothing
//! else but the two derives a schema writes by hand, so if the client half of `#[shoal::db]`
//! ever names `glommio`, `kanal`, `uuid` or anything under `shoal::server`, this stops building
//! and says which crate it could not find.
//!
//! That is [D5](../../../docs/src/direction/runtimes.md)'s stated check - "a test crate that
//! declares only the client crate, writes a schema, and compiles" - and it would have failed for
//! three separate reasons before [F15](../../../docs/src/features/client-server-split.md).
//!
//! # The rule this crate pins
//!
//! **A client schema names its table and storage types in field position only, and never in a
//! `use`.** `PersistentSortedTable` and `FileSystem` below are read for their names by the macro
//! and then discarded - `db(client)` does not emit the struct, so neither type ever reaches type
//! resolution. An import of either would fail here even though the field type does not, because
//! `shoal::tables` and `shoal::storage` do not exist without the `server` feature.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::{ShoalProjection, ShoalSortedTable, ShoalUnsortedTable};

/// An unsorted table with a partition key, two filters and an update field
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "CheckDb")]
pub struct Movie {
    /// The partition key for this movie
    #[shoal(partition)]
    pub id: u64,
    /// The title of this movie, which can be filtered on
    #[shoal(filter)]
    pub title: String,
    /// Whether this movie has been watched, which can be filtered on
    #[shoal(filter)]
    pub watched: bool,
    /// A payload that cannot be used in a where clause
    #[shoal(update)]
    pub data: String,
}

/// A subset of [`Movie`], so the projection emissions are exercised too
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalProjection, PartialEq, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_projection(table = "Movie")]
pub struct MovieTitle {
    /// The partition key this projection came from
    #[shoal(partition)]
    pub id: u64,
    /// The only field this projection carries
    pub title: String,
}

/// A sorted table, so both table kinds are covered
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "CheckDb")]
pub struct MovieByKeyword {
    /// The keyword this row is filed under
    #[shoal(partition)]
    pub keyword: String,
    /// The title of the movie this keyword points at
    #[shoal(sort)]
    pub title: String,
    /// The id of that movie
    #[shoal(filter)]
    pub movie_id: u64,
}

// no `use shoal::tables::...` and no `use shoal::storage::...`, deliberately - see the module docs
#[shoal::db(client)]
pub struct CheckDb {
    /// The movies, by id
    #[shoal(projections(MovieTitle))]
    pub movies: PersistentUnsortedTable<Movie, FileSystem>,
    /// The same movies, by keyword
    pub movies_by_keyword: PersistentSortedTable<MovieByKeyword, FileSystem>,
}
