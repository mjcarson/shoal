//! A Shoal node serving the TMDB schema `tmdbctl` is built against
//!
//! ```sh
//! tmdb_node serve --conf shoal.yml
//! tmdb_node claim --conf shoal.yml
//! ```
//!
//! This is the server program `tmdbctl cluster` deploys: the whole of it is
//! `shoal::server::node::main` for the same tables `tmdbctl` includes, so a client of one and a
//! server of the other agree on the schema fingerprint the hello checks
//! ([F51](../../docs/src/features/cluster-deployment.md)). The tables are not `tmdb.rs`'s, which is
//! a tour with a smaller movie; they are the wide row the dataset has.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::{
    FileSystem, PersistentSortedTable, PersistentUnsortedTable, ShoalProjection, ShoalSortedTable,
    ShoalUnsortedTable,
};

// the tables, shared with the client half `tmdbctl`
include!("../../shoalctl/examples/tmdb/tables.rs");

/// The tables we are adding to to shoal
#[shoal::db]
pub struct Tmdb {
    /// A basic key value table
    #[shoal(projections(MovieSummary))]
    pub movie: PersistentUnsortedTable<Movie, FileSystem>,
    /// A sorted table of movies by keywords
    pub movie_by_keyword: PersistentSortedTable<MovieByKeyword, FileSystem>,
}

/// Serve or claim a node of the TMDB schema
fn main() -> Result<(), shoal::server::ServerError> {
    shoal::server::node::main::<Tmdb>()
}
