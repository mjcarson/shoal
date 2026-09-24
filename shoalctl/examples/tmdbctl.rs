//! Shoalctl for the tmdb database

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::{ShoalProjection, ShoalSortedTable, ShoalUnsortedTable};

// the tables, shared with the server half deployed as `tmdb_node`
include!("tmdb/tables.rs");

/// The tables we are adding to to shoal
#[shoal::db(client)]
pub struct Tmdb {
    /// A basic key value table
    #[shoal(projections(MovieSummary))]
    pub movie: PersistentUnsortedTable<Movie, FileSystem>,
    /// A sorted table of movies by keywords
    pub movie_by_keyword: PersistentSortedTable<MovieByKeyword, FileSystem>,
}

/// Query a TMDB database, or deploy a cluster of `tmdb_node` over ssh
///
/// With no arguments this opens the terminal UI against `127.0.0.1:12000`, as it always did;
/// `tmdbctl cluster --help` lists the deployment commands ([F51](../../docs/src/features/cluster-deployment.md)).
#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    shoalctl::cli::main::<TmdbClient>().await
}
