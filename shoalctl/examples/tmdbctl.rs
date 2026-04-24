//! Shoalctl for the tmdb database

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::client::Shoal;
use shoal::storage::FileSystem;
use shoal::tables::{PersistentSortedTable, PersistentUnsortedTable};
use shoal::{shoal_db, ShoalSortedTable, ShoalUnsortedTable};
use std::sync::Arc;

/// Deserialize a comma-space separated string into a Vec<String>
fn deserialize_comma_separated<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    if s.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(s.split(", ").map(|s| s.to_string()).collect())
    }
}

#[derive(
    Debug,
    Archive,
    Serialize,
    Deserialize,
    Clone,
    ShoalUnsortedTable,
    serde::Deserialize,
    serde::Serialize,
    PartialEq,
    DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Tmdb")]
pub struct Movie {
    /// The id for this movie
    #[shoal(partition)]
    pub id: u64,
    /// The name of this move
    #[shoal(filter)]
    pub title: String,
    /// The vote average
    pub vote_average: f64,
    /// The total number of votes
    pub vote_count: u64,
    /// The status of this movie
    pub status: String,
    /// The Date this movie was release
    pub release_date: String,
    /// The total revenue this movie made
    pub revenue: u64,
    /// The runtime for this movie in minutes
    pub runtime: u64,
    /// Whether this is an adult movie
    pub adult: bool,
    /// The path to this movies backdrop on tmdb
    pub backdrop_path: String,
    /// The budget for this movie
    pub budget: u64,
    /// The url to this movies homepage
    pub homepage: String,
    /// The imdb id for this movie
    pub imdb_id: String,
    /// The original language for this movie
    pub original_language: String,
    /// The original title for this movie
    pub original_title: String,
    /// The overview for this movie
    #[shoal(update)]
    pub overview: String,
    /// The popularity of this movie
    pub popularity: f64,
    /// The path to this movies poster on tmdb
    pub poster_path: String,
    /// The tagline for this movie
    pub tagline: String,
    /// The genres for this movie
    #[serde(deserialize_with = "deserialize_comma_separated")]
    pub genres: Vec<String>,
    /// The production companies for this movie
    #[serde(deserialize_with = "deserialize_comma_separated")]
    pub production_companies: Vec<String>,
    /// The countries this movie was produced in
    #[serde(deserialize_with = "deserialize_comma_separated")]
    pub production_countries: Vec<String>,
    /// The languages spoken in this movie
    #[serde(deserialize_with = "deserialize_comma_separated")]
    pub spoken_languages: Vec<String>,
    /// The keywords for this movie
    #[serde(deserialize_with = "deserialize_comma_separated")]
    pub keywords: Vec<String>,
}

#[derive(
    Debug,
    Archive,
    Serialize,
    Deserialize,
    Clone,
    ShoalSortedTable,
    serde::Deserialize,
    serde::Serialize,
    PartialEq,
    DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Tmdb")]
pub struct MovieByKeyword {
    /// The keyword for this movie
    #[shoal(partition)]
    pub keyword: String,
    /// The name of this movie
    #[shoal(sort)]
    pub title: String,
}

/// The tables we are adding to to shoal
#[shoal_db]
pub struct Tmdb {
    /// A basic key value table
    pub movie: PersistentUnsortedTable<Movie, FileSystem>,
    /// A sorted table of movies by keywords
    pub movie_by_keyword: PersistentSortedTable<MovieByKeyword, FileSystem>,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // Create your Shoal client with your database type
    let shoal = Arc::new(Shoal::<TmdbClient>::new("127.0.0.1:12000").await.unwrap());
    // Run shoalctl with the client type
    shoalctl::run(shoal).await
}
