//! The TMDB dataset as a deployable Shoal database
//!
//! Two programs are built from this crate, and both from the schema below, so they agree on the
//! fingerprint a client's hello is checked against
//! ([F54](../../docs/src/features/tmdb-dataset-deployment.md)):
//!
//! - `tmdb-dataset-node` is the server program. An inventory's `server:` names it, and
//!   `cluster bootstrap` copies it to every host. It reads the `shoal.yml` the deployment rendered
//!   for its node and nothing else, so there is nothing to configure in it.
//! - `tmdb-dataset-loader` fills a deployed cluster from the csv (`load -i <inventory>`), and
//!   carries every `shoalctl` command for this schema, `cluster new` and `cluster bootstrap`
//!   included, so one program deploys the database and loads it.
//!
//! The dataset is `TMDB_movie_dataset_v11.csv`, about 538 MB and 1.19 million movies, from
//! <https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies>. Nothing in
//! this repository fetches it.

pub mod bench;
pub mod load;

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::{
    FileSystem, PersistentSortedTable, PersistentUnsortedTable, ShoalSortedTable,
    ShoalUnsortedTable,
};

/// Deserialize a comma-space separated column into a list
///
/// The dataset joins its list valued columns with `", "` and quotes the whole thing, so what
/// arrives here is one string per row rather than a sequence.
///
/// # Arguments
///
/// * `deserializer` - The deserializer to read this column from
fn deserialize_comma_separated<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // read the whole column as one string
    let raw: String = serde::Deserialize::deserialize(deserializer)?;
    // an empty cell is an empty list rather than a list holding one empty string
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    Ok(raw.split(", ").map(ToString::to_string).collect())
}

/// Deserialize a column that should hold an integer, tolerating one that does not
///
/// A cell that is empty or unparseable becomes zero. This is deliberate and only safe because none
/// of the fields it is used on identify a row: `id` is left strict, so a row whose key cannot be
/// read is skipped rather than being filed under partition zero along with every other unreadable
/// row.
///
/// # Arguments
///
/// * `deserializer` - The deserializer to read this column from
fn deserialize_lenient_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // csv hands every column over as text, so asking for a string always succeeds
    let raw: String = serde::Deserialize::deserialize(deserializer)?;
    Ok(raw.trim().parse().unwrap_or_default())
}

/// Deserialize a column that should hold a float, tolerating one that does not
///
/// The same bargain as [`deserialize_lenient_u64`], for the two rating columns.
///
/// # Arguments
///
/// * `deserializer` - The deserializer to read this column from
fn deserialize_lenient_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // csv hands every column over as text, so asking for a string always succeeds
    let raw: String = serde::Deserialize::deserialize(deserializer)?;
    Ok(raw.trim().parse().unwrap_or_default())
}

/// Deserialize a column that should hold a boolean, tolerating one that does not
///
/// # Arguments
///
/// * `deserializer` - The deserializer to read this column from
fn deserialize_lenient_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // csv hands every column over as text, so asking for a string always succeeds
    let raw: String = serde::Deserialize::deserialize(deserializer)?;
    Ok(raw.trim().eq_ignore_ascii_case("true"))
}

/// A movie, stored one per partition
///
/// The whole TMDB record, column for column: a wide row with several variable length fields.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Archive,
    Serialize,
    Deserialize,
    ShoalUnsortedTable,
    DeepSizeOf,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Tmdb")]
pub struct Movie {
    /// The id for this movie, which it is partitioned by
    ///
    /// Left strict on purpose: a row whose key will not parse is skipped by the loader instead of
    /// being written under partition zero.
    #[shoal(partition)]
    pub id: u64,
    /// The name of this movie, which a get can filter on
    #[shoal(filter)]
    pub title: String,
    /// The vote average
    #[serde(deserialize_with = "deserialize_lenient_f64")]
    pub vote_average: f64,
    /// The total number of votes
    #[serde(deserialize_with = "deserialize_lenient_u64")]
    pub vote_count: u64,
    /// The status of this movie
    pub status: String,
    /// The date this movie was released
    pub release_date: String,
    /// The total revenue this movie made
    #[serde(deserialize_with = "deserialize_lenient_u64")]
    pub revenue: u64,
    /// The runtime for this movie in minutes
    #[serde(deserialize_with = "deserialize_lenient_u64")]
    pub runtime: u64,
    /// Whether this is an adult movie
    #[serde(deserialize_with = "deserialize_lenient_bool")]
    pub adult: bool,
    /// The path to this movies backdrop on tmdb
    pub backdrop_path: String,
    /// The budget for this movie
    #[serde(deserialize_with = "deserialize_lenient_u64")]
    pub budget: u64,
    /// The url to this movies homepage
    pub homepage: String,
    /// The imdb id for this movie
    pub imdb_id: String,
    /// The original language for this movie
    pub original_language: String,
    /// The original title for this movie
    pub original_title: String,
    /// The overview for this movie, which an update can change
    #[shoal(update)]
    pub overview: String,
    /// The popularity of this movie
    #[serde(deserialize_with = "deserialize_lenient_f64")]
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

/// How many digits an id is padded to in a keyword row's sort key
///
/// Fixed, because lexicographic order only agrees with numeric order when every id is the same
/// width. `u64::MAX` is twenty digits, so this covers every id.
pub const ID_WIDTH: usize = 20;

/// A movie listed under one of its keywords, stored many per partition
///
/// Ordered by `order`, which is the title and then the id. The id is part of the sort key because
/// an insert replaces the row with the same sort key: sorted by title alone, two films of one
/// title tagged with one keyword would be one row, and loading either would overwrite the other.
/// A sort key is one field, so the two are joined into one string rather than sorted as a pair.
#[derive(
    Debug, Clone, PartialEq, Archive, Serialize, Deserialize, ShoalSortedTable, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Tmdb")]
pub struct MovieByKeyword {
    /// The keyword this row is partitioned by
    #[shoal(partition)]
    pub keyword: String,
    /// The title and the zero padded id, which rows are ordered by within a keyword
    #[shoal(sort)]
    pub order: String,
    /// The movie's title
    pub title: String,
    /// The movie's id, the partition its whole row is in
    pub id: u64,
}

impl MovieByKeyword {
    /// The sort key a movie is listed under: its title, a unit separator, and its padded id
    ///
    /// The separator sorts below every printable character, so a title that is a prefix of
    /// another still sorts first.
    ///
    /// # Arguments
    ///
    /// * `title` - The movie's title
    /// * `id` - The movie's id
    #[must_use]
    pub fn order(title: &str, id: u64) -> String {
        format!("{title}\u{1f}{id:0width$}", width = ID_WIDTH)
    }

    /// Every keyword row a movie is listed under, one per keyword it carries
    ///
    /// # Arguments
    ///
    /// * `movie` - The movie to list
    #[must_use]
    pub fn rows(movie: &Movie) -> Vec<MovieByKeyword> {
        // one sort key for every row of this movie
        let order = Self::order(&movie.title, movie.id);
        // one row per keyword, naming the movie by title and id
        movie
            .keywords
            .iter()
            .map(|keyword| MovieByKeyword {
                keyword: keyword.clone(),
                order: order.clone(),
                title: movie.title.clone(),
                id: movie.id,
            })
            .collect()
    }
}

/// The database
///
/// `#[shoal::db]` reads this struct and generates the client type, the query enum and the
/// response enum the loader sends with, and the schema the node serves.
#[shoal::db]
pub struct Tmdb {
    /// Movies, one per partition
    pub movie: PersistentUnsortedTable<Movie, FileSystem>,
    /// Movies by keyword, many per partition
    pub movie_by_keyword: PersistentSortedTable<MovieByKeyword, FileSystem>,
}
