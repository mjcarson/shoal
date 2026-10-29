//! Spike X16's schema: the walk beside the foreground, and the scans rows' before and after
//! ([X16](../../docs/src/future-work/scans/spikes.md#x16-the-walk-beside-the-foreground))
//!
//! Three tables, every one loaded from the dataset folder `x16-data` writes, so `shoaladm bench`
//! drives them with nothing written for this schema but the tables:
//!
//! - [`Cell`], the foreground: a kibibyte row a partition, the reference cell's shape
//!   (`grid/unsorted/r50/1024`), few enough to stay resident in a side cluster's node;
//! - [`Big`], the table scanned from its archives: the same rows, three million of them, read from
//!   disk after every restart, since a walk never makes a row resident;
//! - [`Keyword`], the table scanned resident and overlaid: the TMDB csv's keyword rows, sorted
//!   partitions of every size up to 27,614 rows, written into again while a scan reads it.
//!
//! Nothing here is benchmark code: the benchmark is the generic one, built from what the table
//! derives emit ([F66](../../docs/src/features/dataset-benchmarks.md)).

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal::{FileSystem, PersistentSortedTable, PersistentUnsortedTable, ShoalSortedTable, ShoalUnsortedTable};

/// The foreground's table: one row of about a kibibyte a partition, kept resident
#[derive(
    Debug,
    Clone,
    PartialEq,
    Archive,
    Serialize,
    Deserialize,
    serde::Deserialize,
    serde::Serialize,
    ShoalUnsortedTable,
    DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "ScanLab", dataset)]
pub struct Cell {
    /// The row's id, which it is partitioned by
    #[shoal(partition)]
    pub id: u64,
    /// Its bytes, so a row has the reference cell's width
    pub body: String,
}

/// The table scanned from its archives: the same rows as the cell's, far more of them
#[derive(
    Debug,
    Clone,
    PartialEq,
    Archive,
    Serialize,
    Deserialize,
    serde::Deserialize,
    serde::Serialize,
    ShoalUnsortedTable,
    DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "ScanLab", dataset)]
pub struct Big {
    /// The row's id, which it is partitioned by
    #[shoal(partition)]
    pub id: u64,
    /// Its bytes
    pub body: String,
}

/// The table scanned resident and overlaid: a movie listed under one of its keywords, the TMDB
/// loader's `MovieByKeyword` row
#[derive(
    Debug,
    Clone,
    PartialEq,
    Archive,
    Serialize,
    Deserialize,
    serde::Deserialize,
    serde::Serialize,
    ShoalSortedTable,
    DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "ScanLab", dataset)]
pub struct Keyword {
    /// The keyword this row is partitioned by
    #[shoal(partition)]
    pub keyword: String,
    /// The title and the zero padded id, which rows are ordered by within a keyword
    #[shoal(sort)]
    pub order: String,
    /// The movie's title
    pub title: String,
    /// The movie's id
    pub id: u64,
}

/// The spike's schema
#[shoal::db]
pub struct ScanLab {
    /// The foreground's rows
    pub cell: PersistentUnsortedTable<Cell, FileSystem>,
    /// The rows scanned from their archives
    pub big: PersistentUnsortedTable<Big, FileSystem>,
    /// The rows scanned resident and overlaid
    pub keyword: PersistentSortedTable<Keyword, FileSystem>,
}

/// A row's bytes, the same for a seed and an id on every run: hex digits of a SplitMix64 stream
///
/// # Arguments
///
/// * `seed` - The dataset's seed, with the table mixed in
/// * `id` - The row's id
/// * `width` - How many bytes
#[must_use]
pub fn body(seed: u64, id: u64, width: usize) -> String {
    // a SplitMix64 stream from the seed and the id, sixteen hex digits a step
    let mut state = seed ^ id.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut body = String::with_capacity(width + 16);
    while body.len() < width {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^= mixed >> 31;
        body.push_str(&format!("{mixed:016x}"));
    }
    // cut to the width asked for, every digit a byte
    body.truncate(width);
    body
}

/// Every keyword row a TMDB movie is listed under, as the TMDB loader lists it
///
/// # Arguments
///
/// * `movie` - The movie, as the csv reads
#[must_use]
pub fn keyword_rows(movie: &tmdb_dataset::Movie) -> Vec<Keyword> {
    // the loader's own rows, so the partitions and their order are MovieByKeyword's
    tmdb_dataset::MovieByKeyword::rows(movie)
        .into_iter()
        .map(|row| Keyword {
            keyword: row.keyword,
            order: row.order,
            title: row.title,
            id: row.id,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{body, Big, Cell, Keyword};

    /// A row's bytes are its seed's and id's alone, as wide as asked, and differ by id
    #[test]
    fn a_body_is_its_seed_and_id_alone() {
        assert_eq!(body(16, 7, 1000), body(16, 7, 1000));
        assert_eq!(body(16, 7, 1000).len(), 1000);
        assert_ne!(body(16, 7, 1000), body(16, 8, 1000));
        assert_ne!(body(16, 7, 1000), body(17, 7, 1000));
    }

    /// Every row written as a line of json reads back as its table's row, as the bench reads it
    #[test]
    fn a_written_row_reads_back_as_its_table() {
        let cell = Cell { id: 3, body: body(1, 3, 64) };
        let big = Big { id: 4, body: body(2, 4, 64) };
        let keyword = Keyword {
            keyword: "woman director".to_string(),
            order: "Title\u{1f}0000000000000042".to_string(),
            title: "Title".to_string(),
            id: 42,
        };
        let line = serde_json::to_string(&cell).expect("a cell writes");
        assert_eq!(serde_json::from_str::<Cell>(&line).expect("a cell reads"), cell);
        let line = serde_json::to_string(&big).expect("a row writes");
        assert_eq!(serde_json::from_str::<Big>(&line).expect("a row reads"), big);
        let line = serde_json::to_string(&keyword).expect("a keyword writes");
        assert_eq!(serde_json::from_str::<Keyword>(&line).expect("a keyword reads"), keyword);
    }
}
