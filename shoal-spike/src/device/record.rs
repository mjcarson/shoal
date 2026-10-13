//! A side's figures as a record, written to JSON so that rounds run apart can be merged
//!
//! Every side of every cell of every measurement leaves one record a round. `report` reads the
//! records of every round and host, groups them, and prints each figure as its interval across
//! rounds, then judges the four triggers X6 named before it ran.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// One side of one cell in one round
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// The host it ran on
    pub host: String,
    /// The filesystem it ran on
    pub fs: String,
    /// The leg, naming host, device and filesystem together
    pub leg: String,
    /// The measurement: chunk, journal, partial and so on
    pub measurement: String,
    /// The cell within it, such as `size=1M writers=6`
    pub cell: String,
    /// The side within the cell
    pub side: String,
    /// The round
    pub round: u32,
    /// Whether it was a quick run, which is never a measurement
    pub quick: bool,
    /// Its figures, by name
    pub metrics: BTreeMap<String, f64>,
}

/// Write records to a JSON file, adding to what it already holds
///
/// # Arguments
///
/// * `path` - The file
/// * `records` - The records to add
pub fn append(path: &Path, records: &[Record]) {
    // what the file holds already, so a run in pieces keeps every piece
    let mut all: Vec<Record> = std::fs::read(path)
        .ok()
        .and_then(|bytes| shoal::serde_json::from_slice(&bytes).ok())
        .unwrap_or_default();
    all.extend_from_slice(records);
    let bytes = shoal::serde_json::to_vec_pretty(&all).expect("records encode");
    std::fs::write(path, bytes).expect("the records file is writable");
}

/// Read every record from a set of JSON files
///
/// # Arguments
///
/// * `paths` - The files
#[must_use]
pub fn read_all(paths: &[String]) -> Vec<Record> {
    let mut all = Vec::new();
    for path in paths {
        let bytes = std::fs::read(path).unwrap_or_else(|error| panic!("reading {path}: {error}"));
        let records: Vec<Record> = shoal::serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("decoding {path}: {error}"));
        all.extend(records);
    }
    all
}
