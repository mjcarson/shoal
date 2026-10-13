//! A cell's figures as a record, written to JSON so that rounds run apart can be merged
//!
//! Copied from X3's (`shoal-spike-bytes/src/record.rs`), which took X10's and X6's. Every path of
//! every cell of every leg leaves one record a round. `x8 report` reads the records of every
//! round, groups them, prints each figure as its interval across rounds, and judges the triggers
//! X8 named before it ran.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// One side of one cell in one round
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// The leg: `lab` or `loopback`
    pub measurement: String,
    /// The cell within it, such as `size=4096 depth=32`
    pub cell: String,
    /// The side within the cell: a path, `row`, `staged` or `inline`, or `heat`
    pub side: String,
    /// The round
    pub round: u32,
    /// Whether it was a quick run, which is never a measurement
    pub quick: bool,
    /// Its figures, by name
    pub metrics: BTreeMap<String, f64>,
    /// What it was run on that is not a figure: a host, a device
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl Record {
    /// A record with no figures yet
    ///
    /// # Arguments
    ///
    /// * `measurement` - The measurement
    /// * `cell` - The cell
    /// * `side` - The side
    /// * `round` - The round
    /// * `quick` - Whether this is a quick run
    #[must_use]
    pub fn new(measurement: &str, cell: &str, side: &str, round: u32, quick: bool) -> Self {
        Record {
            measurement: measurement.to_string(),
            cell: cell.to_string(),
            side: side.to_string(),
            round,
            quick,
            metrics: BTreeMap::new(),
            labels: BTreeMap::new(),
        }
    }

    /// Set a figure
    ///
    /// # Arguments
    ///
    /// * `name` - The figure's name
    /// * `value` - Its value
    pub fn set(&mut self, name: &str, value: f64) -> &mut Self {
        self.metrics.insert(name.to_string(), value);
        self
    }

    /// Set a label
    ///
    /// # Arguments
    ///
    /// * `name` - The label's name
    /// * `value` - Its value
    pub fn label(&mut self, name: &str, value: impl Into<String>) -> &mut Self {
        self.labels.insert(name.to_string(), value.into());
        self
    }
}

/// Write records to a JSON file, adding to what it already holds
///
/// # Arguments
///
/// * `path` - The file
/// * `records` - The records to add
///
/// # Errors
///
/// When the file holds something other than records, or cannot be written.
pub fn append(path: &Path, records: &[Record]) -> color_eyre::Result<()> {
    // what the file holds already, so a run in pieces keeps every piece
    let mut all: Vec<Record> = match std::fs::read(path) {
        Ok(bytes) => shoal::serde_json::from_slice(&bytes)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    all.extend_from_slice(records);
    // written beside and renamed over, so a run killed mid-write leaves the old file whole
    let bytes = shoal::serde_json::to_vec_pretty(&all)?;
    let partial = path.with_extension("json.partial");
    std::fs::write(&partial, bytes)?;
    std::fs::rename(&partial, path)?;
    Ok(())
}

/// Read every record from a set of JSON files
///
/// # Arguments
///
/// * `paths` - The files
///
/// # Errors
///
/// When a file cannot be read or holds something other than records.
pub fn read_all(paths: &[std::path::PathBuf]) -> color_eyre::Result<Vec<Record>> {
    let mut all = Vec::new();
    for path in paths {
        // each file is an array of records
        let bytes = std::fs::read(path)?;
        let records: Vec<Record> = shoal::serde_json::from_slice(&bytes)?;
        all.extend(records);
    }
    Ok(all)
}
