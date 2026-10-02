//! Stream the rows of one dataset file, one at a time, whatever its format
//!
//! A dataset file can be larger than the memory of the machine reading it (TMDB's is 538 MB), so
//! nothing here holds more than one row. A JSON array is walked element by element rather than
//! parsed whole, which is the one format serde's defaults would buffer.
//!
//! A row that does not parse is handed to the caller as an error with where it was, and the walk
//! carries on: the same file always fails on the same rows, so skipping them is deterministic.

use serde::de::{DeserializeOwned, SeqAccess, Visitor};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::path::Path;

use crate::dataset::Format;

/// A row that did not parse, and where it was
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowError {
    /// Where in the file the row was: a line, or an element of the array
    pub position: String,
    /// What the parser said
    pub message: String,
}

impl std::fmt::Display for RowError {
    /// Write the position and the parser's message
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.position, self.message)
    }
}

/// What reading a whole file came to
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileFacts {
    /// How many bytes the file held
    pub bytes: u64,
    /// The sha256 of those bytes, as hex
    pub sha256: String,
}

/// A reader that hashes and counts every byte read through it
struct Hashing<'a, R> {
    /// The reader underneath
    inner: R,
    /// The digest of every byte read so far
    hasher: &'a mut Sha256,
    /// How many bytes were read so far
    bytes: &'a mut u64,
}

impl<R: Read> Read for Hashing<'_, R> {
    /// Read from the inner reader and fold what was read into the digest
    ///
    /// # Arguments
    ///
    /// * `buf` - Where to read to
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // read, then account for exactly what was read
        let read = self.inner.read(buf)?;
        self.hasher.update(&buf[..read]);
        *self.bytes += read as u64;
        Ok(read)
    }
}

/// Read every row of a file, handing each to a sink until it says to stop
///
/// The bytes are hashed as they are read, so a whole walk also digests the file. A walk the sink
/// stopped early digests only what it read, and says so by returning `None`.
///
/// # Arguments
///
/// * `path` - The file to read
/// * `format` - What format it is in
/// * `sink` - Handed each row or the error it failed with, in file order
///
/// # Errors
///
/// When the file cannot be opened or read, or a JSON file is not an array at all.
pub fn for_each_row<R, F>(
    path: &Path,
    format: Format,
    mut sink: F,
) -> std::io::Result<Option<FileFacts>>
where
    R: DeserializeOwned,
    F: FnMut(Result<R, RowError>) -> ControlFlow<()>,
{
    // every byte read is hashed and counted, through a buffer so the hash sees large reads
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let file = File::open(path)?;
    let hashing = Hashing {
        inner: file,
        hasher: &mut hasher,
        bytes: &mut bytes,
    };
    let reader = BufReader::with_capacity(1 << 20, hashing);
    // walk the rows in the format the file is in
    let finished = match format {
        Format::Csv => csv_rows(reader, &mut sink)?,
        Format::Jsonl => jsonl_rows(reader, &mut sink)?,
        Format::Json => json_rows(reader, &mut sink)?,
    };
    // a walk that was stopped early has not read the whole file, so it has no digest of it
    if !finished {
        return Ok(None);
    }
    Ok(Some(FileFacts {
        bytes,
        sha256: hex(&hasher.finalize()),
    }))
}

/// Write bytes as lowercase hex
///
/// # Arguments
///
/// * `bytes` - The bytes to write
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    // two characters a byte
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Read a csv with a header row, one record a row
///
/// # Arguments
///
/// * `reader` - The file
/// * `sink` - Handed each row
///
/// Returns whether the whole file was read.
fn csv_rows<R, F>(reader: impl Read, sink: &mut F) -> std::io::Result<bool>
where
    R: DeserializeOwned,
    F: FnMut(Result<R, RowError>) -> ControlFlow<()>,
{
    // the header row names the fields, the way serde reads a struct
    let mut csv = csv::ReaderBuilder::new().has_headers(true).from_reader(reader);
    for (index, record) in csv.deserialize::<R>().enumerate() {
        // a record that does not parse names its line, or its record number if it has none
        let row = record.map_err(|error| RowError {
            position: error
                .position()
                .map_or_else(|| format!("record {}", index + 1), |pos| format!("line {}", pos.line())),
            message: error.to_string(),
        });
        // an i/o error is not a bad row, it is a file that cannot be read
        if let Err(error) = &row {
            if error.message.starts_with("I/O error") {
                return Err(std::io::Error::other(error.message.clone()));
            }
        }
        if sink(row).is_break() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Read json lines, one object a line, skipping blank lines
///
/// # Arguments
///
/// * `reader` - The file
/// * `sink` - Handed each row
///
/// Returns whether the whole file was read.
fn jsonl_rows<R, F>(reader: impl BufRead, sink: &mut F) -> std::io::Result<bool>
where
    R: DeserializeOwned,
    F: FnMut(Result<R, RowError>) -> ControlFlow<()>,
{
    for (index, line) in reader.lines().enumerate() {
        // a line that cannot be read is a file that cannot be read
        let line = line?;
        // a blank line holds no row, which is how most writers end the file
        if line.trim().is_empty() {
            continue;
        }
        let row = serde_json::from_str::<R>(&line).map_err(|error| RowError {
            position: format!("line {}", index + 1),
            message: error.to_string(),
        });
        if sink(row).is_break() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Walks a json array an element at a time, handing each element to a sink as a row
struct Elements<'s, R, F> {
    /// Handed each row
    sink: &'s mut F,
    /// Whether the sink asked to stop
    stopped: &'s mut bool,
    /// The row type each element is parsed as
    row: PhantomData<R>,
}

impl<'de, R, F> Visitor<'de> for Elements<'_, R, F>
where
    R: DeserializeOwned,
    F: FnMut(Result<R, RowError>) -> ControlFlow<()>,
{
    /// Nothing is built: every element is handed on
    type Value = ();

    /// Say what a dataset json file must be
    ///
    /// # Arguments
    ///
    /// * `formatter` - Where to say it
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a json array of rows")
    }

    /// Hand each element on as it is read
    ///
    /// # Arguments
    ///
    /// * `seq` - The array's elements
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        // each element is read as a value first, so a row that does not fit the type is one bad
        // row rather than the end of the walk
        let mut index = 0usize;
        while let Some(value) = seq.next_element::<serde_json::Value>()? {
            let row = R::deserialize(value).map_err(|error| RowError {
                position: format!("element {index}"),
                message: error.to_string(),
            });
            index += 1;
            if (self.sink)(row).is_break() {
                *self.stopped = true;
                return Ok(());
            }
        }
        Ok(())
    }
}

/// Read a json array of rows, one element at a time
///
/// # Arguments
///
/// * `reader` - The file
/// * `sink` - Handed each row
///
/// Returns whether the whole file was read.
fn json_rows<R, F>(reader: impl Read, sink: &mut F) -> std::io::Result<bool>
where
    R: DeserializeOwned,
    F: FnMut(Result<R, RowError>) -> ControlFlow<()>,
{
    use serde::Deserializer as _;
    // a streaming deserializer over the file, never the whole of it in memory
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let mut stopped = false;
    let walked = (&mut deserializer).deserialize_seq(Elements {
        sink,
        stopped: &mut stopped,
        row: PhantomData,
    });
    // a walk the sink stopped leaves the array unclosed, which is not the file's fault
    if stopped {
        return Ok(false);
    }
    // anything else wrong with the array is a file that is not a dataset
    walked.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    deserializer
        .end()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::{for_each_row, RowError};
    use crate::dataset::Format;
    use std::ops::ControlFlow;

    /// A row the tests read
    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct Row {
        /// A key
        id: u64,
        /// A value
        name: String,
    }

    /// Write a file into a scratch directory and read every row of it
    ///
    /// # Arguments
    ///
    /// * `format` - The format to read it as
    /// * `body` - What the file holds
    fn read_all(format: Format, body: &str) -> (Vec<Result<Row, RowError>>, Option<super::FileFacts>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Row.data");
        std::fs::write(&path, body).unwrap();
        let mut rows = Vec::new();
        let facts = for_each_row::<Row, _>(&path, format, |row| {
            rows.push(row);
            ControlFlow::Continue(())
        })
        .unwrap();
        (rows, facts)
    }

    /// The three formats read the same rows, and a bad row is one error in place
    #[test]
    fn every_format_reads_rows_and_keeps_going_past_a_bad_one() {
        let csv = "id,name\n1,a\nnope,b\n3,c\n";
        let jsonl = "{\"id\":1,\"name\":\"a\"}\n{\"id\":\"nope\",\"name\":\"b\"}\n\n{\"id\":3,\"name\":\"c\"}\n";
        let json = "[{\"id\":1,\"name\":\"a\"},{\"id\":\"nope\",\"name\":\"b\"},{\"id\":3,\"name\":\"c\"}]";
        for (format, body, bad) in [
            (Format::Csv, csv, "line 3"),
            (Format::Jsonl, jsonl, "line 2"),
            (Format::Json, json, "element 1"),
        ] {
            let (rows, facts) = read_all(format, body);
            assert_eq!(rows.len(), 3, "{format:?}");
            assert_eq!(rows[0].as_ref().unwrap(), &Row { id: 1, name: "a".into() });
            assert_eq!(rows[1].as_ref().unwrap_err().position, bad, "{format:?}");
            assert_eq!(rows[2].as_ref().unwrap(), &Row { id: 3, name: "c".into() });
            // a whole walk digests every byte of the file
            let facts = facts.expect("a whole walk has a digest");
            assert_eq!(facts.bytes, body.len() as u64);
            assert_eq!(facts.sha256.len(), 64);
        }
    }

    /// A walk the sink stops reads no further and has no digest
    #[test]
    fn a_stopped_walk_has_no_digest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Row.json");
        std::fs::write(&path, "[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]").unwrap();
        let mut seen = 0;
        let facts = for_each_row::<Row, _>(&path, Format::Json, |_| {
            seen += 1;
            ControlFlow::Break(())
        })
        .unwrap();
        assert_eq!(seen, 1);
        assert_eq!(facts, None);
    }

    /// A json file that is not an array is refused, not read as no rows
    #[test]
    fn a_json_file_that_is_not_an_array_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Row.json");
        std::fs::write(&path, "{\"id\":1}").unwrap();
        let read = for_each_row::<Row, _>(&path, Format::Json, |_| ControlFlow::Continue(()));
        assert!(read.is_err());
    }

    /// A json array is not held in memory: an element is handed on before the next is read
    #[test]
    fn a_json_array_is_streamed() {
        // an array whose tail is not valid json: a buffering parse would fail before any row
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("Row.json");
        std::fs::write(&path, "[{\"id\":1,\"name\":\"a\"}, this is not json").unwrap();
        let mut first = None;
        let _ = for_each_row::<Row, _>(&path, Format::Json, |row| {
            first.get_or_insert(row.map(|row| row.id).ok());
            ControlFlow::Continue(())
        });
        assert_eq!(first, Some(Some(1)));
    }
}
