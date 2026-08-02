//! The intent log reader for file system storage

use glommio::io::{DmaFile, OpenOptions, ReadResult};
use gxhash::GxHasher;
use std::hash::Hasher;
use std::path::PathBuf;
use tracing::instrument;

use super::stream::{PAD_SENTINEL, PAD_SENTINEL_SIZE};
use crate::server::ServerError;

/// Reads an intent log from disk
pub struct IntentLogReader {
    /// The stream to read and intent log from
    pub file: DmaFile,
    /// The size of this file
    pub size: u64,
    /// The current position of our reader
    pub position: u64,
    /// The direct IO alignment this intent log was written with
    pub alignment: u64,
    /// Whether this reader stopped on a damaged entry rather than the end of the log
    ///
    /// A torn or corrupt entry ends the read exactly like a clean end of log does,
    /// so without this a caller cannot tell a log it read all of from one it gave up
    /// part way through. Only the damaged cases set this - the two shapes a normally
    /// padded log ends in do not, or every clean startup would look damaged.
    pub truncated: bool,
}

impl IntentLogReader {
    /// Create a new intent log reader
    #[instrument(name = "IntentLogReader::new", err(Debug))]
    pub async fn new(path: &PathBuf) -> Result<Self, ServerError> {
        // open the target intent log or create it if it doesn't exist
        let file = OpenOptions::new().read(true).dma_open(&path).await?;
        // get the size of this file
        let size = file.file_size().await?;
        // get the alignment our pad regions were rounded up too
        let alignment = file.alignment();
        // create our intent log reader
        let reader = IntentLogReader {
            file,
            size,
            position: 0,
            alignment,
            truncated: false,
        };
        Ok(reader)
    }

    /// Get the next row of data from this intent log
    ///
    /// Intent log entry format: [8-byte size][8-byte gxhash checksum][data]
    pub async fn next_buff(&mut self) -> Result<Option<ReadResult>, ServerError> {
        // don't read a file with 0 bytes
        if self.size == 0 {
            // bail out since this shards intent log is empty
            return Ok(None);
        }
        // keep reading until we find a record or reach the end of this log
        loop {
            // stop at the end of this file
            if self.position >= self.size {
                return Ok(None);
            }
            // try to read the size of the next entry in this intent log
            let size_read = self.file.read_at(self.position, 8).await?;
            // check if we read a complete size header
            if size_read.len() < 8 {
                tracing::warn!("Truncated size header at position {} (got {} bytes) - treating as end of intent log", self.position, size_read.len());
                // we stopped on damage, not on the end of this log
                self.truncated = true;
                return Ok(None);
            }
            let raw_size = u64::from_le_bytes(size_read[..8].try_into()?);
            // check if this is a pad region written to align a partial flush
            if raw_size == PAD_SENTINEL {
                // skip to the next alignment boundary and keep looking for a record
                self.position = self
                    .position
                    .saturating_add(PAD_SENTINEL_SIZE as u64)
                    .div_ceil(self.alignment)
                    * self.alignment;
                continue;
            }
            let size = raw_size as usize;
            // if our size plus the checksum is bigger than our remaining data
            // then its the end of the log and we are reading padded data
            if size + 8 > (self.size - self.position) as usize {
                // A zero here is the unwritten tail of a partly filled log, which is
                // how a healthy log ends. Anything else is a size header whose entry
                // was never written - a torn tail rather than padding. A short read
                // lands here too, since reading past the end of a file inside an
                // already aligned block zero fills rather than returning fewer bytes.
                if size != 0 {
                    tracing::warn!(
                        "Entry at position {} claims {} bytes but only {} remain - treating as end of intent log",
                        self.position,
                        size,
                        self.size - self.position
                    );
                    // we stopped on damage, not on the end of this log
                    self.truncated = true;
                }
                return Ok(None);
            }
            // increment past the size header
            self.position += 8;
            // if size is 0 then we are done reading from this intent log
            if size == 0 {
                return Ok(None);
            }
            // read the checksum
            let checksum_read = self.file.read_at(self.position, 8).await?;
            if checksum_read.len() < 8 {
                tracing::warn!(
                    "Truncated checksum at position {} - treating as end of intent log",
                    self.position
                );
                // we stopped on damage, not on the end of this log
                self.truncated = true;
                return Ok(None);
            }
            let expected_checksum = u64::from_le_bytes(checksum_read[..8].try_into()?);
            self.position += 8;
            // read the data
            let row_read = self.file.read_at(self.position, size).await?;
            // check if we read the full entry data
            if row_read.len() < size {
                tracing::warn!("Truncated data at position {} (expected {} bytes, got {}) - treating as end of intent log", self.position, size, row_read.len());
                // we stopped on damage, not on the end of this log
                self.truncated = true;
                return Ok(None);
            }
            // verify the checksum
            let mut hasher = GxHasher::default();
            hasher.write(&row_read[..size]);
            let actual_checksum = hasher.finish();
            if expected_checksum != actual_checksum {
                tracing::warn!(
                    "Checksum mismatch at position {} (expected {:#x}, got {:#x}) - treating as end of intent log",
                    self.position, expected_checksum, actual_checksum
                );
                // we stopped on damage, not on the end of this log
                self.truncated = true;
                return Ok(None);
            }
            // increment this readers current position
            self.position += size as u64;
            return Ok(Some(row_read));
        }
    }

    /// Close this reader and its file
    ///
    /// This is recomended by glommio
    pub async fn close(self) -> Result<(), ServerError> {
        self.file.close().await?;
        Ok(())
    }
}
