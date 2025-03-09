//! The intent log reader for file system storage

use glommio::io::{DmaFile, OpenOptions, ReadResult};
use std::path::PathBuf;
use tracing::instrument;

use crate::server::ServerError;

/// Reads an intent log from disk
pub struct IntentLogReader {
    path: PathBuf,
    /// The stream to read and intent log from
    file: DmaFile,
    /// The size of this file
    size: u64,
    /// The current position of our reader
    position: u64,
}

impl IntentLogReader {
    /// Create a new intent log reader
    #[instrument(name = "IntentLogReader::new", err(Debug))]
    pub async fn new(path: &PathBuf) -> Result<Self, ServerError> {
        // open the target intent log or create it if it doesn't exist
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&path)
            .await?;
        // get the size of this file
        let size = file.file_size().await?;
        // create our intent log reader
        let reader = IntentLogReader {
            path: path.clone(),
            file,
            size,
            position: 0,
        };
        Ok(reader)
    }

    /// Get the next row of data from this intent log
    pub async fn next_buff(&mut self) -> Result<Option<ReadResult>, ServerError> {
        // don't read a file with 0 bytes
        if self.size == 0 {
            // bail out since this shards intent log is empty
            return Ok(None);
        }
        // check if we are at the end of this file
        if self.position < self.size {
            // try to read the size of the next entry in this intent log
            let size_read = self.file.read_at(self.position, 8).await?;
            // check if we read any data
            let size = usize::from_le_bytes(size_read[..8].try_into()?);
            // increment our pos 8
            self.position += 8;
            // if size is 0 then skip to the next read
            if size == 0 {
                return Ok(None);
            }
            let row_read = self.file.read_at(self.position, size).await?;
            // increment this readers current position
            self.position += size as u64;
            //Ok(Some(archived))
            Ok(Some(row_read))
        } else {
            Ok(None)
        }
    }
}
