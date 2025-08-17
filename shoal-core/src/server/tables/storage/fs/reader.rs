//! The intent log reader for file system storage

use glommio::io::{DmaFile, OpenOptions, ReadResult};
use std::path::PathBuf;
use tracing::instrument;

use crate::server::ServerError;

/// Reads an intent log from disk
pub struct IntentLogReader {
    /// The stream to read and intent log from
    pub file: DmaFile,
    /// The size of this file
    pub size: u64,
    /// The current position of our reader
    pub position: u64,
}

impl IntentLogReader {
    /// Create a new intent log reader
    #[instrument(name = "IntentLogReader::new", err(Debug))]
    pub async fn new(path: &PathBuf) -> Result<Self, ServerError> {
        // open the target intent log or create it if it doesn't exist
        let file = OpenOptions::new().read(true).dma_open(&path).await?;
        // get the size of this file
        let size = file.file_size().await?;
        // create our intent log reader
        let reader = IntentLogReader {
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
        // Stop at the end of this file
        if self.position < self.size {
            // try to read the size of the next entry in this intent log
            let size_read = self.file.read_at(self.position, 8).await?;
            // check if we read any data
            let size = usize::from_le_bytes(size_read[..8].try_into()?);
            // if our size is bigger then our remaining data then its the end of the
            // log and we are reading padded data
            if size > (self.size - self.position) as usize {
                return Ok(None);
            }
            // increment our pos 8
            self.position += 8;
            // if size is 0 then we are done reading from this intent log
            if size == 0 {
                return Ok(None);
            }
            let row_read = self.file.read_at(self.position, size).await?;
            // increment this readers current position
            self.position += size as u64;
            Ok(Some(row_read))
        } else {
            Ok(None)
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
