//! Utilities to efficiently and performantly streams data to and from  disk
use glommio::io::{DmaBuffer, DmaFile, OpenOptions};
use glommio::task::JoinHandle;
use kanal::AsyncSender;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::rc::Rc;
use tracing::instrument;

use crate::server::messages::ServerMsg;
use crate::server::ServerError;
use crate::shared::traits::ShoalDatabase;

pub struct StreamWriterBuilder<D: ShoalDatabase> {
    /// The table we are writting data for
    pub table: D::TableNames,
    /// The path to write data too
    pub path: PathBuf,
    /// The channel to send write messages over
    pub shard_local_tx: AsyncSender<ServerMsg<D>>,
    /// The default/minumum size to make our DMA buffer
    pub buffer_size: usize,
    /// Maximum number of in-flight write tasks
    pub write_behind: usize,
}

impl<D: ShoalDatabase> StreamWriterBuilder<D> {
    /// Create a new [`StreamWriterBuilder`]
    ///
    /// # Arguments
    ///
    /// * `path` - The path this stream writer should write data too when built
    pub fn new(
        table: D::TableNames,
        path: impl Into<PathBuf>,
        shard_local_tx: AsyncSender<ServerMsg<D>>,
    ) -> Self {
        StreamWriterBuilder {
            table,
            path: path.into(),
            shard_local_tx,
            buffer_size: 4096,
            write_behind: 4,
        }
    }

    /// Set the buffer size to use by default
    ///
    /// # Arguments
    ///
    /// * `buffer_size` - The buffer size to set in bytes
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set the number of write behind buffers to use
    ///
    /// # Arguments
    ///
    /// * `write_behind` - The number of write behind buffers to use
    pub fn write_behind(mut self, write_behind: usize) -> Self {
        self.write_behind = write_behind;
        self
    }

    /// Build a [`StreamWriter`] from this [`StreamWriterBuilder`]
    #[instrument(name = "StreamWriterBuilder::build", skip_all)]
    pub async fn build(self) -> Result<StreamWriter<D>, ServerError> {
        // open this file
        // don't open with append or new writes will overwrite old ones
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&self.path)
            .await?;
        // get a buffer to write
        let buffer = file.alloc_dma_buffer(self.buffer_size);
        // build this stream writer
        let writer = StreamWriter {
            table: self.table,
            path: self.path,
            file: Rc::new(file),
            shard_local_tx: self.shard_local_tx,
            buffer,
            default_buffer_size: self.buffer_size,
            file_pos: 0,
            buff_pos: 0,
            flushed_pos: 0,
            max_write_behind: self.write_behind,
            pending_writes: VecDeque::with_capacity(self.write_behind),
            pending_sync: None,
        };
        Ok(writer)
    }
}

/// Helps a StreamWriter write a block of data to disk
async fn write_helper<D: ShoalDatabase>(
    table: D::TableNames,
    file: Rc<DmaFile>,
    buff: DmaBuffer,
    pos: u64,
    shard_local_tx: AsyncSender<ServerMsg<D>>,
) {
    // write this buffer to disk
    file.write_at(buff, pos).await.unwrap();
    // build a server message saying data has been flushed to disk
    let msg = ServerMsg::DataFlushed {
        table,
        flushed: pos,
    };
    // tell our shard some data has been written to disk
    shard_local_tx.send(msg).await.unwrap()
}

/// A streaming writer that utilizes high queue depth DMA to have efficient
/// and performant IO.
pub struct StreamWriter<D: ShoalDatabase> {
    /// The table we are writting data for
    pub table: D::TableNames,
    /// The path to write data too
    pub path: PathBuf,
    /// The file we are streaming data too
    file: Rc<DmaFile>,
    /// The channel to send write messages over
    shard_local_tx: AsyncSender<ServerMsg<D>>,
    /// The current buffer we are writting too
    buffer: DmaBuffer,
    /// The default/minumum size to make our DMA buffer
    default_buffer_size: usize,
    /// The current position we have written data in our file up too
    file_pos: u64,
    /// The current position we have written data in our buffer up too
    buff_pos: usize,
    /// The position of data that has been flushed to disk
    flushed_pos: u64,
    /// Maximum number of in-flight write tasks
    max_write_behind: usize,
    /// Handles for in-flight write tasks
    pending_writes: VecDeque<JoinHandle<()>>,
    /// Handle for background fdatasync task
    pending_sync: Option<JoinHandle<()>>,
}

impl<D: ShoalDatabase> StreamWriter<D> {
    /// Create a new stream writer
    pub fn builder(
        table: D::TableNames,
        path: impl Into<PathBuf>,
        shard_local_tx: AsyncSender<ServerMsg<D>>,
    ) -> StreamWriterBuilder<D> {
        StreamWriterBuilder::new(table, path, shard_local_tx)
    }

    /// If at write-behind capacity, await the oldest pending write
    async fn flush_oldest_write(&mut self) {
        if self.pending_writes.len() >= self.max_write_behind {
            if let Some(handle) = self.pending_writes.pop_front() {
                handle.await;
            }
        }
    }

    /// Await all pending write tasks
    async fn drain_pending_writes(&mut self) {
        while let Some(handle) = self.pending_writes.pop_front() {
            handle.await;
        }
    }

    /// Write our current buffer to our WAL via a background task
    async fn write(&mut self, new_size: usize) -> Result<(), ServerError> {
        // back-pressure: if at capacity, await the oldest pending write
        self.flush_oldest_write().await;
        // allocate our next buffer
        let mut buff = self.file.alloc_dma_buffer(new_size);
        // swap our new buffer with our old one
        std::mem::swap(&mut self.buffer, &mut buff);
        // trim our buffer down to only the data we wrote data too
        buff.trim_to_size(self.buff_pos);
        // get a local copy of our table, file, position, and shard channel
        let table = self.table;
        let file = self.file.clone();
        let pos = self.file_pos;
        let shard_local_tx = self.shard_local_tx.clone();
        // spawn the write as a background task
        let handle = glommio::spawn_local(async move {
            // write this data to disk and tell our shard when its done
            write_helper(table, file, buff, pos, shard_local_tx).await
        })
        .detach();
        self.pending_writes.push_back(handle);
        // increment our file position
        self.file_pos += self.buff_pos as u64;
        // reset our buff position
        self.buff_pos = 0;
        Ok(())
    }

    /// Make sure we have enough space to fully store this next write
    ///
    /// # Arguments
    ///
    /// * `size` - The size to prep for
    pub async fn prep(&mut self, size: usize) -> &mut [u8] {
        // if we don't have enough space for our buffer then write our current buffer
        if self.buffer.len() < size + self.buff_pos {
            // we won't have enough space to write this new data to out buffer so get a new one
            // make this new buffer big enough for our next write or bigger
            let new_size = std::cmp::max(self.default_buffer_size, size);
            // write but not sync our current buffer to disk
            self.write(new_size).await.unwrap();
        }
        // get a mutable ref to our buffer
        &mut self.buffer.as_bytes_mut()[self.buff_pos..self.buff_pos + size]
    }

    /// Consume some data from our buffer
    ///
    /// # Arguments
    ///
    /// * `size` - The number of bytes that have been consumed
    pub async fn consume(&mut self, size: usize) {
        // increment our buffer position by the amount of data consumed
        self.buff_pos += size;
        // if we have consumed our entire buffer then write it to disk
        if self.buffer.len() <= self.buff_pos {
            // write but not sync our current buffer to disk
            self.write(self.default_buffer_size).await.unwrap();
        }
    }

    /// Get the current flushed position for this stream writer
    pub fn get_flushed_pos(&mut self) -> u64 {
        self.flushed_pos
    }

    /// Update the offset in our writer for how much data has been flushed to disk
    ///
    /// # Arguments
    ///
    /// * `flushed_pos` - The new flushed offset to set
    pub fn set_flushed(&mut self, flushed_pos: u64) {
        self.flushed_pos = flushed_pos;
    }

    /// Get the current unflushed position for this stream writer
    pub fn get_unflushed_pos(&self) -> u64 {
        // unflushed is our file position + buffer position
        self.file_pos + self.buff_pos as u64
    }

    /// Sync any in flight buffers to disk in a non blocking manner
    ///
    /// All writes will not be durably written until the corresponding
    /// [`ServerMsg::DataFlushed`] is recieved by this shard.
    pub async fn sync(&mut self) -> Result<(), ServerError> {
        if self.buff_pos > 0 {
            self.write(self.default_buffer_size).await.unwrap();
        }
        Ok(())
    }

    /// Sync our files data, blocking until durable (for WAL acknowledgment)
    pub async fn sync_blocking(&mut self) -> Result<(), ServerError> {
        if self.buff_pos > 0 {
            self.write(self.default_buffer_size).await.unwrap();
        }
        self.drain_pending_writes().await;
        if let Some(handle) = self.pending_sync.take() {
            handle.await;
        }
        self.file.fdatasync().await?;
        Ok(())
    }

    /// Rename our current backing file and start writing to a new one
    ///
    /// # Arguments
    ///
    /// * `inactive_path` - The path to rename our current backing file to
    pub async fn refresh(&mut self, rename_to: &PathBuf) -> Result<u64, ServerError> {
        // flush this intent log
        self.sync_blocking().await?;
        // get the current flushed position
        let flushed_pos = self.get_flushed_pos();
        // rename our old intent log
        glommio::io::rename(&self.path, &rename_to).await?;
        // get our parent dir and sync it so this rename is durable
        if let Some(parent) = rename_to.parent() {
            // open our parent dir
            let dir = glommio::io::Directory::open(parent).await?;
            // fsync the parent directory to ensure the rename is durable
            dir.sync().await?;
            // close our now durably synced parent dir
            dir.close().await?;
        }
        // open this file
        // don't open with append or new writes will overwrite old ones
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&self.path)
            .await?;
        // get a buffer to write
        self.buffer = file.alloc_dma_buffer(self.default_buffer_size);
        // replace our old file with out new one
        let old_file = std::mem::replace(&mut self.file, Rc::new(file));
        // close our old file
        old_file.close_rc().await?;
        // reset our position counters
        self.file_pos = 0;
        self.buff_pos = 0;
        self.flushed_pos = 0;
        Ok(flushed_pos)
    }

    /// Close this writer
    pub async fn close(mut self) -> Result<AsyncSender<ServerMsg<D>>, ServerError> {
        if let Some(handle) = self.pending_sync.take() {
            handle.await;
        }
        self.drain_pending_writes().await;
        if self.buff_pos > 0 {
            self.write(self.default_buffer_size).await?;
            self.drain_pending_writes().await;
        }
        self.file.close_rc().await?;
        Ok(self.shard_local_tx)
    }
}
