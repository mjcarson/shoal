//! Utilities to efficiently and performantly streams data to and from  disk
use futures::AsyncWriteExt;
use kanal::AsyncSender;
use std::collections::VecDeque;
use std::io::Write;
use std::path::PathBuf;
use std::rc::Rc;

use glommio::io::{DmaBuffer, DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use glommio::task::JoinHandle;
use glommio::LocalExecutorBuilder;

use crate::server::messages::ServerMsg;
use crate::shared::traits::ShoalDatabase;

const DATA: &[u8] = "Hello, World!".as_bytes();
const ITERATIONS: usize = 100_000;
const SYNC: usize = 100;

pub struct StreamWriterBuilder<D: ShoalDatabase> {
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
    pub fn new(path: impl Into<PathBuf>, shard_local_tx: AsyncSender<ServerMsg<D>>) -> Self {
        StreamWriterBuilder {
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
    pub async fn build(self) -> StreamWriter<D> {
        // open this file
        // don't open with append or new writes will overwrite old ones
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .dma_open(&self.path)
            .await
            .unwrap();
        // get a buffer to write
        let buffer = file.alloc_dma_buffer(512);
        // build this stream writer
        StreamWriter {
            file: Rc::new(file),
            shard_local_tx: self.shard_local_tx,
            buffer,
            default_buffer_size: self.buffer_size,
            file_pos: 0,
            buff_pos: 0,
            max_write_behind: self.write_behind,
            pending_writes: VecDeque::with_capacity(self.write_behind),
            pending_sync: None,
        }
    }
}

/// A streaming writer that utilizes high queue depth DMA to have efficient
/// and performant IO.
pub struct StreamWriter<D: ShoalDatabase> {
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
        path: impl Into<PathBuf>,
        shard_local_tx: AsyncSender<ServerMsg<D>>,
    ) -> StreamWriterBuilder<D> {
        StreamWriterBuilder::new(path, shard_local_tx)
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
    async fn write(&mut self, new_size: usize) {
        // back-pressure: if at capacity, await the oldest pending write
        self.flush_oldest_write().await;
        // allocate our next buffer
        let mut buff = self.file.alloc_dma_buffer(new_size);
        // swap our new buffer with our old one
        std::mem::swap(&mut self.buffer, &mut buff);
        // trim our buffer down to only the data we wrote data too
        buff.trim_to_size(self.buff_pos);
        // capture values for the spawned task
        let file = self.file.clone();
        let pos = self.file_pos;
        // spawn the write as a background task
        let handle = glommio::spawn_local(async move {
            file.write_at(buff, pos).await.unwrap();
        })
        .detach();
        self.pending_writes.push_back(handle);
        // increment our file position
        self.file_pos += self.buff_pos as u64;
        // reset our buff position
        self.buff_pos = 0;
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
            self.write(new_size).await;
        }
        // get a mutable ref to our buffer
        &mut self.buffer.as_bytes_mut()[self.buff_pos..self.buff_pos + size]
    }

    /// Consume some data from our buffer
    pub async fn consume(&mut self, size: usize) {
        // increment our buffer position by the amount of data consumed
        self.buff_pos += size;
        // if we have consumed our entire buffer then write it to disk
        if self.buffer.len() <= self.buff_pos {
            // write but not sync our current buffer to disk
            self.write(self.default_buffer_size).await;
        }
    }

    /// Sync our files data, blocking until durable (for WAL acknowledgment)
    pub async fn sync(&mut self) {
        if self.buff_pos > 0 {
            self.write(self.default_buffer_size).await;
        }
        self.drain_pending_writes().await;
        if let Some(handle) = self.pending_sync.take() {
            handle.await;
        }
        self.file.fdatasync().await.unwrap();
    }

    /// Sync our files data in the background (non-blocking)
    pub async fn sync_background(&mut self) {
        if self.buff_pos > 0 {
            self.write(self.default_buffer_size).await;
        }
        self.drain_pending_writes().await;
        if let Some(handle) = self.pending_sync.take() {
            handle.await;
        }
        let file = self.file.clone();
        self.pending_sync = Some(
            glommio::spawn_local(async move {
                file.fdatasync().await.unwrap();
            })
            .detach(),
        );
    }

    /// Close this writer
    pub async fn close(mut self) {
        if let Some(handle) = self.pending_sync.take() {
            handle.await;
        }
        self.drain_pending_writes().await;
        if self.buff_pos > 0 {
            self.write(self.default_buffer_size).await;
            self.drain_pending_writes().await;
        }
        self.file.close_rc().await.unwrap();
    }
}
