//! A WAL segment written with direct writes into blocks written before it was opened
//!
//! What lets shards share a device's flushes ([`super::flush`], [F60](../../../../docs/src/features/shared-wal-flush.md)):
//! a batch written this way lands in blocks the filesystem already maps, so it needs no metadata
//! committed to be found again, and a device flush that starts after the write completed makes
//! it durable, whoever issues the flush and on whatever file.
//!
//! # Invariants
//!
//! **A segment is zero filled and synced before its first batch.** Zeros past the last frame
//! read as a torn tail, which is where recovery already stops, and a file that grows past its
//! prepared size is metadata no shared flush commits, so a batch that would write past it is
//! synced on its own file instead.
//!
//! **A prepared segment is never reused.** It is created empty and filled whatever was at its
//! path, since a file from before a crash may hold frames of an older incarnation.
//!
//! **The block a batch starts in is written again whole.** A direct write covers whole blocks,
//! so each batch rewrites the tail of the block the previous batch ended in with the same bytes
//! it already holds; a torn write of that block leaves those bytes as they were either way.

use std::io;
use std::path::Path;
use std::sync::Arc;

use glommio::io::{DmaFile, OpenOptions};

use super::flush::FlushGroup;

/// The bytes a prepared segment holds past `segment_bytes`, for the frame that crosses it
pub const SEGMENT_SLACK: u64 = 1024 * 1024;

/// The size of each zero write that prepares a segment
const PREPARE_CHUNK: usize = 1024 * 1024;

/// The alignment a batch's write is padded to, whatever smaller one the device allows
const MIN_ALIGNMENT: u64 = 4096;

/// Turn a glommio error into an io error
///
/// # Arguments
///
/// * `error` - The error
fn io<T: std::fmt::Debug>(error: glommio::GlommioError<T>) -> io::Error {
    io::Error::other(error.to_string())
}

/// Create a segment and fill it with zeros up to its capacity, durably
///
/// # Arguments
///
/// * `path` - The segment
/// * `capacity` - The bytes to fill it to
pub async fn prepare(path: &Path, capacity: u64) -> io::Result<()> {
    // created empty whatever was there, then filled
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .dma_open(path)
        .await
        .map_err(io)?;
    let mut offset = 0;
    while offset < capacity {
        // truncation cannot happen: a chunk is a mebibyte
        #[allow(clippy::cast_possible_truncation)]
        let len = (capacity - offset).min(PREPARE_CHUNK as u64) as usize;
        let mut buffer = file.alloc_dma_buffer(len);
        buffer.as_bytes_mut().fill(0);
        file.write_at(buffer, offset).await.map_err(io)?;
        offset += len as u64;
    }
    // the blocks and the size durable, and the name with them
    let synced = file.fdatasync().await.map_err(io);
    file.close().await.map_err(io)?;
    synced?;
    if let Some(dir) = path.parent() {
        super::sync_dir(dir).await?;
    }
    Ok(())
}

/// A prepared segment the writer appends to
pub struct DirectSegment {
    /// The file
    file: DmaFile,
    /// The bytes the segment was prepared to
    capacity: u64,
    /// The alignment writes are padded to
    alignment: u64,
    /// Where the block the next batch starts in begins
    tail_start: u64,
    /// The bytes already written into that block, which the next batch writes again
    tail: Vec<u8>,
    /// The device's shared flushes, when a batch's sync is shared rather than the file's own
    flushes: Option<Arc<FlushGroup>>,
}

impl DirectSegment {
    /// Open a prepared segment to append to from its start
    ///
    /// # Arguments
    ///
    /// * `path` - The segment, prepared by [`prepare`]
    /// * `capacity` - The bytes it was prepared to
    /// * `share` - Whether a batch is made durable by the device's shared flush
    pub async fn open(path: &Path, capacity: u64, share: bool) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .dma_open(path)
            .await
            .map_err(io)?;
        let alignment = file.alignment().max(MIN_ALIGNMENT);
        let flushes = share.then(|| FlushGroup::of(FlushGroup::device_of(&file)));
        Ok(DirectSegment {
            file,
            capacity,
            alignment,
            tail_start: 0,
            tail: Vec::new(),
            flushes,
        })
    }

    /// Where the next batch starts
    #[must_use]
    pub fn end(&self) -> u64 {
        self.tail_start + self.tail.len() as u64
    }

    /// Write a batch at the segment's end and make it durable
    ///
    /// Through the device's shared flush when the write stays inside the prepared blocks, and
    /// through the file's own sync when it does not.
    ///
    /// # Arguments
    ///
    /// * `base` - Where the batch starts, which has to be the segment's end
    /// * `bytes` - The batch
    pub async fn append(&mut self, base: u64, bytes: &[u8]) -> io::Result<()> {
        // the batch has to continue the segment exactly
        if base != self.end() {
            return Err(io::Error::other(format!(
                "a batch at {base} does not continue a direct segment ending at {}",
                self.end()
            )));
        }
        let end = base + bytes.len() as u64;
        // the whole blocks from the tail's through the batch's last, zero padded
        let padded = end.div_ceil(self.alignment) * self.alignment;
        // truncation cannot happen: a batch is bounded by the pending bytes
        #[allow(clippy::cast_possible_truncation)]
        let mut buffer = self
            .file
            .alloc_dma_buffer((padded - self.tail_start) as usize);
        {
            let out = buffer.as_bytes_mut();
            out[..self.tail.len()].copy_from_slice(&self.tail);
            out[self.tail.len()..self.tail.len() + bytes.len()].copy_from_slice(bytes);
            out[self.tail.len() + bytes.len()..].fill(0);
        }
        self.file
            .write_at(buffer, self.tail_start)
            .await
            .map_err(io)?;
        // the block the next batch starts in, and what of it this one wrote
        let next_start = end / self.alignment * self.alignment;
        // truncation cannot happen: both are inside the batch just written
        #[allow(clippy::cast_possible_truncation)]
        let tail = if next_start >= base {
            bytes[(next_start - base) as usize..].to_vec()
        } else {
            // the batch ended in the block it started in, after the old tail
            let mut tail = std::mem::take(&mut self.tail);
            tail.extend_from_slice(bytes);
            tail
        };
        self.tail_start = next_start;
        self.tail = tail;
        // durable: a shared device flush inside the prepared blocks, the file's own past them
        // or when the flush is not shared
        match &self.flushes {
            Some(flushes) if padded <= self.capacity => flushes.sync(&self.file).await,
            _ => self.file.fdatasync().await.map_err(io),
        }
    }

    /// Seal the segment: cut it to what was written and sync that, then close it
    pub async fn seal(self) -> io::Result<()> {
        // the zeros past the last frame are dropped, so a sealed segment is only its frames
        let end = self.end();
        let truncated = self.file.truncate(end).await.map_err(io);
        let synced = match truncated {
            Ok(()) => self.file.fdatasync().await.map_err(io),
            Err(error) => Err(error),
        };
        self.file.close().await.map_err(io)?;
        synced
    }

    /// Sync and close the segment as it stands, for a store that is closing
    pub async fn close(self) -> io::Result<()> {
        let synced = self.file.fdatasync().await.map_err(io);
        self.file.close().await.map_err(io)?;
        synced
    }
}
