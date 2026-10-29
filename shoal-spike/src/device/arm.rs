//! The disk's population for X7's cells that share one arm: chunks a foreground reads, applies
//! write into and a scrub reads
//!
//! A slice holds chunks under many placement groups, so the population is spread over sixty-four
//! of them, each a directory, as S6 lays them out. Where a filesystem puts them is its own
//! business: XFS gives every new directory the next allocation group, so the chunks spread over
//! the platter, while ext4 keeps a directory that is not near the root close to its parent. Every
//! figure that reads the population therefore carries its span: where its chunks lie, as a
//! fraction of the device, which is the seek a figure was measured across. Each chunk's extents
//! are mapped once, so an apply can be ordered by where its bytes lie on the disk.

use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use glommio::io::DmaFile;

use super::io::{self, Payloads, HEADER};
use super::sys::{self, Extent};

/// A chunk's units
pub const CHUNK: u64 = 4 << 20;

/// The placement groups the population is spread over
pub const PGS: usize = 64;

/// The path of chunk `index` of a population: its own object, in one of the placement groups
///
/// # Arguments
///
/// * `root` - The population's directory
/// * `index` - The chunk
#[must_use]
pub fn chunk_path(root: &Path, index: usize) -> PathBuf {
    root.join(format!("pg{:02}", index % PGS))
        .join(format!("{index:016x}"))
        .join("000000.0")
}

/// Make a population of whole chunks if it is not there, each written and synced
///
/// # Arguments
///
/// * `root` - Its directory
/// * `count` - Chunks in it
pub async fn populate(root: PathBuf, count: usize) {
    let marker = root.join("complete");
    if marker.exists() {
        return;
    }
    io::wipe(&root);
    let payloads = Payloads::new(0xa7a7);
    // every placement group first, so each takes its place before any chunk is written
    for pg in 0..PGS {
        std::fs::create_dir_all(root.join(format!("pg{pg:02}"))).expect("made");
    }
    for index in 0..count {
        let path = chunk_path(&root, index);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("made");
        let file = io::open(&path, true).await;
        io::write_chunk(&file, &payloads, CHUNK, 0).await;
        file.fdatasync().await.expect("synced");
        file.close().await.expect("closed");
    }
    let _ = sys::syncfs(&root);
    std::fs::write(&marker, count.to_string()).expect("marked");
}

/// A population held open, with where each chunk lies
pub struct Arm {
    /// Every chunk, open for reading and writing
    pub files: Vec<DmaFile>,
    /// Each chunk's extents
    pub extents: Vec<Vec<Extent>>,
    /// Each chunk's inode, which orders chunks roughly as the filesystem placed them
    pub inodes: Vec<u64>,
}

impl Arm {
    /// Open every chunk of a population and map where it lies
    ///
    /// # Arguments
    ///
    /// * `root` - The population's directory
    /// * `count` - Chunks in it
    pub async fn open(root: &Path, count: usize) -> Arm {
        let mut files = Vec::with_capacity(count);
        let mut extents = Vec::with_capacity(count);
        let mut inodes = Vec::with_capacity(count);
        for index in 0..count {
            let path = chunk_path(root, index);
            let file = io::open(&path, false).await;
            extents.push(sys::extent_map(file.as_raw_fd()).unwrap_or_default());
            inodes.push(std::fs::metadata(&path).map_or(0, |meta| meta.ino()));
            files.push(file);
        }
        Arm { files, extents, inodes }
    }

    /// Where a byte of a chunk lies on the device, or zero if it is not mapped
    ///
    /// # Arguments
    ///
    /// * `chunk` - The chunk
    /// * `offset` - The byte in it
    #[must_use]
    pub fn physical(&self, chunk: usize, offset: u64) -> u64 {
        sys::physical_of(&self.extents[chunk], offset).unwrap_or(0)
    }

    /// Where the population lies, as fractions of the device
    ///
    /// # Arguments
    ///
    /// * `device_bytes` - The size of the filesystem's device
    #[must_use]
    pub fn span(&self, device_bytes: u64) -> Span {
        let starts: Vec<u64> = (0..self.files.len()).map(|chunk| self.physical(chunk, HEADER)).collect();
        Span::of(&starts, device_bytes)
    }

    /// Close every chunk
    pub async fn close(self) {
        for file in self.files {
            file.close().await.expect("closed");
        }
    }
}

/// Where a set of files lies on a device
#[derive(Debug, Clone, Copy, Default)]
pub struct Span {
    /// The 5th percentile of their starts, as a fraction of the device
    pub p5: f64,
    /// The 95th percentile
    pub p95: f64,
    /// GiB between the two
    pub gib: f64,
}

impl Span {
    /// The span of a set of starting offsets
    ///
    /// # Arguments
    ///
    /// * `starts` - Each file's first byte on the device
    /// * `device_bytes` - The device's size
    #[must_use]
    pub fn of(starts: &[u64], device_bytes: u64) -> Span {
        if starts.is_empty() || device_bytes == 0 {
            return Span::default();
        }
        // sorted, so the percentiles can be read off
        let mut sorted = starts.to_vec();
        sorted.sort_unstable();
        let at = |p: f64| sorted[((p * (sorted.len() - 1) as f64).round() as usize).min(sorted.len() - 1)];
        let (low, high) = (at(0.05), at(0.95));
        Span {
            p5: low as f64 / device_bytes as f64,
            p95: high as f64 / device_bytes as f64,
            gib: (high - low) as f64 / f64::from(1 << 30),
        }
    }

    /// The span's figures, for a record
    #[must_use]
    pub fn figures(&self) -> [(&'static str, f64); 3] {
        [("span_p5", self.p5), ("span_p95", self.p95), ("span_gib", self.gib)]
    }

    /// The span in words, for a table's title
    #[must_use]
    pub fn show(&self) -> String {
        format!(
            "population from {:.1}% to {:.1}% of the device, {:.0} GiB apart (p5 to p95)",
            self.p5 * 100.0,
            self.p95 * 100.0,
            self.gib
        )
    }
}

/// The span of files given by their paths, each mapped once
///
/// # Arguments
///
/// * `paths` - The files
/// * `device_bytes` - The device's size
#[must_use]
pub fn span_of_paths(paths: &[PathBuf], device_bytes: u64) -> Span {
    let starts: Vec<u64> = paths
        .iter()
        .filter_map(|path| {
            let file = std::fs::File::open(path).ok()?;
            let extents = sys::extent_map(file.as_raw_fd()).ok()?;
            extents.first().map(|extent| extent.physical)
        })
        .collect();
    Span::of(&starts, device_bytes)
}
