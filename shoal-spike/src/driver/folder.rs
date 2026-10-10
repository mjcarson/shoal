//! Section 4: a folder of real files, the dataset's other shape, read on one core
//!
//! A folder dataset's bytes are not made but read from the driver host's own device. Files of
//! 64 KiB, 1 MiB and 64 MiB are written once and synced, then each side reads every file of a size
//! in 1 MiB reads: alone, with each read's CRC-64/NVME, or through SHA-256 a file at a time, which
//! is the digest F66 takes of a table's file at its scan. *Cold* drops the files from the page cache
//! first with `posix_fadvise`, which needs no root, and the device's own counters say whether the
//! bytes came from the device; *hot* reads them from the cache, which is the cpu's cost alone.

use std::fs::File;
use std::hint::black_box;
use std::io::Read;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Instant;

use sha2::{Digest, Sha256};

use super::dataset::{FolderDataset, ObjectDataset};
use super::generate::{Generator, SplitMix};
use super::make::crc;
use super::Ctx;
use crate::device::counters::Devices;
use crate::device::facts::Facts;
use crate::device::{ordered, size_name, sys, SideOut};

/// The sizes of the folder's files
pub const SIZES: &[u64] = &[64 << 10, 1 << 20, 64 << 20];

/// The size of each read
const READ: usize = 1 << 20;

/// The files of one size, written if they are not there at their size already
///
/// # Arguments
///
/// * `root` - The folder
/// * `size` - The files' size
/// * `total` - The bytes the files of this size hold together
/// * `seed` - The seed their bytes are made from
fn prepare(root: &Path, size: u64, total: u64, seed: u64) -> Vec<PathBuf> {
    let dir = root.join(size_name(size));
    std::fs::create_dir_all(&dir).expect("the folder is made");
    let count = (total / size).max(1);
    let generator = SplitMix::new(seed);
    let mut written = false;
    let mut buf = vec![0u8; size as usize];
    let paths: Vec<PathBuf> = (0..count)
        .map(|index| {
            // a file already its size is kept, so rounds share one folder
            let path = dir.join(format!("{index:06}"));
            if std::fs::metadata(&path).map(|meta| meta.len()).ok() != Some(size) {
                generator.fill((size << 32) | index, 0, &mut buf);
                std::fs::write(&path, &buf).expect("a file is written");
                written = true;
            }
            path
        })
        .collect();
    // everything written reaches the device, so a cold read's pages are clean and can be dropped
    if written {
        sys::syncfs(&dir).expect("the folder syncs");
    }
    paths
}

/// What a side does with each read
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Use {
    /// Nothing
    Read,
    /// Takes its CRC
    Crc,
    /// Feeds it to the file's SHA-256
    Sha256,
}

impl Use {
    /// The side's name in a table
    fn name(self) -> &'static str {
        match self {
            Use::Read => "read",
            Use::Crc => "read+crc",
            Use::Sha256 => "read+sha256",
        }
    }
}

/// Drop files from the page cache
///
/// # Arguments
///
/// * `paths` - The files
fn drop_cached(paths: &[PathBuf]) {
    for path in paths {
        let file = File::open(path).expect("a file opens");
        // SAFETY: the descriptor is open, and DONTNEED on a whole file has no other precondition
        unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
    }
}

/// Read every file, doing one thing with each read, and return the folder as the dataset a scan
/// would judge if it was SHA-256's side
///
/// # Arguments
///
/// * `root` - The folder, which paths are named under
/// * `paths` - The files
/// * `use_` - What each read is for
/// * `buf` - The read buffer
fn read_all(root: &Path, paths: &[PathBuf], use_: Use, buf: &mut [u8]) -> Option<ObjectDataset> {
    let mut sink = 0u64;
    let mut files = Vec::with_capacity(paths.len());
    let mut whole = Sha256::new();
    for path in paths {
        let mut file = File::open(path).expect("a file opens");
        let mut hasher = Sha256::new();
        let mut size = 0u64;
        // 1 MiB at a time until the file ends
        loop {
            let read = file.read(buf).expect("a file reads");
            if read == 0 {
                break;
            }
            size += read as u64;
            match use_ {
                Use::Read => sink ^= u64::from(buf[0]),
                Use::Crc => sink ^= crc(&buf[..read]),
                Use::Sha256 => hasher.update(&buf[..read]),
            }
        }
        if use_ == Use::Sha256 {
            // the file's digest into the folder's, beside its path and size, as F66's scan does
            let name = path.strip_prefix(root).unwrap_or(path).display().to_string();
            whole.update(format!("{name}\t{size}\t{:x}\n", hasher.finalize()).as_bytes());
            files.push((name, size));
        }
    }
    black_box(sink);
    (use_ == Use::Sha256).then(|| {
        ObjectDataset::Folder(FolderDataset {
            root: root.to_path_buf(),
            files,
            digest: format!("{:x}", whole.finalize()),
        })
    })
}

/// Run section four on the calling thread: every size, cold and hot, every use
///
/// # Arguments
///
/// * `ctx` - What the run is run with
/// * `dir` - The scratch directory on the device being read
/// * `round` - The round, which orders the sides
#[must_use]
pub fn section(ctx: &Ctx, dir: &Path, round: u32) -> Vec<SideOut> {
    let root = dir.join("x13-folder");
    std::fs::create_dir_all(&root).expect("the folder is made");
    let devices: Devices = Facts::gather(&root).devices;
    let mut buf = vec![0u8; READ];
    let mut outs = Vec::new();
    for &size in SIZES {
        let paths = prepare(&root, size, ctx.folder_bytes, super::SEED);
        let bytes = size * paths.len() as u64;
        for cold in [true, false] {
            let cell = format!("{} {}", size_name(size), if cold { "cold" } else { "hot" });
            for use_ in ordered(&[Use::Read, Use::Crc, Use::Sha256], round) {
                // the files out of the cache, or every one of them in it
                if cold {
                    drop_cached(&paths);
                } else {
                    let _ = read_all(&root, &paths, Use::Read, &mut buf);
                }
                let before = devices.snap();
                let cpu_before = sys::thread_cpu_ns();
                let started = Instant::now();
                let folder = read_all(&root, &paths, use_, &mut buf);
                let secs = started.elapsed().as_secs_f64();
                let cpu = sys::thread_cpu_ns() - cpu_before;
                let delta = before.delta(&devices.snap());
                if let Some(ObjectDataset::Folder(folder)) = folder {
                    eprintln!(
                        "x13: folder {} under {} holds {} files, digest {}",
                        size_name(size),
                        folder.root.display(),
                        folder.files.len(),
                        &folder.digest[..16]
                    );
                }
                let gib = bytes as f64 / f64::from(1u32 << 30);
                outs.push(SideOut::new(
                    cell.clone(),
                    use_.name(),
                    &[
                        ("mib_s", bytes as f64 / secs / f64::from(1u32 << 20)),
                        ("cpu_ms_gib", cpu as f64 / 1e6 / gib),
                        ("device_read_mib", delta.read as f64 / f64::from(1u32 << 20)),
                        ("from_device", delta.read as f64 / bytes as f64),
                        ("files", paths.len() as f64),
                    ],
                ));
            }
        }
    }
    outs
}
