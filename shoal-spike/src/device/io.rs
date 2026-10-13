//! The I/O every measurement shares: opening a file, writing a chunk, syncing a directory,
//! cloning a range
//!
//! Chunks are laid out as S6 lays them out: a header block of 4 KiB and then the units. The
//! bytes are seeded noise, filled once into one buffer for each piece size and written from
//! there again and again, so no allocation or fill is timed.

use std::cell::RefCell;
use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use glommio::io::{Directory, DmaBuffer, DmaFile, OpenOptions};

use super::stats::Rng;
use super::sys;

/// The alignment of every offset and length: the filesystems' block, which is also at least
/// every lab device's logical block
pub const ALIGN: u64 = 4096;

/// A stripe chunk's header block
pub const HEADER: u64 = 4096;

/// The largest piece one write carries
pub const PIECE: u64 = 1 << 20;

/// How many pieces of one chunk are in flight at once
pub const PIECES_IN_FLIGHT: usize = 8;

/// How a directory is synced so that a rename in it is durable
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirSync {
    /// `fdatasync` through the ring, which is what glommio's `Directory::sync` issues
    Fdatasync,
    /// `fsync` on the blocking thread, which glommio does not offer
    Fsync,
}

/// Open a file for reading and writing with direct I/O and no access times
///
/// # Arguments
///
/// * `path` - The file
/// * `create` - Whether to create it, truncating whatever is there
pub async fn open(path: &Path, create: bool) -> DmaFile {
    let mut options = OpenOptions::new();
    options.read(true).write(true).custom_flags(libc::O_NOATIME);
    if create {
        options.create(true).truncate(true);
    }
    options
        .dma_open(path)
        .await
        .unwrap_or_else(|error| panic!("opening {}: {error}", path.display()))
}

/// Open a file for reading only, with direct I/O and no access times
///
/// # Arguments
///
/// * `path` - The file
pub async fn open_read(path: &Path) -> DmaFile {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOATIME)
        .dma_open(path)
        .await
        .unwrap_or_else(|error| panic!("opening {}: {error}", path.display()))
}

/// Buffers of seeded noise, one for each length a write is cut into
pub struct Payloads {
    /// The buffers by length
    by_len: RefCell<HashMap<u64, Rc<DmaBuffer>>>,
    /// The generator that fills a new one
    rng: RefCell<Rng>,
}

impl Payloads {
    /// An empty set, filled as lengths are asked for
    ///
    /// # Arguments
    ///
    /// * `seed` - The seed of the noise
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Payloads {
            by_len: RefCell::new(HashMap::new()),
            rng: RefCell::new(Rng::new(seed)),
        }
    }

    /// The buffer of a length, made the first time it is asked for
    ///
    /// # Arguments
    ///
    /// * `len` - The length, a multiple of the alignment
    pub fn get(&self, len: u64) -> Rc<DmaBuffer> {
        debug_assert_eq!(len % ALIGN, 0, "a payload is whole blocks");
        if let Some(buffer) = self.by_len.borrow().get(&len) {
            return buffer.clone();
        }
        // filled once, outside anything timed
        let mut buffer = glommio::allocate_dma_buffer(len as usize);
        self.rng.borrow_mut().fill(buffer.as_bytes_mut());
        let buffer = Rc::new(buffer);
        self.by_len.borrow_mut().insert(len, buffer.clone());
        buffer
    }
}

/// Write a run of bytes at an offset, in pieces of at most a mebibyte, eight in flight
///
/// # Arguments
///
/// * `file` - The file
/// * `payloads` - Where the bytes come from
/// * `len` - How many bytes, a multiple of the alignment
/// * `offset` - Where they go, aligned
pub async fn write_body(file: &DmaFile, payloads: &Payloads, len: u64, offset: u64) {
    // the pieces, each at its place
    let pieces: Vec<(u64, u64)> = (0..len.div_ceil(PIECE))
        .map(|index| {
            let start = index * PIECE;
            (offset + start, PIECE.min(len - start))
        })
        .collect();
    stream::iter(pieces)
        .map(|(at, piece)| async move {
            let written = file
                .write_rc_at(payloads.get(piece), at)
                .await
                .expect("a direct write lands");
            assert_eq!(written as u64, piece, "a direct write lands whole");
        })
        .buffer_unordered(PIECES_IN_FLIGHT)
        .collect::<Vec<()>>()
        .await;
}

/// Write a stripe chunk's header block and units, together
///
/// # Arguments
///
/// * `file` - The chunk
/// * `payloads` - Where the bytes come from
/// * `size` - The units' length
/// * `base` - Where the chunk starts in the file: zero for a file a chunk, a slot's offset in
///   a shared file
pub async fn write_chunk(file: &DmaFile, payloads: &Payloads, size: u64, base: u64) {
    // the header and the body issued at once, as an apply would issue them
    futures::join!(
        write_body(file, payloads, HEADER, base),
        write_body(file, payloads, size, base + HEADER)
    );
}

/// Fill a file with zeros up to a length and sync it: a file written ahead
///
/// # Arguments
///
/// * `file` - The file
/// * `len` - Its length
pub async fn zero_fill(file: &DmaFile, len: u64) {
    // one buffer of zeros, written as every piece
    let mut zeros = glommio::allocate_dma_buffer(PIECE as usize);
    zeros.as_bytes_mut().fill(0);
    let zeros = Rc::new(zeros);
    let pieces: Vec<(u64, u64)> = (0..len.div_ceil(PIECE))
        .map(|index| (index * PIECE, PIECE.min(len - index * PIECE)))
        .collect();
    stream::iter(pieces)
        .map(|(at, piece)| {
            let zeros = zeros.clone();
            async move {
                // a short tail gets a buffer of its own length
                let buffer = if piece == PIECE {
                    zeros
                } else {
                    let mut tail = glommio::allocate_dma_buffer(piece as usize);
                    tail.as_bytes_mut().fill(0);
                    Rc::new(tail)
                };
                file.write_rc_at(buffer, at).await.expect("zeros land");
            }
        })
        .buffer_unordered(PIECES_IN_FLIGHT)
        .collect::<Vec<()>>()
        .await;
    file.fdatasync().await.expect("the zeros are synced");
}

/// Sync a directory in the form the probes chose
///
/// # Arguments
///
/// * `dir` - The directory
/// * `form` - The form
pub async fn sync_dir(dir: &Directory, form: DirSync) {
    match form {
        DirSync::Fdatasync => dir.sync().await.expect("the directory is synced"),
        DirSync::Fsync => {
            let fd = dir.as_raw_fd();
            glommio::executor()
                .spawn_blocking(move || sys::fsync(fd))
                .await
                .expect("the directory is synced");
        }
    }
}

/// How long a clone waited for the blocking thread, and how long it then took
#[derive(Debug, Clone, Copy)]
pub struct CloneTook {
    /// From the call to the blocking thread starting it
    pub waited: Duration,
    /// The ioctl itself
    pub took: Duration,
}

/// Clone a range of one file into another, on the blocking thread
///
/// The descriptors stay open for as long as the borrow, and the future is always awaited to
/// its end, so the blocking thread never holds a descriptor that was closed.
///
/// # Arguments
///
/// * `src` - The file the bytes come from, open for reading
/// * `src_offset` - Where they start
/// * `length` - How many bytes
/// * `dst` - The file they are cloned into
/// * `dst_offset` - Where they land
pub async fn clone(
    src: &DmaFile,
    src_offset: u64,
    length: u64,
    dst: &DmaFile,
    dst_offset: u64,
) -> std::io::Result<CloneTook> {
    let (from, into) = (src.as_raw_fd(), dst.as_raw_fd());
    let called = Instant::now();
    let (result, started, ended) = glommio::executor()
        .spawn_blocking(move || {
            // the ioctl, timed on the thread that makes it
            let started = Instant::now();
            let result = sys::clone_range(from, src_offset, length, into, dst_offset);
            (result, started, Instant::now())
        })
        .await;
    result.map(|()| CloneTook {
        waited: started.saturating_duration_since(called),
        took: ended.duration_since(started),
    })
}

/// Remove every file and directory under a path, outside anything timed
///
/// # Arguments
///
/// * `path` - The directory to empty and remove
pub fn wipe(path: &Path) {
    if path.exists() {
        std::fs::remove_dir_all(path)
            .unwrap_or_else(|error| panic!("removing {}: {error}", path.display()));
    }
}
