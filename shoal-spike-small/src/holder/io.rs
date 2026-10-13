//! The I/O the holder shares: opening a file for direct I/O, writing one ahead with zeros, a
//! chunk's header block, and the figures its counters read
//!
//! `open` and `zero_fill` are X6's (`shoal-spike/src/device/io.rs`); the rest is the holder's.

use std::os::fd::RawFd;
use std::path::Path;
use std::rc::Rc;

use futures::stream::{self, StreamExt};
use glommio::io::{DmaBuffer, DmaFile, OpenOptions};

use crate::wire::{StageHead, CHUNK_HEADER};

/// The largest piece a zero fill writes at once
const PIECE: u64 = 1 << 20;

/// How many pieces of a zero fill are in flight at once
const PIECES_IN_FLIGHT: usize = 8;

/// Open a file for reading and writing with direct I/O and no access times
///
/// # Arguments
///
/// * `path` - The file
/// * `create` - Whether to create it, truncating whatever is there
///
/// # Errors
///
/// When the file cannot be opened.
pub async fn open(path: &Path, create: bool) -> std::io::Result<DmaFile> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).custom_flags(libc::O_NOATIME);
    if create {
        options.create(true).truncate(true);
    }
    options
        .dma_open(path)
        .await
        .map_err(|error| std::io::Error::other(format!("opening {}: {error}", path.display())))
}

/// Fill a file with zeros up to a length and sync it: a file written ahead
///
/// # Arguments
///
/// * `file` - The file
/// * `len` - Its length, a multiple of the block
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

/// A chunk's or a record's header block: what a stage names, then zeros to the block's end
///
/// S6's header holds the chunk's identity, its label and a checksum a unit; the spike's holds the
/// stage's head, which is the same few dozen bytes in the same one block.
///
/// # Arguments
///
/// * `head` - What the stage named
#[must_use]
pub fn header_block(head: &StageHead) -> Rc<DmaBuffer> {
    // the head at the front of a zeroed block
    let mut block = glommio::allocate_dma_buffer(CHUNK_HEADER as usize);
    let bytes = block.as_bytes_mut();
    bytes.fill(0);
    bytes[..StageHead::LEN].copy_from_slice(&head.encode());
    Rc::new(block)
}

/// The cpu time the calling thread has used, in nanoseconds
#[must_use]
pub fn thread_cpu_ns() -> u64 {
    let mut spec = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: the timespec outlives the call, which writes only it
    let result = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut spec) };
    if result != 0 {
        return 0;
    }
    u64::try_from(spec.tv_sec).unwrap_or(0) * 1_000_000_000 + u64::try_from(spec.tv_nsec).unwrap_or(0)
}

/// Whether kTLS has taken a socket over
///
/// # Arguments
///
/// * `fd` - The socket
#[must_use]
pub fn is_ktls(fd: RawFd) -> bool {
    shoal::shared::tls::ktls::ulp_name(fd).as_deref() == Some("tls")
}

/// Turn Nagle off, as Shoal does on both ends
///
/// # Arguments
///
/// * `fd` - The socket
///
/// # Errors
///
/// When the kernel refuses the option.
pub fn set_nodelay(fd: RawFd) -> std::io::Result<()> {
    let one: libc::c_int = 1;
    // SAFETY: the value outlives the call, and its size is passed with it
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            std::ptr::from_ref(&one).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}
