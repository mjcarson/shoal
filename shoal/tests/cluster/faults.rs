//! The storage faults a test applies from outside a child
//!
//! [F70](../../../docs/src/features/storage-faults.md)'s faults - a torn write, a full disk, a
//! lost device - are armed inside a child, on the I/O hook its storage asks, and act on calls
//! going wrong. [S16](../../../docs/src/object-storage/testing.md#the-fixture) asks for two more
//! that are not a call going wrong but what a device holds changing under a node: a device
//! replaced by an empty one while its node is down, and one bit of what a node wrote changed on
//! disk. Both are done here, by the test process, through `std::fs` (M11). Nothing in the
//! engine is asked or changed, and neither is an emulation, so there is no kernel behaviour to
//! hold them to as `kernel_faults.rs` holds the hooked faults: an emptied directory is empty, and
//! a flipped bit is a write.

use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::FileExt;
use std::path::Path;

/// Remove everything under a directory and keep the directory itself
///
/// This is what a replaced disk mounted at the old path looks like: the path is there and holds
/// nothing. The removals are synced through the directory before this returns. A directory that
/// is not there is an error, not one made empty.
///
/// # Arguments
///
/// * `dir` - The directory to empty
///
/// # Errors
///
/// The directory cannot be read, or an entry cannot be removed.
pub fn empty_dir(dir: &Path) -> io::Result<usize> {
    let mut removed = 0;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        // a directory goes with everything under it, anything else alone
        if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(&path)?;
        } else {
            std::fs::remove_file(&path)?;
        }
        removed += 1;
    }
    // the directory's own entries are durable before anything opens it again
    File::open(dir)?.sync_all()?;
    Ok(removed)
}

/// Flip one bit of a file in place, and sync it
///
/// One bit of one byte changes and nothing else about the file does: not its length, not
/// another byte. The write is followed by `fdatasync`, which is what makes it the device's and
/// not only the page cache's; nothing short of a power cut shows that it was, so a test holds
/// the byte read back, not the sync.
///
/// # Arguments
///
/// * `file` - The file
/// * `offset` - The byte to change, which the file must hold
/// * `bit` - Which bit of it, from 0 for the lowest to 7
///
/// # Errors
///
/// A bit past 7, a byte past the end of the file, or the file cannot be read, written or synced.
pub fn flip_bit(file: &Path, offset: u64, bit: u8) -> io::Result<(u8, u8)> {
    // a byte has eight bits
    if bit > 7 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("bit {bit} of a byte"),
        ));
    }
    let handle = OpenOptions::new().read(true).write(true).open(file)?;
    // the byte as it is; past the end this is an error, so nothing is ever appended
    let mut byte = [0u8];
    handle.read_exact_at(&mut byte, offset)?;
    let was = byte[0];
    // that one bit changed, in place
    byte[0] ^= 1 << bit;
    handle.write_all_at(&byte, offset)?;
    // on the device, not only in the page cache
    handle.sync_data()?;
    Ok((was, byte[0]))
}
