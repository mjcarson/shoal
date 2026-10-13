//! Every system call X6 makes that neither std nor the glommio fork offers
//!
//! Each is a thin wrapper that turns a return code into an `io::Result`. They are kept in one
//! file so that every `unsafe` block of the spike is read in one place.

use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

/// The ioctl that maps a file's extents, which libc does not name
const FS_IOC_FIEMAP: libc::Ioctl = 0xC020_660B;

/// Ask FIEMAP to sync the file before it maps it
const FIEMAP_FLAG_SYNC: u32 = 0x1;

/// An extent allocated and not yet written
const FIEMAP_EXTENT_UNWRITTEN: u32 = 0x800;

/// An extent whose blocks another file shares
const FIEMAP_EXTENT_SHARED: u32 = 0x2000;

/// The header FIEMAP reads and fills, as `linux/fiemap.h` lays it out
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Fiemap {
    /// The first byte of the range to map
    fm_start: u64,
    /// The length of the range to map
    fm_length: u64,
    /// The flags asked for
    fm_flags: u32,
    /// How many extents were mapped
    fm_mapped_extents: u32,
    /// How many extents the array after the header holds
    fm_extent_count: u32,
    /// Unused
    fm_reserved: u32,
}

/// One extent as FIEMAP fills it
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FiemapExtent {
    /// The extent's first byte in the file
    fe_logical: u64,
    /// The extent's first byte on the device
    fe_physical: u64,
    /// The extent's length
    fe_length: u64,
    /// Unused
    fe_reserved64: [u64; 2],
    /// What kind of extent it is
    fe_flags: u32,
    /// Unused
    fe_reserved: [u32; 3],
}

/// A file's extents, counted by kind
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Extents {
    /// Every extent
    pub total: u32,
    /// Extents whose blocks another file shares
    pub shared: u32,
    /// Extents allocated and not yet written
    pub unwritten: u32,
}

/// Turn a C return code into an `io::Result`
///
/// # Arguments
///
/// * `rc` - What the call returned
fn check(rc: libc::c_int) -> io::Result<libc::c_int> {
    // a negative code means errno says why
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(rc)
    }
}

/// A path as the C string a system call takes
///
/// # Arguments
///
/// * `path` - The path
pub fn cpath(path: &Path) -> CString {
    CString::new(path.as_os_str().as_bytes()).expect("a path holds no NUL")
}

/// Clone a range of one file into another with `FICLONERANGE`
///
/// The offsets and the length must be multiples of the filesystem's block, and both files on
/// one filesystem that shares blocks. A filesystem that cannot clone refuses with an error,
/// where `copy_file_range` would have copied in silence.
///
/// # Arguments
///
/// * `src` - The file the bytes come from, open for reading
/// * `src_offset` - Where they start in it
/// * `length` - How many bytes
/// * `dst` - The file they are cloned into, open for writing
/// * `dst_offset` - Where they land in it
pub fn clone_range(
    src: RawFd,
    src_offset: u64,
    length: u64,
    dst: RawFd,
    dst_offset: u64,
) -> io::Result<()> {
    // the argument the ioctl takes, naming the source by its descriptor
    let range = libc::file_clone_range {
        src_fd: i64::from(src),
        src_offset,
        src_length: length,
        dest_offset: dst_offset,
    };
    // SAFETY: the descriptors are open for the call and the argument outlives it
    check(unsafe { libc::ioctl(dst, libc::FICLONERANGE, &range) })?;
    Ok(())
}

/// One extent of a file, as FIEMAP maps it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    /// Its first byte in the file
    pub logical: u64,
    /// Its first byte on the filesystem's device
    pub physical: u64,
    /// Its length
    pub length: u64,
    /// What kind of extent it is
    pub flags: u32,
}

/// Map a file's extents, syncing it first
///
/// # Arguments
///
/// * `fd` - The file
pub fn extent_map(fd: RawFd) -> io::Result<Vec<Extent>> {
    // one call with no array to learn how many extents there are
    let mut head = Fiemap {
        fm_start: 0,
        fm_length: u64::MAX,
        fm_flags: FIEMAP_FLAG_SYNC,
        ..Fiemap::default()
    };
    // SAFETY: the header is the whole argument when it asks for no extents
    check(unsafe { libc::ioctl(fd, FS_IOC_FIEMAP, &mut head) })?;
    let count = head.fm_mapped_extents as usize + 16;
    // the header and an array of that many extents, in memory aligned for both
    let words = (std::mem::size_of::<Fiemap>()
        + count * std::mem::size_of::<FiemapExtent>())
    .div_ceil(8);
    let mut buffer = vec![0_u64; words];
    let header = Fiemap {
        fm_start: 0,
        fm_length: u64::MAX,
        fm_flags: FIEMAP_FLAG_SYNC,
        fm_extent_count: count as u32,
        ..Fiemap::default()
    };
    // SAFETY: the buffer is large enough for the header and `count` extents, and aligned to 8
    unsafe {
        std::ptr::write(buffer.as_mut_ptr().cast::<Fiemap>(), header);
        check(libc::ioctl(fd, FS_IOC_FIEMAP, buffer.as_mut_ptr()))?;
    }
    // read the header back, then each extent it mapped
    // SAFETY: the kernel filled the header and `fm_mapped_extents` extents after it
    let mapped = unsafe { std::ptr::read(buffer.as_ptr().cast::<Fiemap>()) }.fm_mapped_extents;
    let mut extents = Vec::with_capacity(mapped as usize);
    for index in 0..mapped as usize {
        // SAFETY: index is below the count the kernel filled, which is below `count`
        let extent = unsafe {
            std::ptr::read(
                buffer
                    .as_ptr()
                    .cast::<u8>()
                    .add(std::mem::size_of::<Fiemap>() + index * std::mem::size_of::<FiemapExtent>())
                    .cast::<FiemapExtent>(),
            )
        };
        extents.push(Extent {
            logical: extent.fe_logical,
            physical: extent.fe_physical,
            length: extent.fe_length,
            flags: extent.fe_flags,
        });
    }
    Ok(extents)
}

/// Count a file's extents by kind, syncing it first
///
/// # Arguments
///
/// * `fd` - The file
pub fn fiemap(fd: RawFd) -> io::Result<Extents> {
    // every extent, then counted by its flags
    let mut extents = Extents::default();
    for extent in extent_map(fd)? {
        extents.total += 1;
        if extent.flags & FIEMAP_EXTENT_SHARED != 0 {
            extents.shared += 1;
        }
        if extent.flags & FIEMAP_EXTENT_UNWRITTEN != 0 {
            extents.unwritten += 1;
        }
    }
    Ok(extents)
}

/// Where a byte of a file lies on the filesystem's device, from its extent map
///
/// # Arguments
///
/// * `extents` - The file's extents
/// * `offset` - The byte in the file
#[must_use]
pub fn physical_of(extents: &[Extent], offset: u64) -> Option<u64> {
    extents
        .iter()
        .find(|extent| offset >= extent.logical && offset < extent.logical + extent.length)
        .map(|extent| extent.physical + (offset - extent.logical))
}

/// Write back everything dirty, then drop the page, dentry and inode caches
///
/// Needs root. Done three times, since XFS keeps metadata buffers on a list one pass may not
/// empty.
pub fn drop_caches() -> io::Result<()> {
    for _ in 0..3 {
        // SAFETY: sync takes no arguments and cannot fail
        unsafe { libc::sync() };
        std::fs::write("/proc/sys/vm/drop_caches", "3")?;
    }
    Ok(())
}

/// Write back everything dirty on the filesystem a directory is on
///
/// # Arguments
///
/// * `dir` - Any directory on the filesystem
pub fn syncfs(dir: &Path) -> io::Result<()> {
    // the directory open just long enough to name the filesystem
    let file = std::fs::File::open(dir)?;
    // SAFETY: the descriptor is open for the call
    check(unsafe { libc::syncfs(file.as_raw_fd()) })?;
    Ok(())
}

/// `fsync` a descriptor, which glommio's reactor never issues: it only has `fdatasync`
///
/// # Arguments
///
/// * `fd` - The descriptor
pub fn fsync(fd: RawFd) -> io::Result<()> {
    // SAFETY: the descriptor is open for the call
    check(unsafe { libc::fsync(fd) })?;
    Ok(())
}

/// Read a clock as nanoseconds
///
/// # Arguments
///
/// * `clock` - The clock
fn clock_ns(clock: libc::clockid_t) -> u64 {
    // SAFETY: a zeroed timespec is a valid out parameter, filled by the call
    let mut now: libc::timespec = unsafe { std::mem::zeroed() };
    unsafe { libc::clock_gettime(clock, &mut now) };
    now.tv_sec as u64 * 1_000_000_000 + now.tv_nsec as u64
}

/// The CPU time the calling thread has used, in nanoseconds
#[must_use]
pub fn thread_cpu_ns() -> u64 {
    clock_ns(libc::CLOCK_THREAD_CPUTIME_ID)
}

/// The CPU time every thread of this process has used, in nanoseconds
///
/// That counts the executors, their blocking threads and the io_uring workers the kernel runs
/// on the process's behalf for a sync, an allocation or an open that missed the cache.
#[must_use]
pub fn process_cpu_ns() -> u64 {
    clock_ns(libc::CLOCK_PROCESS_CPUTIME_ID)
}

/// How many times the calling thread has given up its cpu of its own accord
#[must_use]
pub fn voluntary_switches() -> u64 {
    // the thread's own status, which counts its sleeps
    std::fs::read_to_string("/proc/thread-self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|line| line.strip_prefix("voluntary_ctxt_switches:"))
        .and_then(|count| count.trim().parse().ok())
        .unwrap_or(0)
}

/// One entry of a directory, as `getdents64` gives it
#[derive(Debug, Clone)]
pub struct Entry {
    /// The entry's inode number
    pub ino: u64,
    /// Its name
    pub name: CString,
    /// Whether it is a directory, by the type the directory stores
    pub is_dir: bool,
}

/// Open a directory without updating its access time
///
/// # Arguments
///
/// * `at` - The directory a relative name is opened under, or `None` for a path
/// * `name` - The name, or the path
pub fn open_dir(at: Option<RawFd>, name: &CStr) -> io::Result<OwnedFd> {
    // O_NOATIME keeps a listing from writing an access time under relatime
    let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOATIME | libc::O_CLOEXEC;
    // SAFETY: the name is a valid C string and the descriptor, if any, is open
    let fd = check(unsafe { libc::openat(at.unwrap_or(libc::AT_FDCWD), name.as_ptr(), flags) })?;
    // SAFETY: the descriptor was just opened and is owned by nobody else
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Every entry of an open directory but `.` and `..`, read with `getdents64`
///
/// # Arguments
///
/// * `fd` - The directory
/// * `buffer` - Scratch space for the kernel to fill, reused across calls
pub fn read_dir(fd: RawFd, buffer: &mut [u8]) -> io::Result<Vec<Entry>> {
    let mut entries = Vec::new();
    loop {
        // SAFETY: the buffer is valid for its length and the descriptor is open
        let read = unsafe {
            libc::syscall(libc::SYS_getdents64, fd, buffer.as_mut_ptr(), buffer.len())
        };
        if read < 0 {
            return Err(io::Error::last_os_error());
        }
        if read == 0 {
            return Ok(entries);
        }
        // walk the records the kernel packed into the buffer
        let mut at = 0_usize;
        while at < read as usize {
            // the fixed fields of a linux_dirent64: ino, off, reclen, type, then the name
            let ino = u64::from_ne_bytes(buffer[at..at + 8].try_into().expect("eight bytes"));
            let reclen =
                u16::from_ne_bytes(buffer[at + 16..at + 18].try_into().expect("two bytes")) as usize;
            let kind = buffer[at + 18];
            let name = CStr::from_bytes_until_nul(&buffer[at + 19..at + reclen])
                .expect("a dirent name ends in NUL");
            if name.to_bytes() != b"." && name.to_bytes() != b".." {
                entries.push(Entry {
                    ino,
                    name: name.to_owned(),
                    is_dir: kind == libc::DT_DIR,
                });
            }
            at += reclen;
        }
    }
}

/// A file's length, by `statx` under a directory
///
/// # Arguments
///
/// * `at` - The directory
/// * `name` - The file's name in it
pub fn statx_size(at: RawFd, name: &CStr) -> io::Result<u64> {
    // SAFETY: a zeroed statx is a valid out parameter, filled by the call
    let mut stat: libc::statx = unsafe { std::mem::zeroed() };
    // SAFETY: the name is a valid C string and the descriptor is open
    check(unsafe {
        libc::statx(
            at,
            name.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
            libc::STATX_SIZE,
            &mut stat,
        )
    })?;
    Ok(stat.stx_size)
}

/// Read an extended attribute of a file, by its path
///
/// # Arguments
///
/// * `path` - The file
/// * `name` - The attribute
/// * `value` - Where its value is read to
pub fn get_xattr(path: &CStr, name: &CStr, value: &mut [u8]) -> io::Result<usize> {
    // SAFETY: both names are valid C strings and the value buffer is valid for its length
    let read = unsafe {
        libc::lgetxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_mut_ptr().cast(),
            value.len(),
        )
    };
    if read < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(read as usize)
    }
}

/// Set an extended attribute on an open file
///
/// # Arguments
///
/// * `fd` - The file
/// * `name` - The attribute
/// * `value` - Its value
pub fn set_xattr(fd: RawFd, name: &CStr, value: &[u8]) -> io::Result<()> {
    // SAFETY: the name is a valid C string and the value valid for its length
    check(unsafe { libc::fsetxattr(fd, name.as_ptr(), value.as_ptr().cast(), value.len(), 0) })?;
    Ok(())
}

/// A buffer aligned for direct I/O, owned outside any executor
pub struct Aligned {
    /// The memory
    ptr: *mut u8,
    /// Its layout, for freeing it
    layout: std::alloc::Layout,
}

impl Aligned {
    /// Allocate a zeroed buffer aligned to 4 KiB
    ///
    /// # Arguments
    ///
    /// * `len` - Its length, a multiple of 4 KiB
    #[must_use]
    pub fn new(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len, 4096).expect("a valid layout");
        // SAFETY: the layout is non-zero in size
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "an aligned buffer is allocated");
        Aligned { ptr, layout }
    }

    /// The buffer as a slice
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: the pointer is valid for the layout's size and owned by this buffer
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.layout.size()) }
    }
}

impl Drop for Aligned {
    /// Free the buffer
    fn drop(&mut self) {
        // SAFETY: the pointer was allocated with this layout
        unsafe { std::alloc::dealloc(self.ptr, self.layout) };
    }
}

/// Read a file's first block with direct I/O, opened under a directory
///
/// This is what a light scrub that keeps a chunk's label in its header has to do for every
/// chunk: open it, read its header, close it.
///
/// # Arguments
///
/// * `at` - The directory
/// * `name` - The file's name in it
/// * `buffer` - At least one aligned block
pub fn read_head(at: RawFd, name: &CStr, buffer: &mut Aligned) -> io::Result<()> {
    // direct, so the read is the device's, and without an access time
    let flags = libc::O_RDONLY | libc::O_DIRECT | libc::O_NOATIME | libc::O_CLOEXEC;
    // SAFETY: the name is a valid C string and the descriptor is open
    let fd = check(unsafe { libc::openat(at, name.as_ptr(), flags) })?;
    // SAFETY: the descriptor was just opened and is owned by nobody else
    let file = unsafe { OwnedFd::from_raw_fd(fd) };
    let block = &mut buffer.as_mut_slice()[..4096];
    // SAFETY: the buffer is valid for 4096 bytes and aligned for direct I/O
    let read = unsafe { libc::pread(file.as_raw_fd(), block.as_mut_ptr().cast(), 4096, 0) };
    if read < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Remove a name under a directory, on the calling thread
///
/// # Arguments
///
/// * `at` - The directory
/// * `name` - The name
/// * `dir` - Whether the name is a directory
pub fn unlink_at(at: RawFd, name: &CStr, dir: bool) -> io::Result<()> {
    let flags = if dir { libc::AT_REMOVEDIR } else { 0 };
    // SAFETY: the name is a valid C string and the descriptor is open
    check(unsafe { libc::unlinkat(at, name.as_ptr(), flags) })?;
    Ok(())
}
