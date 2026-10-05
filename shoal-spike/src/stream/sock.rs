//! The socket options X11 turns, and the socket memory it reads
//!
//! Three knobs and one figure, all by `setsockopt` and `getsockopt` on a raw descriptor, so that
//! the glommio server and the tokio client set them the same way:
//!
//! - `TCP_NOTSENT_LOWAT`, which keeps a writer from queueing more unsent bytes in the kernel than
//!   it names, so a small frame chosen next is not behind megabytes already handed over;
//! - `TLS_RX_EXPECT_NO_PAD`, which lets kTLS decrypt a TLS 1.3 record straight into the reader's
//!   buffer instead of through one of its own, and which Shoal does not set;
//! - `SO_MEMINFO`, the socket's receive and send memory as the kernel counts it.

use std::os::fd::RawFd;

/// `TCP_NOTSENT_LOWAT` from `linux/tcp.h`
const TCP_NOTSENT_LOWAT: libc::c_int = 25;

/// `SOL_TLS` from `linux/socket.h`
const SOL_TLS: libc::c_int = 282;

/// `TLS_RX_EXPECT_NO_PAD` from `linux/tls.h`, since 6.0
const TLS_RX_EXPECT_NO_PAD: libc::c_int = 4;

/// `SO_MEMINFO` from `asm-generic/socket.h`
const SO_MEMINFO: libc::c_int = 55;

/// How many words `SO_MEMINFO` answers with, `SK_MEMINFO_VARS`
const MEMINFO_VARS: usize = 9;

/// Set an integer socket option
///
/// # Arguments
///
/// * `fd` - The socket
/// * `level` - The option's level
/// * `name` - The option
/// * `value` - Its value
fn set_int(fd: RawFd, level: libc::c_int, name: libc::c_int, value: u32) -> std::io::Result<()> {
    // SAFETY: the value outlives the call, and its size is passed with it
    let result = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            std::ptr::from_ref(&value).cast(),
            std::mem::size_of::<u32>() as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Keep at most this many unsent bytes in a socket's send queue
///
/// # Arguments
///
/// * `fd` - The socket
/// * `bytes` - The low water mark
pub fn set_notsent_lowat(fd: RawFd, bytes: u32) -> std::io::Result<()> {
    set_int(fd, libc::IPPROTO_TCP, TCP_NOTSENT_LOWAT, bytes)
}

/// Tell kTLS that the peer pads no record, so it may decrypt into the reader's buffer
///
/// # Arguments
///
/// * `fd` - A socket kTLS has taken over
pub fn set_rx_no_pad(fd: RawFd) -> std::io::Result<()> {
    set_int(fd, SOL_TLS, TLS_RX_EXPECT_NO_PAD, 1)
}

/// Turn Nagle off, as Shoal does on both ends
///
/// # Arguments
///
/// * `fd` - The socket
pub fn set_nodelay(fd: RawFd) -> std::io::Result<()> {
    set_int(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, 1)
}

/// The memory a socket holds now: received and not read, plus queued to send
///
/// # Arguments
///
/// * `fd` - The socket
#[must_use]
pub fn memory(fd: RawFd) -> u64 {
    let mut info = [0u32; MEMINFO_VARS];
    let mut len = std::mem::size_of_val(&info) as libc::socklen_t;
    // SAFETY: the buffer and its length are passed together, and the kernel writes no more
    let result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            SO_MEMINFO,
            info.as_mut_ptr().cast(),
            &mut len,
        )
    };
    if result != 0 {
        return 0;
    }
    // SK_MEMINFO_RMEM_ALLOC is the first word and SK_MEMINFO_WMEM_QUEUED the sixth
    u64::from(info[0]) + u64::from(info[5])
}

/// The host's busy time over every cpu since boot, in nanoseconds
///
/// User, nice, system, irq and softirq from the summary line of `/proc/stat`, so the kernel's
/// work for a socket - the copy, the record layer's crypto, the loopback's softirq - is counted
/// wherever it ran.
#[must_use]
pub fn host_busy_ns() -> u64 {
    let text = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let Some(line) = text.lines().find(|line| line.starts_with("cpu ")) else {
        return 0;
    };
    let fields: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .map(|field| field.parse().unwrap_or(0))
        .collect();
    let field = |index: usize| fields.get(index).copied().unwrap_or(0);
    let busy = field(0) + field(1) + field(2) + field(5) + field(6);
    // SAFETY: sysconf has no preconditions
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) }.max(1) as u64;
    busy * (1_000_000_000 / ticks)
}

/// Whether kTLS has taken this socket over
///
/// # Arguments
///
/// * `fd` - The socket
#[must_use]
pub fn is_ktls(fd: RawFd) -> bool {
    shoal::shared::tls::ktls::ulp_name(fd).as_deref() == Some("tls")
}
