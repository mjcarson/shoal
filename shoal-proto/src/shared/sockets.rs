//! A connection's socket options both ends of Shoal's connections set alike
//! ([Resolved #310](../../../docs/src/appendix/resolved/peer-connections-keepalive.md),
//! [F112](../../../docs/src/features/object-congestion.md))
//!
//! Plain `setsockopt` on a descriptor, so it serves a glommio stream on a node and a tokio stream
//! in a client: nothing here names a runtime.

/// How long a client's connection carries nothing before either of its ends probes the other, so a
/// node lets go of a client that vanished and a client notices a node that did
pub const CLIENT_KEEPALIVE: std::time::Duration = std::time::Duration::from_secs(60);

/// The probes a connection sends a peer that does not answer before the kernel gives up on it
pub const KEEPALIVE_PROBES: libc::c_int = 3;

/// Probe a connection that carried nothing for a while, so one whose peer vanished is noticed
///
/// Sets `SO_KEEPALIVE`, `TCP_KEEPIDLE` at `idle`, `TCP_KEEPINTVL` at a fifth of it and a second at
/// least, and `TCP_KEEPCNT` at [`KEEPALIVE_PROBES`]. `TCP_USER_TIMEOUT` bounds only data the
/// connection sent, so one that owed nothing, its requests sent and its peer gone, was never
/// noticed; probed, it is aborted once its probes go unanswered, which a peer connection's
/// unacknowledged timeout bounds too
/// ([Resolved #310](../../../docs/src/appendix/resolved/peer-connections-keepalive.md)).
///
/// # Arguments
///
/// * `stream` - The connection
/// * `idle` - How long it carries nothing before it is probed; zero leaves keepalive off
///
/// # Errors
///
/// Fails as `setsockopt` does.
pub fn set_keepalive<S: std::os::fd::AsRawFd>(
    stream: &S,
    idle: std::time::Duration,
) -> std::io::Result<()> {
    // zero is no keepalive, the kernel's own default
    if idle.is_zero() {
        return Ok(());
    }
    // the idle time and the probes' interval, in whole seconds as the kernel takes them
    let idle_secs = libc::c_int::try_from(idle.as_secs().max(1)).unwrap_or(libc::c_int::MAX);
    let interval = (idle_secs / 5).max(1);
    for (level, name, value) in [
        (libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1),
        (libc::IPPROTO_TCP, libc::TCP_KEEPIDLE, idle_secs),
        (libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, interval),
        (libc::IPPROTO_TCP, libc::TCP_KEEPCNT, KEEPALIVE_PROBES),
    ] {
        // SAFETY: the descriptor is an open TCP socket for as long as the stream is borrowed,
        // and the value is a c_int of the size passed
        let rc = unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                level,
                name,
                std::ptr::from_ref(&value).cast(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// The longest name the kernel gives a congestion control, its `TCP_CA_NAME_MAX` less the
/// terminating zero
pub const CONGESTION_NAME_MAX: usize = 15;

/// Whether a name could be a congestion control's: lowercase letters, digits and `_`, at most
/// [`CONGESTION_NAME_MAX`] of them
///
/// What a configuration is judged by before any socket is asked, and what keeps a name a unit
/// file writes into a command line nothing but a name.
///
/// # Arguments
///
/// * `name` - The name
#[must_use]
pub fn is_congestion_name(name: &str) -> bool {
    // the kernel's names are short words, and nothing else is one a unit may write
    !name.is_empty()
        && name.len() <= CONGESTION_NAME_MAX
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// Run a connection under a congestion control, by its name (`TCP_CONGESTION`)
///
/// A process without `CAP_NET_ADMIN` may take only one whose module is loaded (`ENOENT`
/// otherwise) and that its host allows (`net.ipv4.tcp_allowed_congestion_control`, `EPERM`
/// otherwise); a connection refused keeps what it had.
///
/// # Arguments
///
/// * `fd` - The connection's descriptor, open for as long as this runs
/// * `name` - The congestion control
///
/// # Errors
///
/// Fails as `setsockopt` does.
pub fn set_congestion_of(fd: std::os::fd::RawFd, name: &str) -> std::io::Result<()> {
    // the kernel copies at most its longest name, so a longer one is cut there and refused unknown
    let len = libc::socklen_t::try_from(name.len()).unwrap_or(libc::socklen_t::MAX);
    // SAFETY: the caller holds the descriptor open as a TCP socket for this call, and the value
    // is the name's bytes with their length
    let rc = unsafe {
        libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_CONGESTION, name.as_ptr().cast(), len)
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// A connection's congestion control as the kernel holds it
///
/// # Arguments
///
/// * `fd` - The connection's descriptor, open for as long as this runs
///
/// # Errors
///
/// Fails as `getsockopt` does.
pub fn congestion_of(fd: std::os::fd::RawFd) -> std::io::Result<String> {
    // the kernel's longest name and its terminator
    let mut name = [0u8; CONGESTION_NAME_MAX + 1];
    let mut len = name.len() as libc::socklen_t;
    // SAFETY: the caller holds the descriptor open as a TCP socket for this call, and the out
    // pointers are valid for the length passed
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_CONGESTION,
            name.as_mut_ptr().cast(),
            &raw mut len,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // the name is what the kernel wrote, up to its terminator
    let written = &name[..(len as usize).min(name.len())];
    let end = written.iter().position(|byte| *byte == 0).unwrap_or(written.len());
    Ok(String::from_utf8_lossy(&written[..end]).into_owned())
}

/// Whether this process has said its host refuses the congestion control its object connections
/// ask for
static CONGESTION_REFUSED_SAID: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Run a connection that carries objects under a congestion control, saying once if its host
/// refuses it ([F112](../../../docs/src/features/object-congestion.md))
///
/// A connection refused keeps its host's own congestion control, which is correct and slower to
/// start again after an idle, so a refusal is said once a process and never ends the connection.
///
/// # Arguments
///
/// * `fd` - The connection's descriptor, open for as long as this runs
/// * `name` - The congestion control
///
/// # Errors
///
/// Fails as `setsockopt` does.
pub fn set_object_congestion_of(fd: std::os::fd::RawFd, name: &str) -> std::io::Result<()> {
    let Err(error) = set_congestion_of(fd, name) else {
        return Ok(());
    };
    // a host without its module, or that keeps it from this process, is said once
    if !CONGESTION_REFUSED_SAID.swap(true, std::sync::atomic::Ordering::Relaxed) {
        tracing::event!(
            tracing::Level::WARN,
            msg = "this host refuses the congestion control object connections ask for; they keep the host's, which restarts a window after an idle",
            congestion = name,
            allow = "load its module, tcp_<name>, and add it to net.ipv4.tcp_allowed_congestion_control, as shoaladm's unit does",
            ?error
        );
    }
    Err(error)
}

/// Run a connection that carries objects under a congestion control, saying once if its host
/// refuses it
///
/// What [`set_object_congestion_of`] does, for a connection whose stream is at hand.
///
/// # Arguments
///
/// * `stream` - The connection
/// * `name` - The congestion control
///
/// # Errors
///
/// Fails as `setsockopt` does.
pub fn set_object_congestion<S: std::os::fd::AsRawFd>(
    stream: &S,
    name: &str,
) -> std::io::Result<()> {
    set_object_congestion_of(stream.as_raw_fd(), name)
}

#[cfg(test)]
mod tests {
    use std::os::fd::AsRawFd;

    /// One option of a socket, as the kernel holds it
    ///
    /// # Arguments
    ///
    /// * `stream` - The socket
    /// * `level` - The option's level
    /// * `name` - The option
    fn option(stream: &std::net::TcpStream, level: libc::c_int, name: libc::c_int) -> libc::c_int {
        let mut value: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: the descriptor is an open TCP socket and the out pointers are valid for their sizes
        let rc = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                level,
                name,
                std::ptr::from_mut(&mut value).cast(),
                &raw mut len,
            )
        };
        assert_eq!(rc, 0, "{}", std::io::Error::last_os_error());
        value
    }

    /// A connection given keepalive probes its peer after the idle it was given, every fifth of it,
    /// three times, and one given none is left without (item 310)
    #[test]
    fn keepalive_is_set_on_the_socket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback listener");
        let at = listener.local_addr().expect("its address");
        let kept = std::net::TcpStream::connect(at).expect("a dial");
        let left = std::net::TcpStream::connect(at).expect("another");
        // set, and read back from the kernel
        super::set_keepalive(&kept, std::time::Duration::from_secs(10))
            .expect("the options are set");
        assert_eq!(option(&kept, libc::SOL_SOCKET, libc::SO_KEEPALIVE), 1);
        assert_eq!(option(&kept, libc::IPPROTO_TCP, libc::TCP_KEEPIDLE), 10);
        assert_eq!(option(&kept, libc::IPPROTO_TCP, libc::TCP_KEEPINTVL), 2);
        assert_eq!(option(&kept, libc::IPPROTO_TCP, libc::TCP_KEEPCNT), super::KEEPALIVE_PROBES);
        // and zero sets nothing
        super::set_keepalive(&left, std::time::Duration::ZERO).expect("nothing to set");
        assert_eq!(option(&left, libc::SOL_SOCKET, libc::SO_KEEPALIVE), 0);
    }

    /// A connection runs under the congestion control it names, read back from the kernel, and one
    /// its host has not got leaves it as it was (F112)
    ///
    /// Reno is every kernel's and any process may take it, so this runs on a host that allows
    /// nothing else; a name no kernel has stands in for a congestion control the host refuses.
    #[test]
    fn a_connection_runs_under_the_congestion_control_it_names() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback listener");
        let stream = std::net::TcpStream::connect(listener.local_addr().expect("its address"))
            .expect("a dial");
        let fd = stream.as_raw_fd();
        // what the host gave it is a name, and reno once named, as the kernel reads it back
        let hosts = super::congestion_of(fd).expect("the host's congestion control");
        assert!(super::is_congestion_name(&hosts), "{hosts:?}");
        super::set_congestion_of(fd, "reno").expect("reno is taken");
        assert_eq!(super::congestion_of(fd).expect("read back"), "reno");
        // a name the host has no module for is refused, and changes nothing
        let error = super::set_object_congestion(&stream, "nosuchcc").expect_err("no kernel has it");
        assert_eq!(error.raw_os_error(), Some(libc::ENOENT), "{error:?}");
        assert_eq!(super::congestion_of(fd).expect("read back"), "reno");
    }

    /// A congestion control's name is a short lowercase word, and nothing else a unit could write
    /// into a command line (F112)
    #[test]
    fn a_congestion_controls_name_is_a_short_word() {
        for name in ["bbr", "cubic", "reno", "htcp", "dctcp", "bbr2", "lp_x"] {
            assert!(super::is_congestion_name(name), "{name}");
        }
        for name in ["", "BBR", "bbr cubic", "bbr;rm", "a-b", "sixteen_letters_", "b\u{e9}"] {
            assert!(!super::is_congestion_name(name), "{name:?}");
        }
    }
}
