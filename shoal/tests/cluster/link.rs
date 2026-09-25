//! A directed link: a proxy every connection in one direction has to cross
//!
//! [C11](../../../docs/src/distributed/testing.md)'s fault table asks for A→B and B→A blocked
//! independently, and for every discovered endpoint and every reconnect to traverse the fault.
//! A link is a TCP proxy that a node is given as its peer's address, so there is no path in that
//! direction that does not cross it: a cut ends every live stream and closes every new one
//! until healed. Two links, one per direction, make a partition asymmetric or full as the test
//! likes. This is a byte proxy: it cannot see inside TLS, which is why frame-class faults are
//! for a fake transport later, not for this.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// What a link does with traffic
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkState {
    /// Forward everything
    Pass,
    /// Close every connection, live or new
    Cut,
    /// Forward, after holding each new connection this long before it is opened
    Delay(Duration),
    /// Forward at most this many bytes per second on each connection, in either direction
    ///
    /// What makes a snapshot stream take seconds on a loopback that would carry it in
    /// milliseconds, so a cut can land in the middle of one
    /// ([F43](../../../docs/src/features/node-recovery.md)).
    Throttle(u64),
    /// Hold everything: live connections stay open and carry nothing, new ones are accepted
    /// and carry nothing, until the link is healed or cut
    ///
    /// A partition by dropped packets rather than by a reset: nothing a peer sends reaches the
    /// other side and nothing says so ([Resolved #143](../../../docs/src/appendix/resolved/silent-partition-hops.md)).
    Blackhole,
}

/// A directed proxy
pub struct Link {
    /// Where it listens; what the sending side is told its peer's address is
    addr: SocketAddr,
    /// Where it forwards to
    target: SocketAddr,
    /// What it is doing
    state: Arc<Mutex<LinkState>>,
    /// The accept loop
    acceptor: JoinHandle<()>,
    /// Every live forwarded connection
    streams: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Link {
    /// Start a proxy to a target
    ///
    /// # Arguments
    ///
    /// * `target` - Where to forward
    pub async fn start(target: SocketAddr) -> std::io::Result<Self> {
        // any port; the sender is told which
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let state = Arc::new(Mutex::new(LinkState::Pass));
        let streams: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
        let acceptor = tokio::spawn(accept_loop(
            listener,
            target,
            state.clone(),
            streams.clone(),
        ));
        Ok(Self {
            addr,
            target,
            state,
            acceptor,
            streams,
        })
    }

    /// The address a sender should use
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Where it forwards to
    pub fn target(&self) -> SocketAddr {
        self.target
    }

    /// What it is doing
    pub fn state(&self) -> LinkState {
        *self.state.lock().unwrap()
    }

    /// Cut the link: end every live stream and close every new one
    pub fn cut(&self) {
        *self.state.lock().unwrap() = LinkState::Cut;
        // aborting a forwarding task drops both sockets, which the peers see as a reset or
        // end of stream - a connection that is gone, not one that hangs
        for stream in self.streams.lock().unwrap().drain(..) {
            stream.abort();
        }
    }

    /// Hold every new connection before opening it
    ///
    /// # Arguments
    ///
    /// * `delay` - How long
    pub fn delay(&self, delay: Duration) {
        *self.state.lock().unwrap() = LinkState::Delay(delay);
    }

    /// Forward every new connection at a bounded rate
    ///
    /// # Arguments
    ///
    /// * `bytes_per_second` - The rate
    pub fn throttle(&self, bytes_per_second: u64) {
        *self.state.lock().unwrap() = LinkState::Throttle(bytes_per_second);
    }

    /// Hold every byte on every connection, live or new, without closing any
    pub fn blackhole(&self) {
        *self.state.lock().unwrap() = LinkState::Blackhole;
    }

    /// Forward again
    pub fn heal(&self) {
        *self.state.lock().unwrap() = LinkState::Pass;
    }

    /// How many connections are being forwarded right now
    pub fn live(&self) -> usize {
        let mut streams = self.streams.lock().unwrap();
        streams.retain(|stream| !stream.is_finished());
        streams.len()
    }
}

impl Drop for Link {
    /// Stop accepting and drop every stream
    fn drop(&mut self) {
        self.acceptor.abort();
        for stream in self.streams.lock().unwrap().drain(..) {
            stream.abort();
        }
    }
}

/// Accept connections and forward each, or refuse it, by the link's state
async fn accept_loop(
    listener: TcpListener,
    target: SocketAddr,
    state: Arc<Mutex<LinkState>>,
    streams: Arc<Mutex<Vec<JoinHandle<()>>>>,
) {
    loop {
        let Ok((inbound, _)) = listener.accept().await else {
            return;
        };
        // read the state once per connection; a later cut aborts the task instead
        let current = *state.lock().unwrap();
        match current {
            // accepted and closed at once: the sender sees a connection that ended, which is
            // what a reconnect into a cut link is meant to see
            LinkState::Cut => drop(inbound),
            LinkState::Pass | LinkState::Delay(_) | LinkState::Throttle(_) | LinkState::Blackhole => {
                let handle = tokio::spawn(forward(inbound, target, current, state.clone()));
                streams.lock().unwrap().push(handle);
            }
        }
    }
}

/// Forward one connection both ways until either side ends it
///
/// # Arguments
///
/// * `inbound` - The sender's connection
/// * `target` - Where it goes
/// * `state` - The link's state when the connection arrived
/// * `live` - The link's state as it changes, which a blackhole is read from on every chunk
async fn forward(
    mut inbound: TcpStream,
    target: SocketAddr,
    state: LinkState,
    live: Arc<Mutex<LinkState>>,
) {
    // a delayed link holds the connection before opening its other half
    if let LinkState::Delay(delay) = state {
        tokio::time::sleep(delay).await;
    }
    let Ok(mut outbound) = TcpStream::connect(target).await else {
        return;
    };
    // a throttled link copies each direction a chunk at a time, sleeping for the rate
    if let LinkState::Throttle(rate) = state {
        // both halves on this task, so aborting it on a cut ends both at once
        let (in_rx, in_tx) = inbound.into_split();
        let (out_rx, out_tx) = outbound.into_split();
        tokio::join!(trickle(in_rx, out_tx, rate), trickle(out_rx, in_tx, rate));
        return;
    }
    // copy until one side closes, holding every chunk while the link is a blackhole
    let (in_rx, in_tx) = inbound.into_split();
    let (out_rx, out_tx) = outbound.into_split();
    tokio::join!(
        hold_or_copy(in_rx, out_tx, live.clone()),
        hold_or_copy(out_rx, in_tx, live)
    );
}

/// Copy one direction until it ends, holding each chunk while the link is a blackhole
///
/// # Arguments
///
/// * `from` - The side to read
/// * `to` - The side to write
/// * `live` - The link's state as it changes
async fn hold_or_copy(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    live: Arc<Mutex<LinkState>>,
) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = match from.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        // a blackhole keeps the bytes and the connection, and says nothing
        while matches!(*live.lock().unwrap(), LinkState::Blackhole) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if to.write_all(&buffer[..read]).await.is_err() {
            break;
        }
    }
    let _ = to.shutdown().await;
}

/// Copy one direction at a bounded rate until it ends
///
/// # Arguments
///
/// * `from` - The side to read
/// * `to` - The side to write
/// * `rate` - Bytes per second
async fn trickle(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    rate: u64,
) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut buffer = vec![0u8; 4096];
    loop {
        let read = match from.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        if to.write_all(&buffer[..read]).await.is_err() {
            break;
        }
        // the time these bytes take at the rate
        let nanos = (read as u64).saturating_mul(1_000_000_000) / rate.max(1);
        tokio::time::sleep(Duration::from_nanos(nanos)).await;
    }
    let _ = to.shutdown().await;
}
