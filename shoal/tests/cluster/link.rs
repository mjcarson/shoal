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
        let acceptor = tokio::spawn(accept_loop(listener, target, state.clone(), streams.clone()));
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
            LinkState::Pass | LinkState::Delay(_) => {
                let handle = tokio::spawn(forward(inbound, target, current));
                streams.lock().unwrap().push(handle);
            }
        }
    }
}

/// Forward one connection both ways until either side ends it
async fn forward(mut inbound: TcpStream, target: SocketAddr, state: LinkState) {
    // a delayed link holds the connection before opening its other half
    if let LinkState::Delay(delay) = state {
        tokio::time::sleep(delay).await;
    }
    let Ok(mut outbound) = TcpStream::connect(target).await else {
        return;
    };
    // copy until one side closes; the error of a reset is the end of the stream
    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
}
