//! The driver's lanes to the holders: connections under the product's mutual TLS, one request
//! at a time each, pooled a holder
//!
//! A lane is what a node's lane to a slice would be: a TCP connection, Nagle off, its handshake
//! rustls's and its records the kernel's (`shoal::client::tls::connect`), presenting a leaf the
//! holder checks against the authority it was issued by. The Shoal client keeps a pool of
//! connections a member, so a holder gets a pool of lanes, enough that no write waits for one
//! while another is busy: a request in flight a lane, and every write's stage, apply or fold takes
//! one lane of each holder it touches, at most twice the depth at once
//! ([`crate::deferred`]).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use color_eyre::eyre::{bail, eyre, WrapErr};
use rustls::ClientConfig;
use shoal::shared::tls::{peer_client_config, PeerTlsOptions, TlsClientOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

use crate::wire::{Header, HolderStats, Kind, ProbeOut, Request, HEADER_LEN};

/// What the driver presents to the holders and trusts them by
#[derive(Clone)]
pub struct LaneTls {
    /// The client config, the driver's leaf in it
    pub config: Arc<ClientConfig>,
    /// The authority a holder's leaf is checked against, by the address it is dialled at
    pub options: TlsClientOptions,
}

impl LaneTls {
    /// The driver's TLS from its leaf and the holders' authority
    ///
    /// # Arguments
    ///
    /// * `options` - The driver's certificate and key, and the authority
    ///
    /// # Errors
    ///
    /// When the files cannot be read or rustls refuses them.
    pub fn load(options: &PeerTlsOptions) -> color_eyre::Result<Self> {
        let config = peer_client_config(options).map_err(|error| eyre!("the driver's tls: {error}"))?;
        Ok(LaneTls {
            config,
            options: TlsClientOptions::new(options.ca.clone()),
        })
    }
}

/// One lane to a holder
pub struct Lane {
    /// The connection, kTLS holding its records
    stream: TcpStream,
    /// What rustls kept of the session, held for the life of the connection
    _session: shoal::shared::tls::Established<rustls::client::ClientConnectionData>,
}

impl Lane {
    /// Open a lane to a holder
    ///
    /// # Arguments
    ///
    /// * `addr` - Where the holder listens
    /// * `tls` - What the driver presents and trusts
    ///
    /// # Errors
    ///
    /// When the holder does not accept or the handshake fails.
    pub async fn open(addr: SocketAddr, tls: &LaneTls) -> color_eyre::Result<Lane> {
        let mut stream = TcpStream::connect(addr)
            .await
            .wrap_err_with(|| format!("connecting to the holder at {addr}"))?;
        stream.set_nodelay(true)?;
        // the handshake, then kTLS, as the product's client does it
        let session = shoal::client::tls::connect(&mut stream, &tls.config, &tls.options, &addr)
            .await
            .map_err(|error| eyre!("the TLS handshake with {addr}: {error}"))?;
        Ok(Lane {
            stream,
            _session: session,
        })
    }

    /// Send one whole frame and read the holder's answer
    ///
    /// # Arguments
    ///
    /// * `frame` - The request's frame
    ///
    /// # Errors
    ///
    /// When the lane fails or the answer is not a frame the spike speaks.
    pub async fn call(&mut self, frame: &[u8]) -> color_eyre::Result<(Kind, Vec<u8>)> {
        self.stream.write_all(frame).await?;
        // the answer's header, then its body
        let mut bytes = [0u8; HEADER_LEN];
        self.stream.read_exact(&mut bytes).await?;
        let header = Header::decode(&bytes).map_err(|error| eyre!("the holder answered {error}"))?;
        let mut body = vec![0u8; header.len as usize];
        self.stream.read_exact(&mut body).await?;
        Ok((header.kind, body))
    }
}

/// A holder's lanes, each in use by one request at a time
pub struct Pool {
    /// The holder's name: its node's in the inventory
    pub name: String,
    /// Where it listens
    pub addr: SocketAddr,
    /// What the lanes present and trust
    tls: LaneTls,
    /// The lanes not in use
    idle: Mutex<Vec<Lane>>,
    /// One permit a lane not in use
    free: Semaphore,
}

/// A lane taken from a pool, given back when dropped
pub struct Taken {
    /// The pool it came from
    pool: Arc<Pool>,
    /// The lane, until it is given back
    lane: Option<Lane>,
    /// Whether a call on it failed, so it is replaced rather than given back
    broken: bool,
}

impl Pool {
    /// Open a pool of lanes to a holder
    ///
    /// # Arguments
    ///
    /// * `name` - The holder's name
    /// * `addr` - Where it listens
    /// * `lanes` - How many lanes
    /// * `tls` - What the lanes present and trust
    ///
    /// # Errors
    ///
    /// When a lane cannot be opened.
    pub async fn open(name: &str, addr: SocketAddr, lanes: usize, tls: &LaneTls) -> color_eyre::Result<Pool> {
        let mut idle = Vec::with_capacity(lanes);
        for _ in 0..lanes {
            idle.push(Lane::open(addr, tls).await?);
        }
        Ok(Pool {
            name: name.to_string(),
            addr,
            tls: tls.clone(),
            idle: Mutex::new(idle),
            free: Semaphore::new(lanes),
        })
    }

    /// Take a lane, waiting for one to be given back if every lane is in use
    pub async fn take(self: &Arc<Self>) -> Taken {
        let permit = self.free.acquire().await.expect("the pool is never closed");
        permit.forget();
        let lane = self.idle.lock().expect("not poisoned").pop();
        Taken {
            pool: self.clone(),
            lane,
            broken: false,
        }
    }

    /// Send a request on a lane of this pool and read its answer, refusing an error answer
    ///
    /// # Arguments
    ///
    /// * `frame` - The request's frame
    /// * `expect` - The kind of answer that means it was done
    ///
    /// # Errors
    ///
    /// When the lane fails or the holder answers otherwise.
    pub async fn call(self: &Arc<Self>, frame: &[u8], expect: Kind) -> color_eyre::Result<Vec<u8>> {
        let mut lane = self.take().await;
        let (kind, body) = lane.call(frame).await?;
        if kind == Kind::Error {
            bail!("{} refused: {}", self.name, String::from_utf8_lossy(&body));
        }
        if kind != expect {
            bail!("{} answered {kind:?} where {expect:?} was expected", self.name);
        }
        Ok(body)
    }

    /// The holder's counters
    ///
    /// # Errors
    ///
    /// When the holder cannot be asked.
    pub async fn stats(self: &Arc<Self>) -> color_eyre::Result<HolderStats> {
        let body = self.call(&Request::Stats.frame(), Kind::StatsOut).await?;
        HolderStats::decode(&body).map_err(|error| eyre!("{}'s counters: {error}", self.name))
    }

    /// The holder's device's sync floor
    ///
    /// # Arguments
    ///
    /// * `count` - How many overwrites and syncs it is taken over
    ///
    /// # Errors
    ///
    /// When the holder cannot be asked.
    pub async fn probe(self: &Arc<Self>, count: u32) -> color_eyre::Result<ProbeOut> {
        let body = self.call(&Request::Probe { count }.frame(), Kind::ProbeOut).await?;
        ProbeOut::decode(&body).map_err(|error| eyre!("{}'s probe: {error}", self.name))
    }
}

impl Taken {
    /// Send one whole frame on this lane and read the answer
    ///
    /// # Arguments
    ///
    /// * `frame` - The request's frame
    ///
    /// # Errors
    ///
    /// When there is no lane or the call fails, after which the lane is replaced.
    pub async fn call(&mut self, frame: &[u8]) -> color_eyre::Result<(Kind, Vec<u8>)> {
        // a lane lost to an earlier failure is opened again here
        if self.lane.is_none() {
            self.lane = Some(Lane::open(self.pool.addr, &self.pool.tls).await?);
        }
        let lane = self.lane.as_mut().expect("a lane was just opened");
        let answer = lane.call(frame).await;
        if answer.is_err() {
            self.broken = true;
        }
        answer
    }
}

impl Drop for Taken {
    /// Give the lane back, or leave its place empty to be opened again if it broke
    fn drop(&mut self) {
        let lane = self.lane.take().filter(|_| !self.broken);
        if let Some(lane) = lane {
            self.pool.idle.lock().expect("not poisoned").push(lane);
        }
        // the permit returns either way; a missing lane is opened by whoever takes its place
        self.pool.free.add_permits(1);
    }
}
