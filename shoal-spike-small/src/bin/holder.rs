//! A holder: one slice's journal and chunks on one glommio executor, as `x8 holders up` runs it
//!
//! ```sh
//! x8-holder --listen 172.16.2.4:13300 --dir /xfs/shoal-x8-holder --cpu 1 --slots 256 \
//!     --chunk-bytes 1048576 --ring-bytes 1073741824 --io-memory 134217728 \
//!     --cert tls/cert.pem --key tls/key.pem --ca tls/ca.pem
//! ```
//!
//! It writes its journal and every chunk ahead before it listens, and serves until it is stopped.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use color_eyre::eyre::eyre;
use shoal::shared::tls::PeerTlsOptions;
use shoal_spike_small::holder::{self, HolderConf, InPlaceSync};

/// The allocator every Shoal server program in this workspace runs with
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Run one holder
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Where it listens: its node's address and the holders' port
    #[clap(long)]
    listen: SocketAddr,
    /// The directory its journal and chunks are in
    #[clap(long)]
    dir: PathBuf,
    /// The cpu its executor is pinned to
    #[clap(long)]
    cpu: usize,
    /// How many chunk slots it keeps
    #[clap(long)]
    slots: u32,
    /// The bytes of units each chunk holds, after its header block
    #[clap(long)]
    chunk_bytes: u64,
    /// The length of the journal's ring
    #[clap(long)]
    ring_bytes: u64,
    /// The memory its executor's ring registers for I/O buffers
    #[clap(long)]
    io_memory: usize,
    /// How an apply or a fold is made durable: `each` syncs its chunk, `batch` shares a flush
    #[clap(long, default_value = "each")]
    in_place_sync: String,
    /// Its certificate
    #[clap(long)]
    cert: PathBuf,
    /// Its key
    #[clap(long)]
    key: PathBuf,
    /// The authority its lanes' peers are signed by
    #[clap(long)]
    ca: PathBuf,
}

/// Run the holder until its process is stopped
fn main() -> color_eyre::Result<()> {
    // parse what we were asked to do
    let args = Args::parse();
    color_eyre::install()?;
    holder::run(HolderConf {
        listen: args.listen,
        dir: args.dir,
        cpu: args.cpu,
        slots: args.slots,
        chunk_bytes: args.chunk_bytes,
        ring_bytes: args.ring_bytes,
        io_memory: args.io_memory,
        in_place: InPlaceSync::from_name(&args.in_place_sync)
            .ok_or_else(|| eyre!("--in-place-sync is each or batch, not {}", args.in_place_sync))?,
        tls: PeerTlsOptions {
            cert: args.cert,
            key: args.key,
            ca: args.ca,
            bind_identity: false,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::Args;
    use clap::CommandFactory;

    /// Every argument has one name
    #[test]
    fn every_argument_has_one_name() {
        Args::command().debug_assert();
    }
}
