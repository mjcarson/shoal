//! `stripes`: posters past the inline threshold, written and read through every member
//!
//! The lab's check that a client's bytes reach the pool's devices and come back whole
//! ([F86](../../../docs/src/features/replicated-pools.md)). Through every member in turn it puts a
//! poster of each size, past the pool's 16 KiB threshold so each is stripes, reads every one back
//! whole through every member at `One` and at `Quorum`, writes part of each in place - once under
//! the pool's small-write threshold, so it rides its commit, and once past it, so it is staged -
//! and reads them all back again. Every read is compared byte for byte with what was written. The
//! posters it writes are kept under `stripes/`, so a second run replaces them.

use clap::{ArgGroup, Args};
use color_eyre::eyre::bail;
use shoal::shared::protocol::read::ReadLevel;
use std::path::PathBuf;

use crate::load::connect_targets;
use crate::Posters;

/// Write posters past the inline threshold through every member and read them back everywhere
#[derive(Args, Debug, Clone)]
#[command(group(ArgGroup::new("target").required(true).args(["inventory", "addr"])))]
pub struct StripesArgs {
    /// The inventory of the deployed cluster to write through
    #[clap(long, short)]
    pub inventory: Option<PathBuf>,
    /// A single node's client address, for a node started by hand
    #[clap(long)]
    pub addr: Option<String>,
    /// The sizes of the posters each member puts, in bytes
    #[clap(long, value_delimiter = ',', default_values_t = [20_000u64, 1 << 20, (9 << 20) + 11])]
    pub sizes: Vec<u64>,
}

/// Bytes that differ at every offset and between posters, so a byte from the wrong place shows
///
/// # Arguments
///
/// * `len` - How many
/// * `seed` - What sets them apart
fn poster_bytes(len: u64, seed: u64) -> Vec<u8> {
    (0..len)
        .map(|index| (shoal::shared::rendezvous::mix64(index ^ seed.rotate_left(40)) >> 24) as u8)
        .collect()
}

/// Lay a write over a poster's bytes as the cluster should
///
/// # Arguments
///
/// * `bytes` - The poster
/// * `offset` - Where the write starts
/// * `patch` - What it writes
fn laid(bytes: &mut [u8], offset: u64, patch: &[u8]) {
    let at = offset as usize;
    bytes[at..at + patch.len()].copy_from_slice(patch);
}

/// Put posters through every member, write part of each in place, and read them all through every
/// member at both levels
///
/// # Arguments
///
/// * `args` - Which cluster, and the posters' sizes
///
/// # Errors
///
/// When a member cannot be reached, a write is refused, or any read is not what was written.
pub async fn stripes(args: StripesArgs) -> color_eyre::Result<()> {
    // every member as the deployment's user, each sending only through its own
    let clients = connect_targets(
        args.inventory.as_ref(),
        args.addr.as_deref(),
        shoal::client::Routing::Endpoints,
    )
    .await?;
    // a poster of each size through each member, and what each now holds
    let mut posters: Vec<(String, Vec<u8>)> = Vec::new();
    for (member, client) in clients.iter().enumerate() {
        for (index, size) in args.sizes.iter().enumerate() {
            let path = format!("stripes/member-{member}-{index}.bin");
            let bytes = poster_bytes(*size, (member as u64) << 8 | index as u64);
            let put = client.bucket::<Posters>().put(&path, &bytes).await?;
            println!("member {member} put {path}: {} bytes, object {:032x}", put.size, put.object);
            posters.push((path, bytes));
        }
    }
    // every poster read through every member, at both levels
    let mut wrong = Vec::new();
    let mut check = |pass: &str, member: usize, level: ReadLevel, path: &str, read: &[u8], held: &[u8]| {
        if read != held {
            wrong.push(format!("{pass}: member {member} at {level:?} read {path} other than written"));
        }
    };
    for (member, client) in clients.iter().enumerate() {
        for (path, bytes) in &posters {
            for level in [ReadLevel::One, ReadLevel::Quorum] {
                let (_, read) = client
                    .bucket::<Posters>()
                    .read_range(path, 0, bytes.len() as u64, Some(level))
                    .await?;
                check("after the puts", member, level, path, &read, bytes);
            }
        }
    }
    // part of each written in place through the next member: a small write and a staged one
    for (at, (path, bytes)) in posters.iter_mut().enumerate() {
        let client = &clients[(at + 1) % clients.len()];
        let small = poster_bytes(4096.min(bytes.len() as u64 / 2), 0x5a5a ^ at as u64);
        client.bucket::<Posters>().write_at(path, 100, &small).await?;
        laid(bytes, 100, &small);
        let staged_len = (200_000u64).min(bytes.len() as u64 / 2);
        let staged = poster_bytes(staged_len, 0xa5a5 ^ at as u64);
        let offset = bytes.len() as u64 - staged_len;
        client.bucket::<Posters>().write_at(path, offset, &staged).await?;
        laid(bytes, offset, &staged);
    }
    for (member, client) in clients.iter().enumerate() {
        for (path, bytes) in &posters {
            for level in [ReadLevel::One, ReadLevel::Quorum] {
                let (_, read) = client
                    .bucket::<Posters>()
                    .read_range(path, 0, bytes.len() as u64, Some(level))
                    .await?;
                check("after the writes in place", member, level, path, &read, bytes);
            }
        }
    }
    if !wrong.is_empty() {
        bail!("{} reads were not what was written: {wrong:?}", wrong.len());
    }
    println!(
        "{} posters past the inline threshold, written through {} members and in place, read back \
         whole through every member at One and at Quorum",
        posters.len(),
        clients.len()
    );
    Ok(())
}
