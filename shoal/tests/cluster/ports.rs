//! The fixture's peer ports, handed out from a fixed block below the ephemeral range
//!
//! Every node the fixture starts binds a data port and a control port. Until
//! [Resolved #102](../../../docs/src/appendix/resolved/fixture-port-block.md) both were taken from
//! the ephemeral range by binding a never-listening `SO_REUSEPORT` socket and dropping it once the
//! nodes had bound - which left a *deferred* node's ports free between the drop and its start,
//! free for any other test in the suite to be handed as the local end of an outbound connection,
//! after which the deferred node failed to bind with `AddrInUse`. And a listener cannot bind over
//! a client-side `TIME_WAIT` whatever it sets, so the reservation could not be extended to cover
//! the gap.
//!
//! A port below the ephemeral floor is never the local end of an outbound connection, which is
//! the whole defect. So the fixture hands out sequential ports from [`FIXTURE_BASE_PORT`], refuses
//! to reach the floor, and reserves nothing: the number is the reservation. The block sits above
//! everything the benchmark harness binds - `12000` plus a workload's position for the single
//! node arms, `13871` for `stage_join`, `20000` plus a cluster arm's block for the cluster arms -
//! and only `cluster_fixture.rs` includes this module, so one counter is the whole space.

use std::sync::atomic::{AtomicU16, Ordering};

use super::FixtureError;

/// The first port the fixture hands out
///
/// Above the benchmark harness's ranges, which end below `28_000` for every arm count the
/// harness allows, and low enough that the suite's worst case - every test at eight nodes with
/// two ports each, plus every clone and restart at fresh ports - stays under the floor.
pub const FIXTURE_BASE_PORT: u16 = 28_000;

/// The ephemeral floor assumed when the kernel does not say
///
/// Linux's default `net.ipv4.ip_local_port_range` begins here.
pub const DEFAULT_EPHEMERAL_FLOOR: u16 = 32_768;

/// The next port to hand out
static NEXT_PORT: AtomicU16 = AtomicU16::new(FIXTURE_BASE_PORT);

/// The first port the kernel hands out as the local end of an outbound connection
///
/// Read from `/proc/sys/net/ipv4/ip_local_port_range` so the bound is the host's rather than
/// the default's; the default when the file cannot be read or parsed.
pub fn ephemeral_floor() -> u16 {
    // the file holds two numbers, the floor and the ceiling
    std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
        .ok()
        .and_then(|range| range.split_whitespace().next()?.parse().ok())
        .unwrap_or(DEFAULT_EPHEMERAL_FLOOR)
}

/// Hand out the next port of the block
///
/// The port is not bound here - a port below the floor is ours by number - but it is probed
/// once, so a port some other process on the host happens to hold is skipped with a note
/// rather than handed to a child that will fail to bind it.
///
/// # Errors
///
/// Refuses once the block reaches the ephemeral floor, naming the item, since a port in the
/// ephemeral range is the defect this module exists to keep out of the suite.
pub fn next_port() -> Result<u16, FixtureError> {
    let floor = ephemeral_floor();
    loop {
        // take the next number of the block
        let port = NEXT_PORT.fetch_add(1, Ordering::SeqCst);
        // never reach the ephemeral range, where a client-side TIME_WAIT can take the port
        if port >= floor || port < FIXTURE_BASE_PORT {
            return Err(FixtureError::Allocation(format!(
                "the fixture's port block ran into the ephemeral range at {port} (floor {floor}); \
                 a listener there can lose its port to an outbound connection (item 102)"
            )));
        }
        // a port a foreign process holds is not ours, whatever the block says
        match std::net::TcpListener::bind(("127.0.0.1", port)) {
            Ok(probe) => {
                // the probe never accepted anything, so closing it leaves no TIME_WAIT behind
                drop(probe);
                return Ok(port);
            }
            Err(error) => {
                eprintln!("port {port} of the fixture's block is held by another process ({error}); skipping it");
            }
        }
    }
}

/// The two ports a node binds: its data port and its control port
///
/// # Errors
///
/// As [`next_port`].
pub fn next_pair() -> Result<(u16, u16), FixtureError> {
    // the data port first, then the control port, so a node's two ports are adjacent
    let data = next_port()?;
    let control = next_port()?;
    Ok((data, control))
}

#[cfg(test)]
mod tests {
    use super::{ephemeral_floor, next_pair, next_port, FIXTURE_BASE_PORT};

    /// Every port the fixture hands out sits below the host's ephemeral floor and above the
    /// benchmark harness's ranges
    ///
    /// The harness's top is `20_000` plus eight nodes of eight ports for every cluster arm; the
    /// bound asserted here is what the `cluster_ports` allocator in `shoal-bench` enforces from
    /// the other side.
    #[test]
    fn fixture_ports_sit_below_the_ephemeral_floor_and_above_the_bench_ranges() {
        // the floor the host runs under, which the default only stands in for
        let floor = ephemeral_floor();
        assert!(
            floor > FIXTURE_BASE_PORT,
            "the host's ephemeral floor {floor} is inside the fixture's block"
        );
        // a handful of ports, in order, each of them ours by number
        let first = next_port().expect("a port");
        let (data, control) = next_pair().expect("a pair");
        assert!(first >= FIXTURE_BASE_PORT && first < floor, "{first}");
        assert!(
            data > first && control > data && control < floor,
            "{data} {control}"
        );
        // the single node arms end at 12_399, stage_join sits at 13_871, and the cluster arms
        // end below 20_000 + 64 * (their count), which is far below the block
        assert!(FIXTURE_BASE_PORT > 20_000 + 64 * 100);
    }
}
