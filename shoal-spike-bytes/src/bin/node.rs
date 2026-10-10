//! A Shoal node serving spike X3's schema, for the spike's inventories to name as their `server:`
//!
//! ```sh
//! x3-node serve --conf shoal.yml
//! x3-node claim --conf shoal.yml
//! ```
//!
//! It has no settings of its own: every host runs it under the systemd unit `x3 bootstrap`
//! installs, and europa's local nodes under the transient units `x3 local up` starts, against the
//! `shoal.yml` rendered for that node.

/// The allocator every Shoal server program in this workspace runs with
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Serve or claim a node of spike X3's schema
fn main() -> Result<(), shoal::server::ServerError> {
    shoal::server::node::main::<shoal_spike_bytes::StripesAsRows>()
}
