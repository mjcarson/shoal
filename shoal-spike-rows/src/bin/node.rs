//! A Shoal node serving spike X10's schema, for the spike's inventory to name as its `server:`
//!
//! ```sh
//! x10-node serve --conf shoal.yml
//! x10-node claim --conf shoal.yml
//! ```
//!
//! It has no settings of its own: every host runs it under the systemd unit `x10 bootstrap`
//! installs, against the `shoal.yml` the deployment rendered for that node.

/// The allocator every Shoal server program in this workspace runs with
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Serve or claim a node of spike X10's schema
fn main() -> Result<(), shoal::server::ServerError> {
    shoal::server::node::main::<shoal_spike_rows::Rows>()
}
