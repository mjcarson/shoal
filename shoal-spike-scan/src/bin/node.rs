//! A Shoal node serving spike X16's schema, the program the side cluster's inventory names as its
//! `server:`
//!
//! ```sh
//! x16-node serve --conf shoal.yml
//! ```
//!
//! It has no settings of its own: every host runs it under the unit the bench's deploy installs,
//! against the `shoal.yml` the deployment rendered for that node.

/// The allocator every Shoal server program in this workspace runs with
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Serve or claim a node of the spike's schema
fn main() -> Result<(), shoal::server::ServerError> {
    shoal::server::node::main::<shoal_spike_scan::ScanLab>()
}
