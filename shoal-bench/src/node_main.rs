//! A Shoal node serving the bench schema, for `shoal-benchctl cluster` to deploy
//!
//! The same schema every workload drives, served from a plain `shoal.yml` rather than one a
//! workload resolved, so a cluster deployed with it can be driven by the workloads' own client
//! types from anywhere ([F51](../../docs/src/features/cluster-deployment.md)).

use mimalloc::MiMalloc;

/// The same allocator the workloads' servers use
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Serve or claim a node of the bench schema
fn main() -> Result<(), shoal::server::ServerError> {
    shoal::server::node::main::<shoal_bench::workloads::schema::Bench>()
}
