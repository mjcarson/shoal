//! Spike X16's admin program: every `shoaladm` command for the spike's schema
//!
//! ```sh
//! x16 bench run -i inventory.yml --dataset <folder> --workloads rw50 --events none,scan ...
//! ```
//!
//! The bench builds no wrapper for a schema whose admin program is given, so the side cluster's
//! runs are driven by this one, built at the commit the node is ([F66](../../../docs/src/features/dataset-benchmarks.md)).

/// Run an admin command against the spike's schema
fn main() -> color_eyre::Result<()> {
    shoaladm::cli::main_blocking::<shoal_spike_scan::ScanLabClient>()
}
