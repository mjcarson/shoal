//! Tables are how different data is stored in Thorium
//!
//! Several different types of tables exist with different trade offs:
//!
//! - Ephemeral

mod ephemeral;
mod partitions;
mod persistent;
pub mod storage;

pub use ephemeral::EphemeralTable;
pub use persistent::{PersistentSortedTable, PersistentUnsortedTable};

/// Crate private internals, re-exported so the benches in `shoal/benches` can reach them
///
/// A criterion bench is a separate binary that links this crate from outside, so it can only
/// see public items. The partition types are the hottest CPU bound code in the tree and are
/// exactly what a micro benchmark needs to reach, but making them public outright would
/// commit us to an API we do not want to promise.
///
/// This module is **not** a supported API. It carries no stability guarantee, it is compiled
/// only under the `bench` feature, and nothing outside `shoal/benches` should use it.
#[cfg(feature = "bench")]
#[doc(hidden)]
pub mod bench_exports {
    pub use super::partitions::{
        MaybeLoaded, MaybeRow, PartitionSupport, SeekBytes, SortedPartition, UnsortedPartition,
        ValidatedArchive,
    };
}
