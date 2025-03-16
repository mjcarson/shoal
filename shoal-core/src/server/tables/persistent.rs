//! Persistent tables cache hot data in memory while also storing data on disk.
//!
//! This means that data is retained through restarts at the cost of speed.

mod sorted;
mod unsorted;

pub use sorted::PersistentSortedTable;
pub use unsorted::PersistentUnsortedTable;
