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
pub use persistent::PersistentTable;
