//! Shoal's wire format, and everything both peers have to agree about
//!
//! This crate holds the protocol, the query and response types, the traits a schema implements,
//! SCRAM, and the TLS configuration - and it links no async runtime at all. That is the point:
//! a client is not obliged to compile an engine, io_uring, or Linux, which it was until
//! [F15](../../../docs/src/features/client-server-split.md).
//!
//! Nothing outside `shoal-core`, `shoal-client` and `shoal-derive` should name this crate
//! directly. Callers go through the `shoal` facade, which is what lets a fourth crate be added
//! later without a workspace wide rename.

pub mod client;
pub mod shared;
pub mod stamps;

pub use client::FromShoal;

// The crates the generated code names by path, re-exported so a schema resolves the exact
// versions these trait signatures were compiled against rather than whatever its own manifest
// happened to pick. See known issue 54.
// gxhash is deliberately absent. Partition keys are hashed with the one `shoal-core` pins, and
// that pin is a different major to the workspace one - see known issue 65. Re-exporting a second
// gxhash from here would make which one hashes a partition key depend on which re-export the
// facade happened to pick, and changing that silently rehashes every persisted dataset.
pub use deepsize2;
pub use rkyv;
pub use serde_json;
pub use tracing;
pub use uuid;
