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
// gxhash is pinned here to the same major `shoal-core` uses rather than to the workspace one,
// which is a different major again - see known issue 65. It has to come from this crate because
// `PartitionKeySupport` lives here and a client hashes its own partition keys, and there has to
// be exactly one of it, because two would mean the client and the ring hashing a key differently.
pub use deepsize2;
pub use gxhash;
pub use rkyv;
pub use serde_json;
pub use tracing;
pub use uuid;
