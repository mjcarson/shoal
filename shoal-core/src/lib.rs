#![feature(trivial_bounds)]

pub mod client;
pub mod server;
mod utils;

// The protocol is a crate of its own now, but from inside the server `shared` still means what it
// always meant, so the whole of `server/` names it unchanged. This is not a compatibility shim -
// it is how the engine refers to the wire format.
pub use shoal_proto::shared;

pub use client::FromShoal;
pub use server::tables;
pub use server::tables::storage;
pub use server::ShoalPool;

// The crates the generated code names by path. They are re-exported from here so that the facade
// can hand a schema the exact versions this crate's trait signatures were compiled against,
// instead of whatever the schema's own manifest happened to resolve - which is what known issue
// 54 was really about.
pub use deepsize2;
pub use glommio;
pub use gxhash;
pub use kanal;
pub use lru;
pub use rkyv;
pub use serde;
pub use serde_json;
pub use tracing;
pub use uuid;
