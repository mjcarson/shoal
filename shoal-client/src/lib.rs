//! Shoal's tokio client
//!
//! This is the half of Shoal that talks to a database without being one. It depends on
//! `shoal-proto` for the wire format and on tokio for the socket, and on no storage engine at
//! all - which is the whole point of [F15](../../../docs/src/features/client-server-split.md).
//! Before it existed, opening a connection meant compiling glommio, and therefore io_uring, and
//! therefore Linux.
//!
//! tokio is the only backend shipped, and
//! [D5](../../../docs/src/direction/runtimes.md) argues it should stay that way until somebody
//! asks for another. Nothing here is generic over a runtime; the separation that matters is the
//! one between this crate and the engine, not the one between tokio and its alternatives.
//!
//! Nothing outside `shoal` should name this crate directly - callers go through the facade.

pub mod client;

pub use client::Shoal;
