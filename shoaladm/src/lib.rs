//! shoaladm - deploying and operating Shoal clusters
//!
//! Everything an operator does to a cluster that is not a query lives here: building an
//! inventory ([`wizard`]), deploying the nodes it names over ssh and running the day-to-day
//! around them ([`deploy`]), the model of a cluster its admin frames describe ([`cluster`]),
//! and, since [F63](../../docs/src/features/shoaladm.md), building the node program itself
//! from the schema's own Rust project for every host's cpu ([`project`], [`build`], [`cpu`]).
//!
//! A schema is a compile-time construct, so anything that connects to a cluster is built
//! against its schema. The `shoaladm` program handles that itself: run in a project directory
//! it finds the `#[shoal::db]` struct, builds an admin program for it and hands the command
//! over. A program of the schema's own is the same thing written by hand:
//!
//! ```ignore
//! fn main() -> shoaladm::Result<()> {
//!     shoaladm::cli::main_blocking::<MyDbClient>()
//! }
//! ```
//!
//! The terminal UI that queries a database is [`shoalctl`](../shoalctl/index.html), which
//! depends on this crate for the cluster tab's model and for connecting to a deployed cluster
//! as its admin.

pub mod build;
pub mod cli;
pub mod cluster;
pub mod config;
pub mod cpu;
pub mod deploy;
pub mod front;
pub mod project;
pub mod wizard;

/// The result type every command returns, so a schema's program needs no dependency for it
pub type Result<T, E = color_eyre::Report> = std::result::Result<T, E>;

/// The error type inside [`Result`]
pub use color_eyre::Report;
