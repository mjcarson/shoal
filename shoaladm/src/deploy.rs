//! Deploying a cluster of any schema to hosts reached over ssh
//!
//! `shoaladm bootstrap` and `shoaladm add` are runbooks
//! [1](../../docs/src/operations/runbooks.md#1-bootstrap) and
//! [2](../../docs/src/operations/runbooks.md#2-add-a-node) as a program
//! ([F51](../../docs/src/features/cluster-deployment.md)):
//!
//! - an [`inventory`] names the server program, the hosts and the cluster's shape;
//! - [`state`] keeps what the deployment minted - the authority, the admin password, the node ids;
//! - [`render`] writes each node's `shoal.yml` and [`unit`] its systemd unit;
//! - [`pki`] issues each node a leaf naming the id its `claim` printed;
//! - [`remote`] runs every command over ssh in batch mode;
//! - [`ops`] puts them together and waits on the cluster's own admin frames between steps;
//! - [`upgrade`] replaces every node's program one node at a time, runbook
//!   [7](../../docs/src/operations/runbooks.md#7-rolling-upgrade)
//!   ([F55](../../docs/src/features/cluster-upgrade.md));
//! - [`ship`] copies a backup's files to every host, so a restore finds each group's file on its
//!   new leader ([F59](../../docs/src/features/backup-shipping.md));
//! - [`programs`] decides where the node program comes from: the inventory's built one, or one
//!   built from the schema's project for each host's cpu ([F63](../../docs/src/features/shoaladm.md)).
//!
//! Nothing here links the engine: the server program is a build of `shoal::server::node::main`
//! for the same schema this program's client was built for. It is copied to the hosts, and when
//! it is built it is built by cargo in the schema's own project, never linked here.

pub mod inventory;
pub mod ops;
pub mod pki;
pub mod programs;
pub mod remote;
pub mod render;
pub mod state;
pub mod unit;
pub mod rebuild;
pub mod ship;
pub mod upgrade;

pub use inventory::Inventory;
pub use ops::Deployment;
pub use programs::{Programs, ProjectHint};
