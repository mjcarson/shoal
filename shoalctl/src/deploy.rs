//! Deploying a cluster of any schema to hosts reached over ssh
//!
//! `shoalctl cluster bootstrap` and `shoalctl cluster add` are runbooks
//! [1](../../docs/src/operations/runbooks.md#1-bootstrap) and
//! [2](../../docs/src/operations/runbooks.md#2-add-a-node) as a program
//! ([F51](../../docs/src/features/cluster-deployment.md)):
//!
//! - an [`inventory`] names the server program, the hosts and the cluster's shape;
//! - [`state`] keeps what the deployment minted - the authority, the admin password, the node ids;
//! - [`render`] writes each node's `shoal.yml` and [`unit`] its systemd unit;
//! - [`pki`] issues each node a leaf naming the id its `claim` printed;
//! - [`remote`] runs every command over ssh in batch mode;
//! - [`ops`] puts them together and waits on the cluster's own admin frames between steps.
//!
//! Nothing here links the engine: the server program is a build of `shoal::server::node::main`
//! for the same schema this program's client was built for, and it is copied, not compiled.

pub mod inventory;
pub mod ops;
pub mod pki;
pub mod remote;
pub mod render;
pub mod state;
pub mod unit;

pub use inventory::Inventory;
pub use ops::Deployment;
