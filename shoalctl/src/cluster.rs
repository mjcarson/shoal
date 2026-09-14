//! The cluster tab: what a cluster looks like to an operator, and the operations they run on it
//!
//! A cluster tab polls the node its client reached for the admin frames every second -
//! `Members`, `Readiness`, `Replication`, `Plans`, `Backups`, `Recoveries` - and draws one
//! model built from them ([`model::ClusterModel`]): every member with its phase, health,
//! grace, weight, bytes and wire version; the desired and active factor; the under-replicated
//! sets; the groups this node hosts and leads and its widest lag; what is installing and
//! quarantined; every open plan with its blocked reason; the recoveries; the activated wire.
//! Its command line takes an operation ([`actions::ClusterAction`]), renders a preview naming
//! the identity it touches, what will move and the boundary that cannot be undone, submits it
//! on a second `Enter`, and follows the record by operation id until it is done
//! ([F50](../../docs/src/features/cluster-operations.md)).
//!
//! Nothing here draws: the model renders itself to lines and the app puts them in the tab's
//! content, so every decision can be tested without a terminal.

pub mod actions;
pub mod model;

pub use actions::{ClusterAction, Follow};
pub use model::ClusterModel;
