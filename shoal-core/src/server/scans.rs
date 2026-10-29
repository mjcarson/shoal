//! What every shard of a node shares for a scan's pages
//! ([F118](../../../docs/src/features/scan-budget.md))
//!
//! A scan's page can be read on any shard of a node, so what bounds a node's scans together lives
//! here, made once at the node's start and handed to every shard: the scan budget every page's
//! buffers are drawn from.
//!
//! # Invariants
//!
//! **One scan budget a node**, never one a shard: the memory it bounds is the process's, and a
//! shard's share of it would let one shard's scans spill past what the node carved.

use std::sync::Arc;

use super::budget::Budget;
use super::conf::Conf;

/// What every shard of a node shares for a scan's pages
#[derive(Debug)]
pub struct ScanResources {
    /// The memory every scan page's buffers on this node are drawn from
    pub budget: Arc<Budget>,
}

impl ScanResources {
    /// What a node's config gives its scans, its scan memory settled at its start
    ///
    /// # Arguments
    ///
    /// * `conf` - The node's config, `resources.scan_memory` settled
    #[must_use]
    pub fn new(conf: &Conf) -> Self {
        ScanResources {
            budget: Budget::new("the scan budget", conf.resources.scan_budget() as u64),
        }
    }
}
