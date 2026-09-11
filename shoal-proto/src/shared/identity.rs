//! The two identities a node carries, and the one every client will eventually be told
//!
//! A node is not its address. An address is where a node happens to be reachable today, and
//! the thing membership, placement and a storage directory have to agree about is *which* node
//! that is - across a restart, a re-address and a move to another machine. So a node mints a
//! random [`NodeId`] once, the first time it claims an empty directory, and keeps it in the
//! storage marker for as long as the directory exists ([C1](../../../docs/src/distributed/node-identity.md)).
//!
//! A [`ClusterId`] is minted once, at the one explicit bootstrap that creates a cluster, and
//! every node that joins adopts it. Two nodes with different cluster ids are two clusters, and a
//! directory naming one cluster is refused by a node configured for another - which is what
//! stops a copied disk or a stale seed list from quietly stitching two clusters into one.
//!
//! These live in the protocol crate rather than the engine because a client will see them:
//! the topology view M3 exposes names the node that owns a tablet by its [`NodeId`], and the
//! handshake M2 adds proves a peer's [`ClusterId`]. Neither needs a runtime, and a type the
//! client has to agree about is exactly what this crate is for.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The identity of one node, independent of its address
///
/// Random, minted once for an empty storage directory and never changed afterwards. A short
/// rendering of it is a display convenience only; the protocol key is the whole value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(pub Uuid);

impl NodeId {
    /// Mint a fresh node identity
    ///
    /// Called exactly once per storage directory, by the marker claim. Anything else minting
    /// one is a node that will never be recognised again.
    #[must_use]
    pub fn mint() -> Self {
        NodeId(Uuid::new_v4())
    }
}

impl fmt::Display for NodeId {
    /// Render the whole uuid, since a prefix is not a key
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for NodeId {
    /// The nil uuid, which no minted node ever has
    ///
    /// Exists for the structures a consensus library requires a default of; a node with this
    /// id is a placeholder and never a member.
    fn default() -> Self {
        NodeId(Uuid::nil())
    }
}

impl From<u64> for NodeId {
    /// Build a node id from a small integer, which is what a conformance suite hands out
    ///
    /// The integer lands in the low bits of an otherwise zero uuid, so ids built this way are
    /// ordered the way the integers are and can never collide with a minted one, whose random
    /// high bits are never all zero.
    ///
    /// # Arguments
    ///
    /// * `raw` - The integer to build from
    fn from(raw: u64) -> Self {
        NodeId(Uuid::from_u128(u128::from(raw)))
    }
}

/// The identity of one cluster
///
/// Minted at bootstrap and adopted by every joiner. A directory that names one is a directory
/// that belongs to that cluster, and is refused by any node configured for a different one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClusterId(pub Uuid);

impl ClusterId {
    /// Mint a fresh cluster identity
    ///
    /// Called exactly once, by the bootstrap that creates a cluster. An established directory
    /// never mints a second one, whatever its seeds are doing.
    #[must_use]
    pub fn mint() -> Self {
        ClusterId(Uuid::new_v4())
    }
}

impl Default for ClusterId {
    /// The nil uuid, which no bootstrapped cluster ever has
    ///
    /// Exists for the structures that need a default; a cluster with this id is a placeholder
    /// and never one a node belongs to.
    fn default() -> Self {
        ClusterId(Uuid::nil())
    }
}

impl fmt::Display for ClusterId {
    /// Render the whole uuid, since a prefix is not a key
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterId, NodeId};

    /// Two minted identities are never the same one
    #[test]
    fn minted_identities_are_distinct() {
        assert_ne!(NodeId::mint(), NodeId::mint());
        assert_ne!(ClusterId::mint(), ClusterId::mint());
    }

    /// An identity survives a trip through json unchanged, as a bare string
    #[test]
    fn identities_serialize_transparently() {
        let node = NodeId::mint();
        let json = serde_json::to_string(&node).expect("a node id serializes");
        assert_eq!(json, format!("\"{node}\""));
        let back: NodeId = serde_json::from_str(&json).expect("a node id parses");
        assert_eq!(back, node);
    }

    /// An id built from an integer is ordered like the integer and is never a minted one
    #[test]
    fn integer_ids_are_ordered_and_never_minted() {
        assert!(NodeId::from(1) < NodeId::from(2));
        assert_eq!(NodeId::from(0), NodeId::default());
        // a minted id is a random v4 uuid, whose version nibble alone keeps its high bits non zero
        assert!(NodeId::mint() > NodeId::from(u64::MAX));
    }
}
