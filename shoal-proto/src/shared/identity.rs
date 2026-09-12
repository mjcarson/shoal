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

/// The seed every table identity is hashed under
///
/// Frozen: a table's identity is persisted in the control state and named on the wire, so a
/// changed seed would give every table a new identity and orphan every placement that named
/// the old one. Zero, which is what every partition key is hashed under as well.
pub const TABLE_ID_SEED: i64 = 0;

/// The identity of one table, stable across builds, peers and restarts
///
/// The hash of the table's name under [`TABLE_ID_SEED`], which is what makes it schema metadata
/// rather than layout: a peer's enum discriminant moves when a table is added ahead of it, and
/// P2 of the protocol contract ([C13](../../../docs/src/distributed/protocol.md)) forbids a
/// stream identity that can move like that. Two tables of one schema with one name is a schema
/// that does not compile, so the identities of one schema are distinct by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableId(pub u64);

impl TableId {
    /// Derive the identity of a table from its name
    ///
    /// # Arguments
    ///
    /// * `name` - The table's name, which is the variant the schema's table enum spells it as
    #[must_use]
    pub fn of(name: &str) -> Self {
        TableId(gxhash::gxhash64(name.as_bytes(), TABLE_ID_SEED))
    }
}

impl fmt::Display for TableId {
    /// Render the whole hash in hex, since a prefix is not a key
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// One shard of one node: where a replica of a tablet lives
///
/// The member of a data-plane replication group ([F40](../../../docs/src/features/replication.md)).
/// A node holds a tablet on exactly one of its shards, so the pair names a replica without
/// ambiguity, and the pair rather than the node is what a group's log names as its voters:
/// a shard is the unit that owns a WAL and applies a command, and a group's members are the
/// shards that hold copies of its tablets. `Display` renders `node/shard`, which is what a
/// consensus library's log lines and a fixture's digests print.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
pub struct ShardAddr {
    /// The node
    pub node: NodeId,
    /// Which of that node's shards
    pub shard: u16,
}

impl ShardAddr {
    /// Name a shard of a node
    ///
    /// # Arguments
    ///
    /// * `node` - The node
    /// * `shard` - Which of its shards
    #[must_use]
    pub const fn new(node: NodeId, shard: u16) -> Self {
        ShardAddr { node, shard }
    }

    /// The eighteen bytes this address is written as in a frame: the node, then the shard
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 18] {
        let mut out = [0u8; 18];
        out[..16].copy_from_slice(self.node.0.as_bytes());
        out[16..].copy_from_slice(&self.shard.to_le_bytes());
        out
    }

    /// Read an address back out of its eighteen bytes
    ///
    /// # Arguments
    ///
    /// * `raw` - The bytes `to_bytes` wrote
    #[must_use]
    pub fn from_bytes(raw: &[u8; 18]) -> Self {
        let mut node = [0u8; 16];
        node.copy_from_slice(&raw[..16]);
        ShardAddr {
            node: NodeId(Uuid::from_bytes(node)),
            shard: u16::from_le_bytes([raw[16], raw[17]]),
        }
    }
}

impl fmt::Display for ShardAddr {
    /// Render `node/shard`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.node, self.shard)
    }
}

impl From<u64> for ShardAddr {
    /// Build an address from a small integer, which is what a conformance suite hands out
    ///
    /// The integer names the node the way [`NodeId::from`] does, on shard zero.
    ///
    /// # Arguments
    ///
    /// * `raw` - The integer to build from
    fn from(raw: u64) -> Self {
        ShardAddr {
            node: NodeId::from(raw),
            shard: 0,
        }
    }
}

/// The seed every group identity is hashed under
///
/// Frozen for the same reason [`TABLE_ID_SEED`] is: a group's identity names its frames in every
/// shard's WAL, and a changed seed would orphan every frame ever written.
pub const GROUP_ID_SEED: i64 = 0;

/// The identity of one replication group: a table and the ordered replica set it is served by
///
/// Tablets whose replicas land on the same ordered list of shard addresses share one group, so
/// a group's log is the interleaved history of every tablet it serves and a tablet's history is
/// the subsequence of it that names the tablet ([F40](../../../docs/src/features/replication.md)).
/// The identity is the hash of the table and the address list, so every node computes the same
/// one from the same map without agreeing about anything first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GroupId(pub u64);

impl GroupId {
    /// Derive the identity of the group serving a table over an ordered replica set
    ///
    /// # Arguments
    ///
    /// * `table` - The table
    /// * `members` - The replicas, primary first
    #[must_use]
    pub fn of(table: TableId, members: &[ShardAddr]) -> Self {
        // the table, then every address in order, hashed under the frozen seed
        let mut bytes = Vec::with_capacity(8 + members.len() * 18);
        bytes.extend_from_slice(&table.0.to_le_bytes());
        for member in members {
            bytes.extend_from_slice(&member.to_bytes());
        }
        GroupId(gxhash::gxhash64(&bytes, GROUP_ID_SEED))
    }
}

impl fmt::Display for GroupId {
    /// Render the whole hash in hex, since a prefix is not a key
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterId, GroupId, NodeId, ShardAddr, TableId};

    /// A shard address round trips through its bytes and a group identity follows its members
    #[test]
    fn shard_addresses_and_group_ids_are_stable() {
        let a = ShardAddr::new(NodeId::mint(), 3);
        let b = ShardAddr::new(NodeId::mint(), 0);
        assert_eq!(ShardAddr::from_bytes(&a.to_bytes()), a);
        assert_eq!(a.to_string(), format!("{}/3", a.node));
        // the same table over the same members in the same order is the same group
        let table = TableId::of("Row");
        assert_eq!(GroupId::of(table, &[a, b]), GroupId::of(table, &[a, b]));
        // a different order, table or member is a different group
        assert_ne!(GroupId::of(table, &[a, b]), GroupId::of(table, &[b, a]));
        assert_ne!(GroupId::of(table, &[a, b]), GroupId::of(TableId::of("Note"), &[a, b]));
        assert_ne!(GroupId::of(table, &[a, b]), GroupId::of(table, &[a]));
        // an integer address is the integer node on shard zero
        assert_eq!(ShardAddr::from(7), ShardAddr::new(NodeId::from(7), 0));
    }

    /// A table identity is the hash of its name, distinct across names and stable across calls
    #[test]
    fn table_ids_follow_their_names() {
        assert_eq!(TableId::of("Movie"), TableId::of("Movie"));
        assert_ne!(TableId::of("Movie"), TableId::of("Person"));
        assert_ne!(TableId::of("Movie"), TableId::of("movie"));
        // it serializes as the bare number, and renders as sixteen hex digits
        let id = TableId::of("Movie");
        let json = serde_json::to_string(&id).expect("a table id serializes");
        assert_eq!(json, id.0.to_string());
        assert_eq!(id.to_string().len(), 16);
    }

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
