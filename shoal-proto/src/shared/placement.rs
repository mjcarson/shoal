//! The placement rule both peers compute: which tablet a key is in, which members hold it, and
//! which of them its group's lead belongs with
//!
//! The server routes by these functions and a client that routes by topology computes the same
//! answers from the frame it is pushed ([F74](../../../docs/src/features/client-routing.md)).
//! They live here, in the crate both peers link, so there is one copy of each: a client and a
//! server that disagreed about a placement would not be wrong, since the server forwards what
//! it does not serve, but every query the two disagreed about would pay the hop routing exists
//! to remove. Everything here is pure and names no runtime.

use super::identity::{GroupId, NodeId, ShardAddr};

/// The number of bits of a partition key that name its tablet
///
/// Taken from the top of the key rather than the bottom so that a tablet can later be
/// split in two by consuming one more bit: its keys stay contiguous and no other tablet
/// is disturbed. A tablet id taken modulo the tablet count could not be split at all.
pub const TABLET_BITS: u32 = 12;

/// The number of tablets the partition key space is cut into
///
/// This has to be far larger than any shard count for the split to be even, and small
/// enough that the map stays resident — 4096 `u16`s is 8 KiB.
pub const TABLET_COUNT: usize = 1 << TABLET_BITS;

/// The tablet a partition key belongs to
///
/// # Arguments
///
/// * `partition` - The partition key, as its hash
#[must_use]
pub fn tablet_of(partition: u64) -> usize {
    // take the high bits of the key, which is what leaves room for a later split
    //
    // this cannot exceed TABLET_COUNT, since we keep only TABLET_BITS of the key
    #[allow(clippy::cast_possible_truncation)]
    let tablet = (partition >> (u64::BITS - TABLET_BITS)) as usize;
    tablet
}

/// The node and shard a tablet belongs to under a placement
///
/// Returns the node's position in the placement and the shard on it: the node is the tablet
/// modulo the node count, and the next digit up chooses the shard, so the two are chosen
/// independently.
///
/// # Arguments
///
/// * `tablet` - The tablet, as [`tablet_of`] names it
/// * `shards_per_node` - How many shards each node of the placement runs, in placement order
#[must_use]
pub fn owner_of(tablet: usize, shards_per_node: &[u16]) -> (usize, u16) {
    let nodes = shards_per_node.len();
    let which = tablet % nodes;
    // the next digit up chooses the shard, so a node and a shard are chosen independently
    //
    // truncation cannot happen: the modulus is a u16
    #[allow(clippy::cast_possible_truncation)]
    let shard = ((tablet / nodes) % usize::from(shards_per_node[which].max(1))) as u16;
    (which, shard)
}

/// How many replicas each tablet has: the desired factor or the placement's size, whichever is
/// smaller; none before a placement
///
/// # Arguments
///
/// * `desired` - The factor the policy asks for
/// * `placed` - How many nodes the placement names
#[must_use]
pub fn active_rf(desired: u32, placed: usize) -> u32 {
    // nothing is placed before an operator initializes a placement
    if placed == 0 {
        return 0;
    }
    let nodes = u32::try_from(placed).unwrap_or(u32::MAX);
    desired.max(1).min(nodes)
}

/// The replicas of a tablet under the placement rule alone, the placement primary first
///
/// Tablet `t` lives on `counts[(t + k) % N]` for `k` below `copies`, and on each of those nodes
/// on shard `(t / N) % shards` - the same shard the primary rule picks, so a node's replica of a
/// tablet is on the shard that would own it were the node primary. The nodes are distinct by
/// construction, since the factor never passes the placement's size.
///
/// # Arguments
///
/// * `tablet` - The tablet
/// * `counts` - Each placed node with its shard count, in placement order
/// * `copies` - The active replication factor
#[must_use]
pub fn rule_replicas(tablet: usize, counts: &[(NodeId, u16)], copies: usize) -> Vec<ShardAddr> {
    let nodes = counts.len();
    // an empty placement places nothing
    if nodes == 0 {
        return Vec::new();
    }
    (0..copies)
        .map(|k| {
            let (node, shards) = counts[(tablet + k) % nodes];
            // truncation cannot happen: the modulus is a u16
            #[allow(clippy::cast_possible_truncation)]
            let shard = ((tablet / nodes) % usize::from(shards.max(1))) as u16;
            ShardAddr::new(node, shard)
        })
        .collect()
}

/// A voter's weighted rendezvous score for a group, the highest of which leads it
///
/// `-weight / ln(u)` for a `u` in (0, 1) drawn from the group and the node alone: the voter
/// with the highest score wins, and each wins with probability proportional to its weight.
/// The hash is SplitMix64's finaliser over the two identities, so every node and every client
/// computes the same score whatever build or process it runs
/// ([F58](../../../docs/src/features/weighted-leadership.md)).
///
/// # Arguments
///
/// * `group` - The group
/// * `node` - The voter's node
/// * `weight` - The voter's lead weight, at least one
#[must_use]
pub fn rendezvous_score(group: GroupId, node: NodeId, weight: u32) -> f64 {
    // fold the node's sixteen bytes and the group into one word, then mix it
    let bytes = node.0.as_bytes();
    let high = u64::from_le_bytes(bytes[..8].try_into().expect("eight bytes"));
    let low = u64::from_le_bytes(bytes[8..].try_into().expect("eight bytes"));
    let mut mixed = group.0 ^ high.rotate_left(17) ^ low.rotate_left(41);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^= mixed >> 31;
    // the top 53 bits as a uniform in (0, 1), never zero or one
    #[allow(clippy::cast_precision_loss)]
    let uniform = ((mixed >> 11) as f64 + 0.5) / (1u64 << 53) as f64;
    -f64::from(weight) / uniform.ln()
}

/// The voter a group's lead belongs with, by its members' lead weights
///
/// With every voter at the same weight - a cluster that sets none - it is the placement primary,
/// the first voter, whether or not it is up, which spreads leads evenly. Otherwise it is the up
/// voter with the highest weighted rendezvous score for the group, so each voter leads about its
/// weight's share of the groups it is in and one voter going down moves only the leads it held
/// ([F58](../../../docs/src/features/weighted-leadership.md)). The server hands every group's
/// lead back to this voter once it has settled, so in a steady cluster it is the leader.
///
/// # Arguments
///
/// * `group` - The group's identity
/// * `voters` - Its voters, the placement primary first
/// * `weight` - A voter's lead weight, at least one
/// * `is_up` - Whether a voter's node is up
#[must_use]
pub fn preferred_leader(
    group: GroupId,
    voters: &[ShardAddr],
    weight: impl Fn(NodeId) -> u32,
    is_up: impl Fn(NodeId) -> bool,
) -> Option<ShardAddr> {
    let primary = voters.first().copied()?;
    // equal weights are the placement's own spread
    let primary_weight = weight(primary.node);
    if voters
        .iter()
        .all(|voter| weight(voter.node) == primary_weight)
    {
        return Some(primary);
    }
    // the up voter scoring highest; a down one cannot take a lead it is handed
    voters
        .iter()
        .filter(|voter| is_up(voter.node))
        .map(|voter| {
            (
                rendezvous_score(group, voter.node, weight(voter.node)),
                *voter,
            )
        })
        .max_by(|a, b| a.0.total_cmp(&b.0))
        .map(|(_, voter)| voter)
}

#[cfg(test)]
mod tests {
    use super::{
        active_rf, owner_of, preferred_leader, rendezvous_score, rule_replicas, tablet_of,
        TABLET_COUNT,
    };
    use crate::shared::identity::{GroupId, NodeId, ShardAddr};

    /// A key's tablet is its top twelve bits, and every key lands on a tablet that exists
    #[test]
    fn a_tablet_is_the_top_twelve_bits_of_a_key() {
        assert_eq!(tablet_of(0), 0);
        assert_eq!(tablet_of(u64::MAX), TABLET_COUNT - 1);
        assert_eq!(tablet_of(1 << 52), 1);
        assert_eq!(tablet_of((1 << 52) - 1), 0);
    }

    /// The rule spreads replicas over distinct nodes on the primary's shard
    #[test]
    fn the_rule_places_copies_on_distinct_nodes() {
        let nodes: Vec<(NodeId, u16)> = (1..=4u64).map(|n| (NodeId::from(n), 3)).collect();
        for tablet in 0..TABLET_COUNT {
            let replicas = rule_replicas(tablet, &nodes, 3);
            assert_eq!(replicas.len(), 3);
            // the primary is the placement rule's owner
            let shards: Vec<u16> = nodes.iter().map(|(_, shards)| *shards).collect();
            let (which, shard) = owner_of(tablet, &shards);
            assert_eq!(replicas[0], ShardAddr::new(nodes[which].0, shard));
            // every copy is on another node and on the primary's shard
            for (k, replica) in replicas.iter().enumerate() {
                assert!(replicas[..k].iter().all(|other| other.node != replica.node));
                assert_eq!(replica.shard, shard);
            }
        }
        assert!(rule_replicas(5, &[], 3).is_empty());
        assert_eq!(active_rf(3, 0), 0);
        assert_eq!(active_rf(3, 2), 2);
        assert_eq!(active_rf(0, 5), 1);
    }

    /// The rendezvous score is frozen: a client and a server of different builds agree on it
    ///
    /// Moving it would not be incorrect, since a server forwards what it does not lead, but every
    /// client of the old build would route writes to a node that hops them
    /// ([F74](../../../docs/src/features/client-routing.md)).
    #[test]
    fn the_rendezvous_score_is_frozen() {
        let node = NodeId(uuid::Uuid::from_u128(
            0x0123_4567_89ab_cdef_0011_2233_4455_6677,
        ));
        let score = rendezvous_score(GroupId(0xdead_beef), node, 2);
        assert_eq!(score.to_bits(), FROZEN_SCORE_BITS, "score {score}");
    }

    /// The bits of the frozen score above
    const FROZEN_SCORE_BITS: u64 = 0x3ff1_8d91_9ee6_c5a0;

    /// Equal weights name the primary, and unequal ones pass over a voter that is down
    #[test]
    fn the_lead_follows_the_weights() {
        let voters: Vec<ShardAddr> = (1..=3u64)
            .map(|n| ShardAddr::new(NodeId::from(n), 0))
            .collect();
        let group = GroupId(42);
        // equal weights: the primary, even when it is down
        let led = preferred_leader(group, &voters, |_| 1, |node| node != voters[0].node);
        assert_eq!(led, Some(voters[0]));
        // unequal weights: never a voter that is down
        for down in &voters {
            let led = preferred_leader(
                group,
                &voters,
                |node| if node == voters[1].node { 3 } else { 1 },
                |node| node != down.node,
            );
            assert!(led.is_some_and(|led| led.node != down.node));
        }
        assert_eq!(preferred_leader(group, &[], |_| 1, |_| true), None);
    }
}
