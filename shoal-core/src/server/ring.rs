//! The tablet map for Shoal
//!
//! This determines on what shards data lies.
//!
//! A partition key names a tablet, and a tablet names the shard that owns it. The second
//! step is a lookup in a table rather than a hash, which is the whole point: ownership is
//! data, so a tablet can be moved. A hash can only be recomputed, which is why the ring
//! this replaced could not express a migration, a replica set, or a rebalance away from a
//! shard that had grown hot.
//!
//! The map is built whole from the shard count before any shard starts, so no shard ever
//! routes against a partial one.

use super::hosting::Hosting;
use super::shard::{ShardContact, ShardInfo};
use super::ServerError;
use crate::server::errors::ShoalError;
use crate::shared::identity::NodeId;

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

/// The tablet map for Shoal
#[derive(Clone)]
pub struct Ring {
    /// The shard that owns each tablet, indexed by tablet id
    ///
    /// Stored rather than derived, so that a tablet can later be moved by writing to this
    /// map instead of by changing a hash every shard would have to agree about.
    tablets: Vec<u16>,
    /// A map of node info
    pub shards: Vec<ShardInfo>,
}

impl Ring {
    /// Build the tablet map for a node with this many shards
    ///
    /// Every shard builds the same map from the same shard count, so this is complete
    /// before a single query can be routed against it. That is what makes
    /// [`Ring::find_shard`] total.
    ///
    /// # Arguments
    ///
    /// * `shard_count` - The number of shards on this node
    ///
    /// # Errors
    ///
    /// This will fail if there are no shards to own tablets, or if there are more shards
    /// than a tablet can name.
    ///
    /// # Examples
    ///
    /// ```
    /// use shoal_core::server::ring::Ring;
    ///
    /// // every tablet is owned as soon as the map exists
    /// let ring = Ring::new(4)?;
    /// // so routing any key at all names a live shard
    /// ring.find_shard(1234);
    /// # Ok::<(), shoal_core::server::ServerError>(())
    /// ```
    pub fn new(shard_count: usize) -> Result<Self, ServerError> {
        // a node with no shards has nothing that could own a tablet
        if shard_count == 0 {
            return Err(ServerError::Shoal(ShoalError::NoShards));
        }
        // a tablet names its owner in a u16, so that is the most shards we can place
        if shard_count > usize::from(u16::MAX) {
            return Err(ServerError::Shoal(ShoalError::TooManyShards {
                shards: shard_count,
            }));
        }
        // build the info for every shard on this node, in shard id order
        let shards = (0..shard_count).map(ShardInfo::new).collect();
        // hand each tablet to a shard in turn, so the counts differ by at most one
        let tablets = (0..TABLET_COUNT)
            .map(|tablet| {
                // truncation cannot happen here since we rejected a larger shard count above
                #[allow(clippy::cast_possible_truncation)]
                let owner = (tablet % shard_count) as u16;
                owner
            })
            .collect();
        Ok(Ring { tablets, shards })
    }

    /// Build the tablet map a standalone node routes with from its hosting
    ///
    /// One info per executor, and every tablet owned by the executor the hosting deals it to
    /// ([F47](../../../docs/src/features/local-rehome.md)). The identity hosting builds exactly
    /// what [`Ring::new`] builds, so a node that never changed its core count routes as it
    /// always did.
    ///
    /// # Arguments
    ///
    /// * `hosting` - Which executor owns each tablet
    ///
    /// # Errors
    ///
    /// Fails as [`Ring::new`] does.
    pub fn from_hosting(hosting: &Hosting) -> Result<Self, ServerError> {
        // the executors, checked as a shard count is
        let mut ring = Ring::new(hosting.physical)?;
        // and the owners the hosting says
        for (tablet, owner) in ring.tablets.iter_mut().enumerate() {
            // truncation cannot happen: the hosting was checked against the executor count
            #[allow(clippy::cast_possible_truncation)]
            let host = hosting.owner_of_tablet(tablet) as u16;
            *owner = host;
        }
        Ok(ring)
    }

    /// Build the tablet map for a node with a static placement of tablets over nodes
    ///
    /// Tablet `t` belongs to `placement[t % N]`, and on that node to slot `(t / N) % slots`
    /// ([F38](../../../docs/src/features/inter-node-transport.md)). This node's own executors
    /// come first in `shards`, at the indices they have on the mesh, and every remote slot
    /// follows with a `Remote` contact; the map indexes the whole list. A tablet of this node's
    /// is owned by the executor its slot hosts on ([F47](../../../docs/src/features/local-rehome.md)),
    /// so with the identity hosting a placement of one node builds exactly what [`Ring::new`]
    /// builds, which is what keeps a one node cluster's routing byte for byte what a standalone
    /// node's is.
    ///
    /// # Arguments
    ///
    /// * `hosting` - This node's slots, executors, and which executor hosts each slot
    /// * `placement` - Every placed node with its slot count, in placement order, this one included
    /// * `me` - This node's identity
    ///
    /// # Errors
    ///
    /// Fails as [`Ring::new`] does, and when the placement does not name this node with the
    /// slot count it claimed.
    pub fn with_placement(
        hosting: &Hosting,
        placement: &[(NodeId, u16)],
        me: NodeId,
    ) -> Result<Self, ServerError> {
        // this node's own executors are the local half of the answer, and the check on them
        let mut ring = Ring::new(hosting.physical)?;
        // the placement has to be one this node can route against: it names this node, once,
        // with the slots it claimed
        let Some((_, placed_shards)) = placement.iter().find(|(node, _)| *node == me) else {
            return Err(ServerError::Shoal(ShoalError::PlacementMissingSelf {
                node: me,
            }));
        };
        if usize::from(*placed_shards) != hosting.slots {
            return Err(ServerError::Shoal(ShoalError::PlacementShardCount {
                node: me,
                entry: *placed_shards,
                actual: hosting.slots,
            }));
        }
        // hand every tablet to its slot, this node's through the executors hosting them
        Self::assign(ring, hosting, placement, me)
    }

    /// Build the tablet map for a member of the cluster that the placement does not name
    ///
    /// The same rule as [`Ring::with_placement`], over the same list, so a tablet is sent to
    /// exactly the slot a placed node would send it to; this node's executors are still at the
    /// front of `shards` but own nothing, and every tablet is a `Remote` contact. It is what
    /// lets a member admitted after the placement was initialized, and not yet brought in by a
    /// move, coordinate every query it is sent instead of refusing it
    /// ([Resolved #169](../../../docs/src/appendix/resolved/unplaced-member-forwards.md)).
    ///
    /// # Arguments
    ///
    /// * `hosting` - This node's slots, executors, and which executor hosts each slot
    /// * `placement` - Every placed node with its slot count, in placement order
    /// * `me` - This node's identity
    ///
    /// # Errors
    ///
    /// Fails as [`Ring::new`] does, when the placement is empty, and when it names this node,
    /// which is [`Ring::with_placement`]'s to build.
    pub fn coordinator(
        hosting: &Hosting,
        placement: &[(NodeId, u16)],
        me: NodeId,
    ) -> Result<Self, ServerError> {
        // this node's executors are still the local half, and coordinate what it is sent
        let ring = Ring::new(hosting.physical)?;
        // a placement naming this node is routed by the placed ring, never this one
        if placement.iter().any(|(node, _)| *node == me) {
            return Err(ServerError::Shoal(ShoalError::PlacementNamesSelf {
                node: me,
            }));
        }
        // and one naming nobody has no tablet to send anywhere
        if placement.is_empty() {
            return Err(ServerError::Shoal(ShoalError::NoShards));
        }
        // hand every tablet to its remote slot
        Self::assign(ring, hosting, placement, me)
    }

    /// Append every remote slot of a placement and hand each tablet to its slot
    ///
    /// The one assignment [`Ring::with_placement`] and [`Ring::coordinator`] share, so a
    /// coordinating member and a placed one cannot disagree about which slot owns a tablet.
    ///
    /// # Arguments
    ///
    /// * `ring` - The ring holding this node's executors and nothing else yet
    /// * `hosting` - This node's slots, executors, and which executor hosts each slot
    /// * `placement` - Every node tablets are placed over, with its slot count
    /// * `me` - This node's identity, whose slots are hosted by its executors
    fn assign(
        mut ring: Ring,
        hosting: &Hosting,
        placement: &[(NodeId, u16)],
        me: NodeId,
    ) -> Result<Self, ServerError> {
        // append an info for every remote slot, remembering where each node's run starts
        let mut first_index = Vec::with_capacity(placement.len());
        for (node, shards) in placement {
            if *node == me {
                // this node's executors are already at 0..physical
                first_index.push(0usize);
                continue;
            }
            if *shards == 0 {
                return Err(ServerError::Shoal(ShoalError::NoShards));
            }
            first_index.push(ring.shards.len());
            for shard in 0..*shards {
                ring.shards.push(ShardInfo {
                    name: format!("Node-{node}/Shard-{shard}"),
                    contact: ShardContact::Remote { node: *node, shard },
                });
            }
        }
        // the whole list has to be addressable by a u16, as `new` requires of the local one
        if ring.shards.len() > usize::from(u16::MAX) {
            return Err(ServerError::Shoal(ShoalError::TooManyShards {
                shards: ring.shards.len(),
            }));
        }
        // hand each tablet to a node in turn, and within a node spread its tablets over its
        // shards by the next digit up, so a node and a shard are chosen independently: with the
        // shard taken from the same modulus as the node, a node whose shard count shared a
        // factor with the node count would leave some of its shards owning nothing
        let nodes = placement.len();
        for (tablet, owner) in ring.tablets.iter_mut().enumerate() {
            let which = tablet % nodes;
            let shard = (tablet / nodes) % usize::from(placement[which].1);
            // this node's slot is hosted by an executor; a remote slot is its own contact
            //
            // truncation cannot happen: the list was bounded above, and a slot by its hosting
            #[allow(clippy::cast_possible_truncation)]
            let index = if placement[which].0 == me {
                hosting.host_of_slot(shard as u16) as u16
            } else {
                (first_index[which] + shard) as u16
            };
            *owner = index;
        }
        Ok(ring)
    }

    /// The node and shard a tablet belongs to under a placement, without building a map
    ///
    /// The one rule [`Ring::with_placement`] applies, exposed so that a benchmark choosing keys
    /// for a particular shard of a particular node cannot drift from the map the server routes
    /// with. Returns the node's position in the placement and the shard on it.
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet, as [`Ring::tablet_of`] names it
    /// * `shards_per_node` - How many shards each node of the placement runs, in placement order
    #[must_use]
    pub fn owner_of(tablet: usize, shards_per_node: &[u16]) -> (usize, u16) {
        let nodes = shards_per_node.len();
        let which = tablet % nodes;
        // the next digit up chooses the shard, so a node and a shard are chosen independently
        //
        // truncation cannot happen: the modulus is a u16
        #[allow(clippy::cast_possible_truncation)]
        let shard = ((tablet / nodes) % usize::from(shards_per_node[which])) as u16;
        (which, shard)
    }

    /// Hand a tablet to a shard by its index in `shards`
    ///
    /// For the read ring, which points a tablet this node holds a replica of at the local
    /// shard holding it ([F40](../../../docs/src/features/replication.md)).
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    /// * `owner` - The shard's index in `shards`
    pub fn set_owner(&mut self, tablet: usize, owner: u16) {
        if let Some(slot) = self.tablets.get_mut(tablet) {
            *slot = owner;
        }
    }

    /// The owner of a tablet, as an index into `shards`
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn owner(&self, tablet: usize) -> u16 {
        self.tablets[tablet]
    }

    /// The index in `shards` of a contact, if the ring knows it
    ///
    /// For the read ring, which points a tablet this node holds no copy of at a holder that
    /// is up rather than at a primary that is down
    /// ([F42](../../../docs/src/features/primary-failover.md)).
    ///
    /// # Arguments
    ///
    /// * `contact` - The shard to find
    #[must_use]
    pub fn index_of(&self, contact: &ShardContact) -> Option<u16> {
        self.shards
            .iter()
            .position(|info| info.contact == *contact)
            .and_then(|index| u16::try_from(index).ok())
    }

    /// Get the tablet a partition key belongs to
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition to find the tablet for
    pub fn tablet_of(partition: u64) -> usize {
        // take the high bits of the key, which is what leaves room for a later split
        //
        // this cannot exceed TABLET_COUNT, since we keep only TABLET_BITS of the key
        #[allow(clippy::cast_possible_truncation)]
        let tablet = (partition >> (u64::BITS - TABLET_BITS)) as usize;
        tablet
    }

    /// Get a shard for this partition
    ///
    /// # Arguments
    ///
    /// * `partition` - The partition to look for
    pub fn find_shard(&self, partition: u64) -> &ShardInfo {
        // find the tablet holding this partition
        let tablet = Self::tablet_of(partition);
        // get the shard that owns that tablet
        //
        // neither index can be out of bounds: `new` fills a tablet for every id this can
        // produce and rejects an empty shard list, and `with_placement` appends every remote
        // shard before it names one as an owner
        let owner = usize::from(self.tablets[tablet]);
        &self.shards[owner]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A node with no shards has nothing that could own a tablet
    ///
    /// This is the state that used to panic when it was routed against. It is now
    /// unbuildable, which is why nothing below has to test routing against it.
    #[test]
    fn empty_ring_is_not_constructable() {
        // a map with no shards to own tablets is not a map we can build
        assert!(Ring::new(0).is_err());
    }

    /// Routing must answer for every key, including both ends of the space
    #[test]
    fn find_shard_handles_the_whole_key_space() {
        // build a map for a typical deployment
        let ring = Ring::new(16).expect("failed to build a ring");
        // both ends of the key space have to name a shard
        ring.find_shard(0);
        ring.find_shard(u64::MAX);
        // and so does everything in between
        for sample in 0..10_000u64 {
            // spread our samples over the whole key space
            ring.find_shard(sample.wrapping_mul(u64::MAX / 10_000));
        }
    }

    /// Tablets have to be split as evenly as the shard count allows
    ///
    /// This is what the vnode ring failed to do: its busiest shard owned 3.54x the mean.
    #[test]
    fn tablets_are_evenly_owned() {
        // check the shard counts a node is plausibly configured with
        for shard_count in [1, 2, 3, 7, 16, 31, 64] {
            // build the map for this many shards
            let ring = Ring::new(shard_count).expect("failed to build a ring");
            // count the tablets each shard was given
            let mut owned = vec![0usize; shard_count];
            // walk every tablet in the map
            for owner in &ring.tablets {
                // record that this shard owns another tablet
                owned[usize::from(*owner)] += 1;
            }
            // find the largest and smallest share handed out
            let max = *owned.iter().max().expect("no shards");
            let min = *owned.iter().min().expect("no shards");
            // an even split can only differ by the remainder, which is at most one tablet
            assert!(
                max - min <= 1,
                "{shard_count} shards own between {min} and {max} tablets",
            );
        }
    }

    /// No shard may be left without tablets to own
    #[test]
    fn every_shard_owns_a_tablet() {
        // check every shard count up to more than a large machine has
        for shard_count in 1..=64 {
            // build the map for this many shards
            let ring = Ring::new(shard_count).expect("failed to build a ring");
            // every shard has to appear somewhere in the map
            for id in 0..shard_count {
                // truncation cannot happen for a shard count this small
                #[allow(clippy::cast_possible_truncation)]
                let id = id as u16;
                // look for a tablet this shard was given
                assert!(
                    ring.tablets.contains(&id),
                    "shard {id} of {shard_count} owns no tablets",
                );
            }
        }
    }

    /// Two shards must agree about who owns a key without having to coordinate
    ///
    /// The ring this replaced stored an arrival order index, so two shards genuinely held
    /// different maps and agreeing needed an argument. Building from the shard count means
    /// the maps are identical instead.
    #[test]
    fn find_shard_is_stable_across_rings() {
        // build two maps the way two different shards would
        let left = Ring::new(16).expect("failed to build a ring");
        let right = Ring::new(16).expect("failed to build a ring");
        // both have to route every key the same way
        for sample in 0..10_000u64 {
            // spread our samples over the whole key space
            let key = sample.wrapping_mul(u64::MAX / 10_000);
            // the two maps have to name the same shard for it
            assert_eq!(
                left.find_shard(key).contact,
                right.find_shard(key).contact,
                "two rings disagree about who owns {key}",
            );
        }
    }

    /// A ring from the identity hosting is the ring of old, and one from a dealt hosting
    /// routes every tablet to the executor the table names
    #[test]
    fn a_ring_from_hosting_routes_by_the_table() {
        for n in [1usize, 3, 8] {
            let hosted = Ring::from_hosting(&Hosting::identity(n)).expect("a ring");
            let plain = Ring::new(n).expect("a ring");
            assert_eq!(hosted.tablets, plain.tablets);
            assert_eq!(hosted.shards.len(), plain.shards.len());
        }
        // four executors dealt down to three: every tablet goes where the hosting says
        let hosting = Hosting::identity(4).plan(3, false).expect("a plan");
        let ring = Ring::from_hosting(&hosting).expect("a ring");
        assert_eq!(ring.shards.len(), 3);
        for tablet in 0..TABLET_COUNT {
            let key = (tablet as u64) << (u64::BITS - TABLET_BITS);
            assert_eq!(
                ring.find_shard(key).contact,
                ShardContact::Local(hosting.owner_of_tablet(tablet)),
                "tablet {tablet}"
            );
        }
        // and nothing is routed to an executor that does not exist
        assert!(ring.tablets.iter().all(|owner| usize::from(*owner) < 3));
    }

    /// A placement of one node is the standalone map, tablet for tablet
    #[test]
    fn a_one_node_placement_is_the_standalone_ring() {
        let me = NodeId::mint();
        for shard_count in [1, 2, 7, 16] {
            let placement = vec![(me, shard_count)];
            let placed =
                Ring::with_placement(&Hosting::identity(usize::from(shard_count)), &placement, me)
                    .expect("a placement of one");
            let alone = Ring::new(usize::from(shard_count)).expect("a ring");
            assert_eq!(placed.tablets, alone.tablets);
            assert_eq!(placed.shards.len(), alone.shards.len());
            for (a, b) in placed.shards.iter().zip(&alone.shards) {
                assert_eq!(a.contact, b.contact);
            }
        }
    }

    /// A placement of several nodes hands tablets to nodes in turn, then to shards in turn
    #[test]
    fn a_placement_interleaves_nodes_then_shards() {
        let ids = [NodeId::mint(), NodeId::mint(), NodeId::mint()];
        let shards = [2u16, 3, 1];
        let placement: Vec<(NodeId, u16)> = ids.iter().copied().zip(shards).collect();
        // seen from the second node, which runs three shards
        let ring = Ring::with_placement(&Hosting::identity(3), &placement, ids[1])
            .expect("a placement of three");
        assert_eq!(ring.shards.len(), 6);
        for tablet in 0..TABLET_COUNT {
            let (which, shard) = Ring::owner_of(tablet, &shards);
            let info = &ring.shards[usize::from(ring.tablets[tablet])];
            let expected = if which == 1 {
                ShardContact::Local(usize::from(shard))
            } else {
                ShardContact::Remote {
                    node: ids[which],
                    shard,
                }
            };
            assert_eq!(info.contact, expected, "tablet {tablet}");
        }
        // every remote shard owns something, and so does every local one
        for info in &ring.shards {
            assert!(
                ring.tablets
                    .iter()
                    .any(|owner| ring.shards[usize::from(*owner)].contact == info.contact),
                "{} owns no tablets",
                info.name
            );
        }
        // a node the placement does not name cannot route against it
        assert!(Ring::with_placement(&Hosting::identity(3), &placement, NodeId::mint()).is_err());
        // and neither can one with the wrong shard count
        assert!(Ring::with_placement(&Hosting::identity(2), &placement, ids[1]).is_err());
    }

    /// A member the placement does not name routes every tablet to the slot the rule gives it
    ///
    /// Its own executors come first and own nothing; every tablet goes to the remote slot
    /// `owner_of` names, which is the slot every placed node sends it to. A placement naming
    /// this node, or naming nobody, is refused
    /// ([Resolved #169](../../../docs/src/appendix/resolved/unplaced-member-forwards.md)).
    #[test]
    fn a_coordinator_routes_every_tablet_to_its_placed_slot() {
        let ids = [NodeId::mint(), NodeId::mint(), NodeId::mint()];
        let shards = [2u16, 3, 1];
        let placement: Vec<(NodeId, u16)> = ids.iter().copied().zip(shards).collect();
        let me = NodeId::mint();
        let ring = Ring::coordinator(&Hosting::identity(4), &placement, me)
            .expect("a coordinator over three");
        // four local executors, then every placed slot
        assert_eq!(ring.shards.len(), 4 + 6);
        // and the second placed node's view, which the coordinator has to agree with
        let placed = Ring::with_placement(&Hosting::identity(3), &placement, ids[1])
            .expect("a placement of three");
        for tablet in 0..TABLET_COUNT {
            let (which, shard) = Ring::owner_of(tablet, &shards);
            let info = &ring.shards[usize::from(ring.tablets[tablet])];
            assert_eq!(
                info.contact,
                ShardContact::Remote {
                    node: ids[which],
                    shard,
                },
                "tablet {tablet}"
            );
            // the placed node agrees wherever the slot is not its own
            if which != 1 {
                let seen = &placed.shards[usize::from(placed.tablets[tablet])];
                assert_eq!(seen.contact, info.contact, "tablet {tablet}");
            }
        }
        // no local executor owns anything
        assert!(ring.tablets.iter().all(|owner| usize::from(*owner) >= 4));
        // a placement naming this node is the placed ring's, and one naming nobody is nothing
        assert!(Ring::coordinator(&Hosting::identity(3), &placement, ids[1]).is_err());
        assert!(Ring::coordinator(&Hosting::identity(3), &[], me).is_err());
    }

    /// A placement hosts this node's slots on its executors and leaves every remote slot alone
    ///
    /// Four slots on two executors: the placement still names four, a remote peer's slots are
    /// still four contacts, and every local tablet is owned by the executor hosting its slot.
    #[test]
    fn a_placement_hosts_slots_on_executors() {
        let ids = [NodeId::mint(), NodeId::mint()];
        let placement: Vec<(NodeId, u16)> = vec![(ids[0], 4), (ids[1], 3)];
        let hosting = Hosting::identity(4).plan(2, true).expect("a plan");
        let ring = Ring::with_placement(&hosting, &placement, ids[0]).expect("a placement");
        // two local executors and three remote slots
        assert_eq!(ring.shards.len(), 5);
        assert_eq!(
            ring.shards
                .iter()
                .filter(|info| info.contact.local_index().is_some())
                .count(),
            2
        );
        for tablet in 0..TABLET_COUNT {
            let (which, shard) = Ring::owner_of(tablet, &[4, 3]);
            let info = &ring.shards[usize::from(ring.tablets[tablet])];
            let expected = if which == 0 {
                ShardContact::Local(hosting.host_of_slot(shard))
            } else {
                ShardContact::Remote {
                    node: ids[1],
                    shard,
                }
            };
            assert_eq!(info.contact, expected, "tablet {tablet}");
        }
        // both executors own tablets, and every remote slot is still named
        for info in &ring.shards {
            assert!(
                ring.tablets
                    .iter()
                    .any(|owner| ring.shards[usize::from(*owner)].contact == info.contact),
                "{} owns no tablets",
                info.name
            );
        }
        // a placement naming the executors rather than the slots is refused
        assert!(Ring::with_placement(&hosting, &[(ids[0], 2), (ids[1], 3)], ids[0]).is_err());
    }

    /// A tablet id has to come from the top of the key, so a split stays incremental
    ///
    /// Splitting a tablet means consuming one more bit. That only divides the tablet
    /// itself if the bits below the id are the ones that vary within it.
    #[test]
    fn tablet_id_comes_from_the_high_bits() {
        // take a key at the base of some tablet
        let base = 0x1234_0000_0000_0000u64;
        // every key sharing its high bits has to share its tablet
        for low in 0..1000u64 {
            // vary only the bits below the tablet id
            assert_eq!(
                Ring::tablet_of(base),
                Ring::tablet_of(base | low),
                "a low bit changed which tablet {base} belongs to",
            );
        }
        // and changing the high bits has to leave the tablet behind
        assert_ne!(Ring::tablet_of(base), Ring::tablet_of(base << 4));
    }
}
