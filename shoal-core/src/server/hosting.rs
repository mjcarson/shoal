//! Which executor hosts what on this node
//!
//! Before [F47](../../../docs/src/features/local-rehome.md) a node ran one shard per core and
//! the shard was the unit everything was keyed by: the directory a table's files sat under,
//! the modulus of the placement rule, the shard in every address a peer recorded. Changing the
//! core count changed all of those at once, which is why `ShardCountMismatch` refused it.
//!
//! This splits the unit in two. A node runs `physical` *executors*, one per core, and each
//! executor hosts one or more *slots*. On a cluster node the slots are what every peer knows:
//! the `shards` in the member record, the shard in every `ShardAddr`, the modulus of the rule,
//! and so the identity of every replica set the node is in. They are claimed once and never
//! change. Which executor hosts a slot is this node's own business, recorded here and read by
//! nobody else, so the executors can be fewer or more than the slots without a peer noticing.
//! A standalone node has no peers and so no slots to keep still; it hosts *per tablet*, which
//! gives an exact balance at any executor count and no ceiling on growth.
//!
//! The table lives in `shoal-hosting.json` beside the marker, written with the marker's
//! atomic pattern, and a directory that has none is hosted as the identity: slot `n` and tablet
//! `t % n` on executor `n`, which is byte for byte what [`super::ring::Ring::new`] built before
//! this existed. Moving from one table to another is the rehome
//! ([`super::rehome`]), and the plan it follows is computed here, deterministically, so that
//! a resumed rehome plans the same moves the crashed one did.

use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use super::ring::TABLET_COUNT;
use super::ServerError;

/// The version of the hosting file's own format
pub const HOSTING_FORMAT: u32 = 1;

/// The name of the hosting file within a storage directory
pub const HOSTING_FILE: &str = "shoal-hosting.json";

/// The name the hosting file is staged under before it is renamed into place
const HOSTING_TEMP_FILE: &str = "shoal-hosting.json.tmp";

/// Which executor hosts each slot and each tablet on this node
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hosting {
    /// The version of this file's own format
    pub format: u32,
    /// How many slots this node has: the shard count every peer records for it
    pub slots: usize,
    /// How many executors this node runs: one per core
    pub physical: usize,
    /// The executor hosting each slot, indexed by slot
    ///
    /// What a cluster node dispatches by: a frame naming slot `s` reaches `hosts[s]`.
    pub hosts: Vec<u16>,
    /// The executor owning each tablet, indexed by tablet
    ///
    /// What a standalone node routes by. On a cluster node this is derived from `hosts`
    /// through the one node rule and read by nothing, since the map decides what this node
    /// holds.
    pub tablets: Vec<u16>,
}

impl Hosting {
    /// The hosting a directory with no file is under: every slot on the executor of its number
    ///
    /// # Arguments
    ///
    /// * `n` - The slot and executor count
    #[must_use]
    pub fn identity(n: usize) -> Self {
        // slot n on executor n, tablet t on executor t % n, exactly as the ring always did
        //
        // truncation cannot happen: a node runs fewer executors than a u16 holds
        #[allow(clippy::cast_possible_truncation)]
        let hosts = (0..n).map(|slot| slot as u16).collect();
        #[allow(clippy::cast_possible_truncation)]
        let tablets = (0..TABLET_COUNT)
            .map(|tablet| (tablet % n.max(1)) as u16)
            .collect();
        Hosting {
            format: HOSTING_FORMAT,
            slots: n,
            physical: n,
            hosts,
            tablets,
        }
    }

    /// Get the path to the hosting file within a storage directory
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    #[must_use]
    pub fn path(root: &Path) -> PathBuf {
        root.join(HOSTING_FILE)
    }

    /// Read the hosting a directory carries, if it carries one
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be read or parsed, or names a format this build does not read.
    pub fn read(root: &Path) -> Result<Option<Self>, ServerError> {
        // read whatever hosting this directory already carries
        let raw = match std::fs::read(Self::path(root)) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ServerError::IO(error)),
        };
        let found: Hosting = serde_json::from_slice(&raw)?;
        // the format is checked before anything in the file is trusted
        if found.format != HOSTING_FORMAT {
            return Err(ServerError::IO(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "the hosting file is format {} and this build reads format {HOSTING_FORMAT}",
                    found.format
                ),
            )));
        }
        // and the table has to describe itself: a slot and a tablet for every index, each on
        // an executor the node runs
        found.check()?;
        Ok(Some(found))
    }

    /// Read the hosting a directory carries, or the identity for a count if it carries none
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    /// * `n` - The slot and executor count a directory with no file is under
    ///
    /// # Errors
    ///
    /// Fails as [`Hosting::read`] does.
    pub fn read_or_identity(root: &Path, n: usize) -> Result<Self, ServerError> {
        Ok(Self::read(root)?.unwrap_or_else(|| Self::identity(n)))
    }

    /// Write the hosting into a directory so that a crash leaves the old one or the new one
    ///
    /// Staged, synced, renamed, and the directory synced: the marker's pattern.
    ///
    /// # Arguments
    ///
    /// * `root` - The root of the storage directory
    ///
    /// # Errors
    ///
    /// Fails if the file cannot be written.
    pub fn write(&self, root: &Path) -> Result<(), ServerError> {
        // make sure the directory we are writing into exists
        std::fs::create_dir_all(root)?;
        // stage the new table beside the old one
        let staged = root.join(HOSTING_TEMP_FILE);
        let mut file = File::create(&staged)?;
        file.write_all(&serde_json::to_vec_pretty(self)?)?;
        file.sync_all()?;
        drop(file);
        // and swap it in, which is the atomic step
        std::fs::rename(&staged, Self::path(root))?;
        // the rename is only durable once the directory entry is
        File::open(root)?.sync_all()?;
        Ok(())
    }

    /// Check the table describes a node: every slot and tablet on an executor the node runs
    fn check(&self) -> Result<(), ServerError> {
        // every refusal is the same kind of error, naming what is wrong
        let invalid = |what: String| {
            ServerError::IO(std::io::Error::new(std::io::ErrorKind::InvalidData, what))
        };
        // a node with no executors or no slots hosts nothing
        if self.physical == 0 || self.slots == 0 {
            return Err(invalid(
                "the hosting names no executors or no slots".to_string(),
            ));
        }
        // one host per slot and one owner per tablet
        if self.hosts.len() != self.slots {
            return Err(invalid(format!(
                "the hosting names {} slots and hosts {}",
                self.slots,
                self.hosts.len()
            )));
        }
        if self.tablets.len() != TABLET_COUNT {
            return Err(invalid(format!(
                "the hosting owns {} tablets and there are {TABLET_COUNT}",
                self.tablets.len()
            )));
        }
        // each on an executor the node runs
        if let Some(host) = self
            .hosts
            .iter()
            .find(|host| usize::from(**host) >= self.physical)
        {
            return Err(invalid(format!(
                "the hosting puts a slot on executor {host} and the node runs {}",
                self.physical
            )));
        }
        if let Some(owner) = self
            .tablets
            .iter()
            .find(|owner| usize::from(**owner) >= self.physical)
        {
            return Err(invalid(format!(
                "the hosting puts a tablet on executor {owner} and the node runs {}",
                self.physical
            )));
        }
        Ok(())
    }

    /// The executor hosting a slot
    ///
    /// A slot past the count is answered with executor zero rather than a panic; the listener
    /// bounds a frame's slot before it dispatches, so this is never reached with one.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot
    #[must_use]
    pub fn host_of_slot(&self, slot: u16) -> usize {
        // the executor the table names, or zero for a slot past the count
        self.hosts
            .get(usize::from(slot))
            .map_or(0, |host| usize::from(*host))
    }

    /// The executor owning a tablet, on a standalone node
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    #[must_use]
    pub fn owner_of_tablet(&self, tablet: usize) -> usize {
        // the executor the table names, or zero for a tablet past the count
        self.tablets
            .get(tablet)
            .map_or(0, |owner| usize::from(*owner))
    }

    /// The slots each executor hosts, indexed by executor
    #[must_use]
    pub fn slots_by_executor(&self) -> Vec<Vec<u16>> {
        // one list per executor, filled in slot order
        let mut by = vec![Vec::new(); self.physical];
        for (slot, host) in self.hosts.iter().enumerate() {
            // truncation cannot happen: a slot is bounded by the u16 that names it
            #[allow(clippy::cast_possible_truncation)]
            by[usize::from(*host)].push(slot as u16);
        }
        by
    }

    /// How many tablets each executor owns, indexed by executor
    #[must_use]
    pub fn tablets_per_executor(&self) -> Vec<usize> {
        // one count per executor
        let mut counts = vec![0usize; self.physical];
        for owner in &self.tablets {
            counts[usize::from(*owner)] += 1;
        }
        counts
    }

    /// Whether this table is the identity: slot `n` on executor `n` and the ring's tablets
    #[must_use]
    pub fn is_identity(&self) -> bool {
        // the same table the ring would build for this many executors
        *self == Self::identity(self.slots) && self.physical == self.slots
    }

    /// Plan the hosting for another executor count
    ///
    /// A standalone node deals tablets, a cluster node deals slots, and both the same way: a
    /// shrink deals what the vanishing executors held, one item at a time, to the least loaded
    /// surviving executor by count; a growth takes one item at a time from the most loaded
    /// executor and gives it to the least loaded until the counts differ by at most one. Ties
    /// go to the lowest executor, so the plan is a function of the table and the count and a
    /// resumed rehome plans exactly what the crashed one planned. On a cluster node the
    /// tablets are derived from the slots afterwards, so both halves of the table agree.
    ///
    /// # Arguments
    ///
    /// * `to` - The executor count to host on
    /// * `cluster` - Whether this node deals slots (a cluster node) or tablets (standalone)
    ///
    /// # Errors
    ///
    /// Refuses a count of zero, and a cluster count past the slots, since an executor with no
    /// slot to host would own nothing.
    pub fn plan(&self, to: usize, cluster: bool) -> Result<Self, ServerError> {
        // an executor count of zero hosts nothing
        if to == 0 {
            return Err(ServerError::Shoal(super::errors::ShoalError::NoShards));
        }
        // a cluster node cannot run more executors than it has slots to give them
        if cluster && to > self.slots {
            return Err(ServerError::Shoal(
                super::errors::ShoalError::CoresExceedSlots {
                    cores: to,
                    slots: self.slots,
                },
            ));
        }
        let mut after = self.clone();
        after.physical = to;
        if cluster {
            // deal the slots, then derive the tablets from them
            after.hosts = deal(&self.hosts, self.physical, to);
            // truncation cannot happen: the hosts are u16s already
            after.tablets = (0..TABLET_COUNT)
                .map(|tablet| after.hosts[tablet % self.slots])
                .collect();
        } else {
            // deal the tablets; the slots are the executors on a standalone node, so the
            // hosts are the identity of the new count
            after.tablets = deal(&self.tablets, self.physical, to);
            // truncation cannot happen: a node runs fewer executors than a u16 holds
            #[allow(clippy::cast_possible_truncation)]
            let hosts = (0..to).map(|slot| slot as u16).collect();
            after.hosts = hosts;
            after.slots = to;
        }
        after.check()?;
        Ok(after)
    }

    /// The items whose executor differs between this table and another, as (item, from, to)
    ///
    /// Slots on a cluster node, tablets on a standalone one; what the rehome moves.
    ///
    /// # Arguments
    ///
    /// * `after` - The table to move to
    /// * `cluster` - Whether to compare slots or tablets
    #[must_use]
    pub fn moves_to(&self, after: &Hosting, cluster: bool) -> Vec<(u16, u16, u16)> {
        // the half of the table this kind of node deals
        let (before, later) = if cluster {
            (&self.hosts, &after.hosts)
        } else {
            (&self.tablets, &after.tablets)
        };
        // every item whose executor differs, with where it was and where it goes
        before
            .iter()
            .zip(later)
            .enumerate()
            .filter(|(_, (from, to))| from != to)
            // truncation cannot happen: an item index is a slot or a twelve bit tablet
            .map(|(item, (from, to))| (u16::try_from(item).unwrap_or(u16::MAX), *from, *to))
            .collect()
    }
}

/// Deal items from one executor count to another, by count, deterministically
///
/// # Arguments
///
/// * `owners` - The executor holding each item
/// * `from` - The executor count the items are held on
/// * `to` - The executor count to deal onto
fn deal(owners: &[u16], from: usize, to: usize) -> Vec<u16> {
    // start from where everything is
    let mut after = owners.to_vec();
    // how many items each surviving executor holds
    let mut counts = vec![0usize; to];
    for owner in &after {
        if usize::from(*owner) < to {
            counts[usize::from(*owner)] += 1;
        }
    }
    // the least loaded executor, lowest id on a tie
    let least = |counts: &[usize]| -> usize {
        counts
            .iter()
            .enumerate()
            .min_by_key(|(id, count)| (**count, *id))
            .map_or(0, |(id, _)| id)
    };
    if to < from {
        // a shrink: every item on a vanished executor goes to the least loaded survivor, in
        // item order so the deal is a function of the table alone
        for owner in &mut after {
            if usize::from(*owner) >= to {
                let target = least(&counts);
                counts[target] += 1;
                // truncation cannot happen: the count was bounded by a u16 above
                #[allow(clippy::cast_possible_truncation)]
                let target = target as u16;
                *owner = target;
            }
        }
    } else {
        // a growth: take from the most loaded and give to the least until they differ by at
        // most one, taking the highest numbered item of the donor each time
        loop {
            let (most, most_count) = counts
                .iter()
                .enumerate()
                .max_by_key(|(id, count)| (**count, std::cmp::Reverse(*id)))
                .map_or((0, 0), |(id, count)| (id, *count));
            let target = least(&counts);
            if most_count <= counts[target] + 1 {
                break;
            }
            // the last item the donor holds, so the moved set is a suffix of its items
            let Some(item) = after.iter().rposition(|owner| usize::from(*owner) == most) else {
                break;
            };
            counts[most] -= 1;
            counts[target] += 1;
            // truncation cannot happen: the count was bounded by a u16 above
            #[allow(clippy::cast_possible_truncation)]
            let target = target as u16;
            after[item] = target;
        }
    }
    after
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The identity hosting is the ring's: slot n on executor n, tablet t on executor t % n
    #[test]
    fn the_identity_hosting_is_the_ring() {
        for n in [1usize, 2, 3, 7, 16] {
            let hosting = Hosting::identity(n);
            assert!(hosting.is_identity());
            assert_eq!(hosting.physical, n);
            assert_eq!(hosting.slots, n);
            for slot in 0..n {
                assert_eq!(
                    hosting.host_of_slot(u16::try_from(slot).expect("a u16")),
                    slot
                );
            }
            for tablet in 0..TABLET_COUNT {
                assert_eq!(hosting.owner_of_tablet(tablet), tablet % n);
            }
        }
    }

    /// A shrink deals what vanished to the least loaded, a growth takes from the most loaded,
    /// per tablet standalone and per slot on a cluster node, deterministically, leaving every
    /// executor owning something
    #[test]
    fn hosting_deals_vanished_shards_to_the_least_loaded() {
        // standalone, per tablet: four down to three, then to one, then up to five
        let four = Hosting::identity(4);
        let three = four.plan(3, false).expect("a plan");
        assert_eq!(three.physical, 3);
        assert_eq!(
            three.slots, 3,
            "a standalone node's slots are its executors"
        );
        let counts = three.tablets_per_executor();
        assert_eq!(counts.iter().sum::<usize>(), TABLET_COUNT);
        let (max, min) = (
            *counts.iter().max().expect("counts"),
            *counts.iter().min().expect("counts"),
        );
        assert!(max - min <= 1, "{counts:?}");
        // a tablet that was not on the vanished executor did not move
        for tablet in 0..TABLET_COUNT {
            if four.owner_of_tablet(tablet) < 3 {
                assert_eq!(
                    three.owner_of_tablet(tablet),
                    four.owner_of_tablet(tablet),
                    "tablet {tablet} moved"
                );
            }
        }
        // the moves are the vanished executor's tablets and nothing else
        let moves = four.moves_to(&three, false);
        assert_eq!(moves.len(), TABLET_COUNT / 4);
        assert!(moves.iter().all(|(_, from, _)| *from == 3));
        // the same plan twice is the same plan
        assert_eq!(four.plan(3, false).expect("a plan"), three);
        // down to one: everything on executor zero
        let one = three.plan(1, false).expect("a plan");
        assert!(one.tablets.iter().all(|owner| *owner == 0));
        assert_eq!(one.tablets_per_executor(), vec![TABLET_COUNT]);
        // up to five from three: within one of even, and every executor owns something
        let five = three.plan(5, false).expect("a plan");
        let counts = five.tablets_per_executor();
        assert_eq!(counts.len(), 5);
        assert!(counts.iter().all(|count| *count > 0), "{counts:?}");
        let (max, min) = (
            *counts.iter().max().expect("counts"),
            *counts.iter().min().expect("counts"),
        );
        assert!(max - min <= 1, "{counts:?}");
        // only the donors' tablets moved, and only onto the new executors
        for (tablet, from, to) in three.moves_to(&five, false) {
            assert!(
                from < 3 && to >= 3,
                "a growth moved tablet {tablet} from {from} to {to}"
            );
        }
        // cluster, per slot: four slots on four executors down to two, and back up
        let four = Hosting::identity(4);
        let two = four.plan(2, true).expect("a plan");
        assert_eq!(two.slots, 4, "a cluster node's slots never move");
        assert_eq!(two.physical, 2);
        assert_eq!(two.hosts, vec![0, 1, 0, 1]);
        assert_eq!(two.slots_by_executor(), vec![vec![0, 2], vec![1, 3]]);
        // the tablets follow the slots through the one node rule
        for tablet in 0..TABLET_COUNT {
            assert_eq!(
                two.owner_of_tablet(tablet),
                two.host_of_slot(u16::try_from(tablet % 4).expect("a u16"))
            );
        }
        assert_eq!(four.moves_to(&two, true), vec![(2, 2, 0), (3, 3, 1)]);
        let back = two.plan(4, true).expect("a plan");
        assert_eq!(back.hosts.len(), 4);
        let by = back.slots_by_executor();
        assert!(by.iter().all(|slots| slots.len() == 1), "{by:?}");
        // a growth past the slots is refused by name, and a count of zero too
        assert!(matches!(
            four.plan(5, true),
            Err(ServerError::Shoal(
                super::super::errors::ShoalError::CoresExceedSlots { cores: 5, slots: 4 }
            ))
        ));
        assert!(four.plan(0, true).is_err());
        assert!(four.plan(0, false).is_err());
        // three slots on one executor grown to two: the highest slot moves, and the counts are
        // within one of each other, which is as even as three over two gets
        let one = Hosting::identity(3).plan(1, true).expect("a plan");
        assert_eq!(one.hosts, vec![0, 0, 0]);
        let two = one.plan(2, true).expect("a plan");
        assert_eq!(two.hosts, vec![0, 0, 1]);
    }

    /// The file round trips, a directory without one is the identity, and a torn table is refused
    #[test]
    fn a_hosting_file_round_trips() {
        let dir = tempfile::tempdir().expect("a temp dir");
        assert!(Hosting::read(dir.path()).expect("a read").is_none());
        assert_eq!(
            Hosting::read_or_identity(dir.path(), 3).expect("a read"),
            Hosting::identity(3)
        );
        let planned = Hosting::identity(4).plan(2, true).expect("a plan");
        planned.write(dir.path()).expect("a write");
        assert_eq!(
            Hosting::read(dir.path()).expect("a read"),
            Some(planned.clone())
        );
        assert!(!dir.path().join(HOSTING_TEMP_FILE).exists());
        // a table naming an executor the node does not run is refused
        let mut torn = planned;
        torn.hosts[0] = 9;
        std::fs::write(
            Hosting::path(dir.path()),
            serde_json::to_vec(&torn).expect("json"),
        )
        .expect("a write");
        assert!(Hosting::read(dir.path()).is_err());
    }
}
