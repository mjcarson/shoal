//! A volatile log for an ephemeral table's group
//!
//! An ephemeral table stores nothing, so its group's log lives in memory and every append is
//! as durable as it will ever be the moment it is made
//! ([C5](../../../../docs/src/distributed/replication.md), "volatile replicated table"). The
//! group still elects, replicates and commits like any other - the encoding, the quorum and the
//! apply are the same code - and what differs is written down on the artifact rather than
//! hidden: a full-cluster restart empties it. The bound is what keeps a group nobody drains from
//! taking the process with it; past it the shard sheds proposals rather than appending.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use openraft::entry::RaftEntry as _;
use openraft::storage::LogState;

use super::frame::{Entry, Vote, WalLogId};
use crate::server::replication::DataConfig;
use crate::shared::identity::GroupId;

/// One group's volatile log
#[derive(Default)]
struct MemGroup {
    /// Every entry, by index
    entries: BTreeMap<u64, Entry>,
    /// The last vote granted
    vote: Option<Vote>,
    /// The last committed log id recorded
    committed: Option<WalLogId>,
    /// The last purged log id recorded
    purged: Option<WalLogId>,
}

/// The state every handle shares
struct MemInner {
    /// Every group's log
    groups: HashMap<GroupId, MemGroup>,
    /// How many bytes of commands every group holds together
    bytes: usize,
}

/// The volatile logs of one shard's ephemeral groups
#[derive(Clone)]
pub struct MemoryWal {
    /// The shared state
    inner: Rc<RefCell<MemInner>>,
}

impl Default for MemoryWal {
    /// An empty set of logs
    fn default() -> Self {
        MemoryWal {
            inner: Rc::new(RefCell::new(MemInner {
                groups: HashMap::new(),
                bytes: 0,
            })),
        }
    }
}

impl MemoryWal {
    /// An empty set of logs
    #[must_use]
    pub fn new() -> Self {
        MemoryWal::default()
    }

    /// A store for one group over these logs
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    #[must_use]
    pub fn store(&self, group: GroupId) -> super::GroupStore {
        super::GroupStore {
            backend: super::Backend::Memory(self.clone()),
            group,
        }
    }

    /// How many bytes of commands every group holds together
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.inner.borrow().bytes
    }

    /// How many bytes an entry's command takes
    ///
    /// # Arguments
    ///
    /// * `entry` - The entry
    fn weight(entry: &Entry) -> usize {
        match &entry.payload {
            openraft::EntryPayload::Normal(command) => command.encoded_len(),
            _ => 0,
        }
    }

    /// The entries of a group in a range of indexes
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `start` - The first index
    /// * `end` - The last index, or none for everything from the start
    pub(super) fn entries(&self, group: GroupId, start: u64, end: Option<u64>) -> Vec<Entry> {
        let inner = self.inner.borrow();
        let Some(log) = inner.groups.get(&group) else {
            return Vec::new();
        };
        match end {
            Some(end) => log.entries.range(start..=end).map(|(_, entry)| entry.clone()).collect(),
            None => log.entries.range(start..).map(|(_, entry)| entry.clone()).collect(),
        }
    }

    /// The last vote a group granted
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    pub(super) fn vote(&self, group: GroupId) -> Option<Vote> {
        self.inner.borrow().groups.get(&group).and_then(|log| log.vote.clone())
    }

    /// Record a group's vote
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `vote` - The vote
    pub(super) fn save_vote(&self, group: GroupId, vote: Vote) {
        self.inner.borrow_mut().groups.entry(group).or_default().vote = Some(vote);
    }

    /// The committed log id a group last recorded
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    pub(super) fn committed(&self, group: GroupId) -> Option<WalLogId> {
        self.inner.borrow().groups.get(&group).and_then(|log| log.committed.clone())
    }

    /// Record a group's committed log id
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `committed` - The log id
    pub(super) fn save_committed(&self, group: GroupId, committed: Option<WalLogId>) {
        self.inner.borrow_mut().groups.entry(group).or_default().committed = committed;
    }

    /// Where a group's log begins and ends
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    pub(super) fn log_state(&self, group: GroupId) -> LogState<DataConfig> {
        let inner = self.inner.borrow();
        match inner.groups.get(&group) {
            Some(log) => LogState {
                last_purged_log_id: log.purged.clone(),
                last_log_id: log
                    .entries
                    .values()
                    .next_back()
                    .map(|entry| entry.log_id())
                    .or_else(|| log.purged.clone()),
            },
            None => LogState {
                last_purged_log_id: None,
                last_log_id: None,
            },
        }
    }

    /// Append entries to a group's log
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `entries` - The entries
    pub(super) fn append(&self, group: GroupId, entries: Vec<Entry>) {
        let mut inner = self.inner.borrow_mut();
        let mut added = 0;
        let mut removed = 0;
        {
            let log = inner.groups.entry(group).or_default();
            for entry in entries {
                added += Self::weight(&entry);
                if let Some(old) = log.entries.insert(entry.index(), entry) {
                    removed += Self::weight(&old);
                }
            }
        }
        inner.bytes = inner.bytes.saturating_sub(removed) + added;
    }

    /// Drop every entry of a group after an index
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `keep_after` - The last index kept, or none for nothing kept
    pub(super) fn truncate_after(&self, group: GroupId, keep_after: Option<u64>) {
        let mut inner = self.inner.borrow_mut();
        let mut removed = 0;
        if let Some(log) = inner.groups.get_mut(&group) {
            let dropped: Vec<u64> = log
                .entries
                .keys()
                .filter(|index| keep_after.is_none_or(|keep| **index > keep))
                .copied()
                .collect();
            for index in dropped {
                if let Some(entry) = log.entries.remove(&index) {
                    removed += Self::weight(&entry);
                }
            }
        }
        inner.bytes = inner.bytes.saturating_sub(removed);
    }

    /// Drop every entry of a group up to and including a log id
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `log_id` - The log id
    pub(super) fn purge(&self, group: GroupId, log_id: WalLogId) {
        let mut inner = self.inner.borrow_mut();
        let mut removed = 0;
        {
            let log = inner.groups.entry(group).or_default();
            let upto = log_id.index;
            // a purge never moves the boundary backwards
            log.purged = match log.purged.take() {
                Some(existing) if existing.index >= upto => Some(existing),
                _ => Some(log_id),
            };
            let dropped: Vec<u64> = log.entries.range(..=upto).map(|(index, _)| *index).collect();
            for index in dropped {
                if let Some(entry) = log.entries.remove(&index) {
                    removed += Self::weight(&entry);
                }
            }
        }
        inner.bytes = inner.bytes.saturating_sub(removed);
    }
}
