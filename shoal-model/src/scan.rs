//! The scan oracle: what a scan handed out, judged against each tablet group's committed history
//!
//! W13's P20 to P22 (`docs/src/future-work/scans/contract.md`), each judged for one tablet group
//! alone. P23 is the oracle's scope: nothing here relates two groups, so no check can lean on a
//! promise the contract withholds - that two groups were read at one moment, that a scan is a
//! snapshot, or that its order across tablets means anything. Within a group, a page's place in
//! the scan is its tablet's cursor and the group's floor, and nothing else.
//!
//! The history is what a test can know without the engine's help: every write a group committed,
//! with the index its session token named, and every page the scan was answered, with the
//! boundary its copy named, the cursor it began after, the cursor it ended at and what it handed
//! out. A partition's value is a number the test chose, so a page is judged by values rather than
//! by bytes, which keeps this free of any shoal crate.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// A write a tablet group committed: a partition set to a value, or removed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanWrite {
    /// The group that committed it, as its session token named it
    pub group: u64,
    /// The tablet its key is in
    pub tablet: u16,
    /// The partition's key
    pub key: u64,
    /// The log index it committed at
    pub index: u64,
    /// The partition's value after it, or none for a removal
    pub value: Option<u64>,
}

/// One page a scan was answered with
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanPage {
    /// The group whose copy answered it
    pub group: u64,
    /// The tablet it walked
    pub tablet: u16,
    /// The applied index its copy had reached when it began
    pub boundary: u64,
    /// When the scan asked for it, on one clock the scan keeps for all its pages
    pub asked: u64,
    /// When the scan was answered it, on the same clock
    pub answered: u64,
    /// The key it began after, or none for its tablet's first page
    pub after: Option<u64>,
    /// The key the next page begins after, or none once its tablet is walked
    pub next: Option<u64>,
    /// Every partition it handed out, each with the value it read, in key order
    pub partitions: Vec<(u64, u64)>,
}

/// A scan's history: what was committed, and what the scan was answered
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanHistory {
    /// The tablets the scan walked, each with its group; a write to any other is not the scan's
    pub tablets: BTreeMap<u16, u64>,
    /// Every committed write, in any order
    pub writes: Vec<ScanWrite>,
    /// Every page the scan used, in any order
    pub pages: Vec<ScanPage>,
}

/// A way a scan's history breaks P20 to P22, always within one group
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanViolation {
    /// P20: a partition handed out by two pages, as when a page asked again of another copy is
    /// used beside the first answer
    HandedOutTwice {
        /// The group
        group: u64,
        /// The partition
        key: u64,
    },
    /// P20: a partition live at the group's floor and never removed after it, never handed out
    Missed {
        /// The group
        group: u64,
        /// The partition
        key: u64,
    },
    /// P21: a page read at a state older than its group's floor when the scan asked for it
    BelowFloor {
        /// The group
        group: u64,
        /// The page's tablet
        tablet: u16,
        /// The page's boundary
        boundary: u64,
        /// The group's floor when the page was asked for
        floor: u64,
    },
    /// P21: a partition handed out at a value no state at or after its page's boundary held
    OlderThanBoundary {
        /// The group
        group: u64,
        /// The partition
        key: u64,
        /// The value handed out
        value: u64,
        /// The page's boundary
        boundary: u64,
    },
    /// P22: a partition handed out at a value no committed write gave it
    NotCommitted {
        /// The group
        group: u64,
        /// The partition
        key: u64,
        /// The value handed out
        value: u64,
    },
    /// A page of a tablet the scan does not walk or under another group than its tablet's, or a
    /// partition outside its page's range or tablet
    OutsideItsPage {
        /// The group the page named
        group: u64,
        /// The page's tablet
        tablet: u16,
        /// The partition, if the page was at fault for one
        key: Option<u64>,
    },
    /// A tablet whose pages do not join end to end, once each, from its start to its end
    Unwalked {
        /// The tablet's group
        group: u64,
        /// The tablet
        tablet: u16,
    },
}

impl ScanViolation {
    /// The group the violation is within, which every violation names
    #[must_use]
    pub fn group(&self) -> u64 {
        // every arm names the one group it was judged in
        match self {
            ScanViolation::HandedOutTwice { group, .. }
            | ScanViolation::Missed { group, .. }
            | ScanViolation::BelowFloor { group, .. }
            | ScanViolation::OlderThanBoundary { group, .. }
            | ScanViolation::NotCommitted { group, .. }
            | ScanViolation::OutsideItsPage { group, .. }
            | ScanViolation::Unwalked { group, .. } => *group,
        }
    }
}

/// One partition's committed history in a group: each write's index and the value after it
struct KeyHistory {
    /// The tablet the partition is in
    tablet: u16,
    /// Its writes, by index
    writes: BTreeMap<u64, Option<u64>>,
}

impl KeyHistory {
    /// The partition's value once every write at or below an index is applied
    ///
    /// # Arguments
    ///
    /// * `index` - The index
    fn state_at(&self, index: u64) -> Option<u64> {
        // the last write at or below the index, or nothing if none was
        self.writes
            .range(..=index)
            .next_back()
            .and_then(|(_, value)| *value)
    }

    /// Whether a value is the partition's state at some index at or after another
    ///
    /// The state at the index itself, or the value of any write after it.
    ///
    /// # Arguments
    ///
    /// * `value` - The value
    /// * `index` - The index
    fn held_at_or_after(&self, value: u64, index: u64) -> bool {
        // the state at the index, or a later write's value
        self.state_at(index) == Some(value)
            || self
                .writes
                .range(index + 1..)
                .any(|(_, written)| *written == Some(value))
    }

    /// Whether any write gave the partition a value
    ///
    /// # Arguments
    ///
    /// * `value` - The value
    fn ever_held(&self, value: u64) -> bool {
        // any committed write of it
        self.writes.values().any(|written| *written == Some(value))
    }

    /// Whether the partition is removed by any write after an index
    ///
    /// # Arguments
    ///
    /// * `index` - The index
    fn removed_after(&self, index: u64) -> bool {
        // a removal later than the index
        self.writes
            .range(index + 1..)
            .any(|(_, written)| written.is_none())
    }
}

/// Judge a scan's history against P20 to P22, one group at a time
///
/// Returns the first violation found, groups in ascending order and each group's checks in a
/// fixed order: what a page claims, a partition handed out twice, a tablet's pages joining, every
/// partition inside its page, the floors, the values, and what was missed. A group's floor is the
/// highest boundary of any of its pages answered before a page was asked; the floor a partition
/// must be live at to be owed is the group's lowest boundary, where its scan began.
///
/// # Arguments
///
/// * `history` - What was committed, and what the scan was answered
///
/// # Errors
///
/// The first way the history breaks the contract, naming the group it was judged in.
pub fn check(history: &ScanHistory) -> Result<(), ScanViolation> {
    // every group the scan walks a tablet of, or a page names
    let groups: BTreeSet<u64> = history
        .tablets
        .values()
        .copied()
        .chain(history.pages.iter().map(|page| page.group))
        .collect();
    for group in groups {
        check_group(history, group)?;
    }
    Ok(())
}

/// Judge one group's part of a scan's history, and nothing of any other group's
///
/// # Arguments
///
/// * `history` - What was committed, and what the scan was answered
/// * `group` - The group
fn check_group(history: &ScanHistory, group: u64) -> Result<(), ScanViolation> {
    // the group's pages, and the tablets the scan walks of it
    let pages: Vec<&ScanPage> = history.pages.iter().filter(|page| page.group == group).collect();
    let tablets: BTreeSet<u16> = history
        .tablets
        .iter()
        .filter(|(_, owner)| **owner == group)
        .map(|(tablet, _)| *tablet)
        .collect();
    // every page is of a tablet the scan walks, under the tablet's own group
    for page in &pages {
        if history.tablets.get(&page.tablet) != Some(&group) {
            return Err(ScanViolation::OutsideItsPage {
                group,
                tablet: page.tablet,
                key: None,
            });
        }
    }
    // the committed history of every partition the group wrote in a tablet the scan walks
    let mut keys: BTreeMap<u64, KeyHistory> = BTreeMap::new();
    for write in history
        .writes
        .iter()
        .filter(|write| write.group == group && tablets.contains(&write.tablet))
    {
        keys.entry(write.key)
            .or_insert_with(|| KeyHistory {
                tablet: write.tablet,
                writes: BTreeMap::new(),
            })
            .writes
            .insert(write.index, write.value);
    }
    // P20: no partition handed out twice
    let mut handed: BTreeSet<u64> = BTreeSet::new();
    for page in &pages {
        for (key, _) in &page.partitions {
            if !handed.insert(*key) {
                return Err(ScanViolation::HandedOutTwice { group, key: *key });
            }
        }
    }
    // each tablet's pages join from its start to its end, once each
    for tablet in &tablets {
        let mut by_cursor: BTreeMap<Option<u64>, &ScanPage> = BTreeMap::new();
        for page in pages.iter().filter(|page| page.tablet == *tablet) {
            if by_cursor.insert(page.after, page).is_some() {
                return Err(ScanViolation::Unwalked { group, tablet: *tablet });
            }
        }
        let mut cursor = None;
        let mut joined = 0;
        loop {
            let Some(page) = by_cursor.get(&cursor) else {
                return Err(ScanViolation::Unwalked { group, tablet: *tablet });
            };
            joined += 1;
            match page.next {
                // a cursor that does not move on would join forever
                Some(next) if Some(next) <= cursor => {
                    return Err(ScanViolation::Unwalked { group, tablet: *tablet });
                }
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        // a page off the chain is one the scan used and could not have reached
        if joined != by_cursor.len() {
            return Err(ScanViolation::Unwalked { group, tablet: *tablet });
        }
    }
    // every partition inside its page's range, and in its page's tablet where the history knows it
    for page in &pages {
        for (key, _) in &page.partitions {
            let after_cursor = page.after.is_none_or(|after| *key > after);
            let before_next = page.next.is_none_or(|next| *key <= next);
            let in_tablet = keys.get(key).is_none_or(|known| known.tablet == page.tablet);
            if !(after_cursor && before_next && in_tablet) {
                return Err(ScanViolation::OutsideItsPage {
                    group,
                    tablet: page.tablet,
                    key: Some(*key),
                });
            }
        }
    }
    // P21: no page read below its group's floor when it was asked for
    for page in &pages {
        let floor = pages
            .iter()
            .filter(|earlier| earlier.answered < page.asked)
            .map(|earlier| earlier.boundary)
            .max()
            .unwrap_or(0);
        if page.boundary < floor {
            return Err(ScanViolation::BelowFloor {
                group,
                tablet: page.tablet,
                boundary: page.boundary,
                floor,
            });
        }
    }
    // P22 then P21: every value committed, and held at or after its page's boundary
    for page in &pages {
        for (key, value) in &page.partitions {
            let committed = keys.get(key).filter(|known| known.ever_held(*value));
            let Some(known) = committed else {
                return Err(ScanViolation::NotCommitted {
                    group,
                    key: *key,
                    value: *value,
                });
            };
            if !known.held_at_or_after(*value, page.boundary) {
                return Err(ScanViolation::OlderThanBoundary {
                    group,
                    key: *key,
                    value: *value,
                    boundary: page.boundary,
                });
            }
        }
    }
    // P20: every partition live at the group's floor and never removed after it was handed out
    if let Some(floor) = pages.iter().map(|page| page.boundary).min() {
        for (key, known) in &keys {
            let owed = known.state_at(floor).is_some() && !known.removed_after(floor);
            if owed && !handed.contains(key) {
                return Err(ScanViolation::Missed { group, key: *key });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The first key of a tablet, so a test's keys fall in the tablets it names
    ///
    /// # Arguments
    ///
    /// * `tablet` - The tablet
    /// * `n` - The key's place in it
    fn key(tablet: u16, n: u64) -> u64 {
        // a tablet is a key's top twelve bits
        (u64::from(tablet) << 52) | n
    }

    /// A committed write
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `tablet` - The tablet
    /// * `key` - The partition
    /// * `index` - The index it committed at
    /// * `value` - The value after it, or none for a removal
    fn write(group: u64, tablet: u16, key: u64, index: u64, value: Option<u64>) -> ScanWrite {
        ScanWrite {
            group,
            tablet,
            key,
            index,
            value,
        }
    }

    /// A page that walks its tablet whole
    ///
    /// # Arguments
    ///
    /// * `group` - The group
    /// * `tablet` - The tablet
    /// * `boundary` - Its boundary
    /// * `clock` - When it was asked for; it is answered one tick after
    /// * `partitions` - What it handed out
    fn whole(group: u64, tablet: u16, boundary: u64, clock: u64, partitions: &[(u64, u64)]) -> ScanPage {
        ScanPage {
            group,
            tablet,
            boundary,
            asked: clock,
            answered: clock + 1,
            after: None,
            next: None,
            partitions: partitions.to_vec(),
        }
    }

    /// Two groups read at different moments, their tablets interleaved, one tablet in two pages
    fn two_groups() -> ScanHistory {
        let (a, b) = (1, 2);
        let mut history = ScanHistory::default();
        history.tablets.insert(1, a);
        history.tablets.insert(2, b);
        history.tablets.insert(3, a);
        history.writes = vec![
            write(a, 1, key(1, 1), 5, Some(10)),
            write(a, 1, key(1, 2), 6, Some(12)),
            write(a, 3, key(3, 1), 11, Some(30)),
            write(b, 2, key(2, 1), 40, Some(20)),
        ];
        history.pages = vec![
            // group a's first tablet in two pages, at the group's floor
            ScanPage {
                group: a,
                tablet: 1,
                boundary: 10,
                asked: 1,
                answered: 2,
                after: None,
                next: Some(key(1, 1)),
                partitions: vec![(key(1, 1), 10)],
            },
            ScanPage {
                group: a,
                tablet: 1,
                boundary: 10,
                asked: 3,
                answered: 4,
                after: Some(key(1, 1)),
                next: None,
                partitions: vec![(key(1, 2), 12)],
            },
            // group b read far later in its own log, between group a's pages
            whole(b, 2, 50, 5, &[(key(2, 1), 20)]),
            // and group a again, below group b's boundary and above its own floor
            whole(a, 3, 12, 7, &[(key(3, 1), 30)]),
        ];
        history
    }

    /// W13's M22 row: the oracle checks P20 to P22 for each group alone and nothing across groups
    ///
    /// Two groups read at different moments, their tablets interleaved, are accepted though no
    /// moment holds both groups' pages; a partition twice, a page below its floor and a row older
    /// than its boundary are each refused, and each refusal names the group it was judged in.
    #[test]
    fn scan_oracle_promises_nothing_across_groups() {
        let history = two_groups();
        // P23: two groups at different moments, interleaved, are no violation
        assert_eq!(check(&history), Ok(()));
        // a partition handed out twice, as by a page asked again of another copy and both used
        let mut twice = history.clone();
        let mut again = twice.pages[3].clone();
        again.asked = 9;
        again.answered = 10;
        twice.pages.push(again);
        let refused = check(&twice).expect_err("a partition handed out twice was accepted");
        assert_eq!(refused, ScanViolation::HandedOutTwice { group: 1, key: key(3, 1) });
        // a page below its group's floor: tablet 3's page asked once tablet 1's answered at 10
        let mut below = history.clone();
        below.pages[3].boundary = 8;
        below.writes[2].index = 7;
        let refused = check(&below).expect_err("a page below its floor was accepted");
        assert_eq!(
            refused,
            ScanViolation::BelowFloor {
                group: 1,
                tablet: 3,
                boundary: 8,
                floor: 10
            }
        );
        // group b's far higher boundary is no floor of group a's, which the accepted history showed
        assert!(history.pages[2].boundary > history.pages[3].boundary);
        // a row older than its page's boundary: overwritten at 8, handed out at 10 as it was before
        let mut older = history.clone();
        older.writes.push(write(1, 1, key(1, 1), 8, Some(11)));
        let refused = check(&older).expect_err("a row older than its boundary was accepted");
        assert_eq!(
            refused,
            ScanViolation::OlderThanBoundary {
                group: 1,
                key: key(1, 1),
                value: 10,
                boundary: 10
            }
        );
        // every refusal was within group a, whose pages broke it
        for violation in [
            check(&twice).unwrap_err(),
            check(&below).unwrap_err(),
            check(&older).unwrap_err(),
        ] {
            assert_eq!(violation.group(), 1, "{violation:?}");
        }
    }

    /// A scan that missed a partition it owed, handed out one never written, or left a tablet
    /// unwalked is refused; a partition removed after the floor may be missed, and a later write
    /// may be handed out in its place
    #[test]
    fn scan_oracle_refuses_what_a_scan_missed_or_made_up() {
        let history = two_groups();
        // a partition live at the floor and never removed, missed
        let mut missed = history.clone();
        missed.pages[1].partitions.clear();
        assert_eq!(
            check(&missed),
            Err(ScanViolation::Missed {
                group: 1,
                key: key(1, 2)
            })
        );
        // removed after the floor, it may be missed
        let mut removed = missed.clone();
        removed.writes.push(write(1, 1, key(1, 2), 13, None));
        assert_eq!(check(&removed), Ok(()));
        // a value no write gave the partition
        let mut made_up = history.clone();
        made_up.pages[2].partitions[0].1 = 99;
        assert_eq!(
            check(&made_up),
            Err(ScanViolation::NotCommitted {
                group: 2,
                key: key(2, 1),
                value: 99
            })
        );
        // a write after the page's boundary may be what it read
        let mut later = history.clone();
        later.writes.push(write(2, 2, key(2, 1), 60, Some(21)));
        later.pages[2].partitions[0].1 = 21;
        assert_eq!(check(&later), Ok(()));
        // a tablet whose last page never came
        let mut unwalked = history.clone();
        unwalked.pages.remove(1);
        assert_eq!(check(&unwalked), Err(ScanViolation::Unwalked { group: 1, tablet: 1 }));
        // a partition past the page's own range, which no other page holds
        let mut outside = history;
        outside.pages[0].partitions.push((key(1, 3), 7));
        assert_eq!(
            check(&outside),
            Err(ScanViolation::OutsideItsPage {
                group: 1,
                tablet: 1,
                key: Some(key(1, 3))
            })
        );
    }
}
