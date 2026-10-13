//! Which operation, table and key a worker's next query is
//!
//! Every choice is a function of the arm, the run, the worker and the operation's index within
//! that worker, and of nothing that happened during the run - which answer came back first, how
//! long a feed stalled. Two runs of one arm therefore send the same reads in the same order from
//! the same workers, and a difference between them is the cluster's.
//!
//! An insert is the one choice that is not fully addressed: which row it inserts is whichever
//! row its table's feed hands over next, since the insert pool is one stream shared by every
//! worker. The table it inserts into is chosen here like everything else.

use std::collections::BTreeMap;

use crate::feed::TableScan;
use crate::keys::{KeyDistribution, Keys};
use crate::seed::Seeded;
use crate::spec::Workload;
use crate::window::OpKind;

/// One query's choices
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pick {
    /// Read these places in a table's read key pool, as one get
    Read {
        /// The table, by its place in the dataset
        table: usize,
        /// Places in its read key pool
        keys: Vec<u64>,
    },
    /// Insert the next row of a table's insert pool
    Insert {
        /// The table, by its place in the dataset
        table: usize,
    },
    /// One operation of a kind the driver was handed
    /// ([F69](../../docs/src/features/driver-operation-kinds.md))
    Supplied {
        /// The kind, by its place among the kinds the driver was handed
        kind: usize,
        /// The operation's own seed, which the kind builds its query from
        seed: u64,
    },
}

impl Pick {
    /// What kind of operation this is, given the names of the kinds the driver was handed
    ///
    /// # Arguments
    ///
    /// * `names` - The supplied kinds' names, in the order the driver was handed them
    #[must_use]
    pub fn kind(&self, names: &[std::sync::Arc<str>]) -> OpKind {
        match self {
            Pick::Read { .. } => OpKind::Read,
            Pick::Insert { .. } => OpKind::Insert,
            Pick::Supplied { kind, .. } => OpKind::Supplied(names[*kind].clone()),
        }
    }
}

/// Tables with their cumulative weights, for a weighted draw
#[derive(Debug, Clone, Default)]
struct Weighted {
    /// Each table that can be chosen, by its place, with the running total up to and including it
    tables: Vec<(usize, u64)>,
}

impl Weighted {
    /// Build a draw over tables with these weights, leaving out every table weighted zero
    ///
    /// # Arguments
    ///
    /// * `weights` - Each table's weight, by its place
    fn new(weights: impl IntoIterator<Item = u64>) -> Self {
        // a running total, skipping tables that can never be chosen
        let mut total = 0u64;
        let tables = weights
            .into_iter()
            .enumerate()
            .filter(|(_, weight)| *weight > 0)
            .map(|(table, weight)| {
                total += weight;
                (table, total)
            })
            .collect();
        Weighted { tables }
    }

    /// Whether no table can be chosen
    fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Choose a table
    ///
    /// # Arguments
    ///
    /// * `draw` - Where the choice comes from
    fn choose(&self, draw: &mut Seeded) -> usize {
        // one table needs no draw, which keeps a one table dataset's key draws unshifted
        if self.tables.len() == 1 {
            return self.tables[0].0;
        }
        let total = self.tables.last().map_or(1, |(_, total)| *total);
        let point = draw.below(total);
        self.tables
            .iter()
            .find(|(_, running)| point < *running)
            .map_or(self.tables[0].0, |(table, _)| *table)
    }
}

/// Makes every choice an arm's workers make
#[derive(Debug, Clone)]
pub struct Picker {
    /// The weight of reads
    read: u32,
    /// The weight of inserts
    insert: u32,
    /// Each supplied kind the workload names, by its place among the driver's, with its weight
    supplied: Vec<(usize, u32)>,
    /// The tables a read can choose
    read_tables: Weighted,
    /// The tables an insert can choose
    insert_tables: Weighted,
    /// Each table's key chooser, if it has keys to read
    keys: Vec<Option<Keys>>,
    /// How many keys one read asks for
    read_keys: usize,
    /// The seed every choice is addressed from, folded with the arm and the run
    seed: u64,
}

impl Picker {
    /// Build the choices for one run of one arm
    ///
    /// # Arguments
    ///
    /// * `workload` - The arm's workload
    /// * `tables` - What the scan found in each table, in dataset order
    /// * `weights` - Each table's weight by name; empty weighs each by its pool
    /// * `distribution` - Which read keys are asked for most
    /// * `read_keys` - How many keys one read asks for
    /// * `seed` - The spec's seed
    /// * `stream` - The arm and run, so two arms do not make the same choices
    ///
    /// # Errors
    ///
    /// When the workload reads and no table has keys to read, or inserts and no table has rows to
    /// insert, or a weight names a table the dataset does not have.
    pub fn new(
        workload: &Workload,
        tables: &[&TableScan],
        weights: &BTreeMap<String, u32>,
        distribution: KeyDistribution,
        read_keys: usize,
        seed: u64,
        stream: &str,
    ) -> Result<Self, String> {
        // the driver's own two kinds, and nothing it was handed
        Self::new_with_kinds(
            workload,
            tables,
            weights,
            distribution,
            read_keys,
            seed,
            stream,
            &[],
        )
    }

    /// Build the choices for one run of one arm, with the kinds the driver was handed
    ///
    /// # Arguments
    ///
    /// * `workload` - The arm's workload
    /// * `tables` - What the scan found in each table, in dataset order
    /// * `weights` - Each table's weight by name; empty weighs each by its pool
    /// * `distribution` - Which read keys are asked for most
    /// * `read_keys` - How many keys one read asks for
    /// * `seed` - The spec's seed
    /// * `stream` - The arm and run, so two arms do not make the same choices
    /// * `kinds` - The names of the kinds the driver was handed, in its order
    ///   ([F69](../../docs/src/features/driver-operation-kinds.md))
    ///
    /// # Errors
    ///
    /// Everything [`Picker::new`] refuses, and a workload naming a kind the driver was not
    /// handed.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_kinds(
        workload: &Workload,
        tables: &[&TableScan],
        weights: &BTreeMap<String, u32>,
        distribution: KeyDistribution,
        read_keys: usize,
        seed: u64,
        stream: &str,
        kinds: &[&str],
    ) -> Result<Self, String> {
        // every kind the workload names has to be one the driver was handed, found by its place
        let mut supplied = Vec::with_capacity(workload.kinds.len());
        for (name, weight) in &workload.kinds {
            let Some(place) = kinds.iter().position(|kind| kind == name) else {
                return Err(format!(
                    "the workload {} names the kind {name:?}, which this schema does not supply; it supplies {}",
                    workload.name,
                    if kinds.is_empty() {
                        "read and insert alone".to_string()
                    } else {
                        format!("read, insert, {}", kinds.join(", "))
                    }
                ));
            };
            if *weight > 0 {
                supplied.push((place, *weight));
            }
        }
        // a weight for a table the dataset does not hold is a typo, not a zero
        for name in weights.keys() {
            if !tables.iter().any(|table| &table.table == name) {
                return Err(format!("--tables names {name:?}, which the dataset has no file for"));
            }
        }
        // each table's weight for each kind: as given, or its pool
        let weight = |table: &TableScan, pool: u64| {
            // no weights weighs each table by its pool, and a table with an empty pool is never
            // chosen whatever its weight
            if weights.is_empty() {
                return pool;
            }
            weights
                .get(&table.table)
                .map_or(0, |weight| u64::from(*weight) * u64::from(pool > 0))
        };
        let read_tables = Weighted::new(tables.iter().map(|table| weight(table, table.read_keys)));
        let insert_tables =
            Weighted::new(tables.iter().map(|table| weight(table, table.insert_rows)));
        if workload.reads() && read_tables.is_empty() {
            return Err(format!(
                "the workload {} reads, and no table has preloaded rows to read",
                workload.name
            ));
        }
        if workload.inserts() && insert_tables.is_empty() {
            return Err(format!(
                "the workload {} inserts, and no table has rows left after its preload",
                workload.name
            ));
        }
        // a key chooser per table with keys, each its own stream
        let keys = tables
            .iter()
            .map(|table| {
                (table.read_keys > 0)
                    .then(|| Keys::new(distribution, table.read_keys, seed, &table.table))
            })
            .collect();
        Ok(Picker {
            read: workload.read,
            insert: workload.insert,
            supplied,
            read_tables,
            insert_tables,
            keys,
            read_keys,
            seed: Seeded::stream(seed, stream).next_u64(),
        })
    }

    /// The choices for one worker's operation
    ///
    /// # Arguments
    ///
    /// * `worker` - Which worker
    /// * `index` - The operation's index within that worker
    #[must_use]
    pub fn at(&self, worker: usize, index: u64) -> Pick {
        // one generator per operation, addressed by worker and index
        let address = ((worker as u64) << 40) | (index & ((1 << 40) - 1));
        let mut draw = Seeded::at(self.seed, address);
        // a supplied kind, by weight beside read and insert, when the workload names one; a
        // workload that names none draws exactly as it did before there were any (F69)
        if !self.supplied.is_empty() {
            if let Some(pick) = self.supplied_at(&mut draw) {
                return pick;
            }
        }
        // the kind, by weight, without a draw when only one kind can be chosen
        let read = match (self.read, self.insert) {
            (0, _) => false,
            (_, 0) => true,
            (read, insert) => draw.below(u64::from(read + insert)) < u64::from(read),
        };
        if !read {
            return Pick::Insert {
                table: self.insert_tables.choose(&mut draw),
            };
        }
        // the table, then its keys
        let table = self.read_tables.choose(&mut draw);
        let chooser = self.keys[table]
            .as_ref()
            .expect("a table a read can choose has keys");
        let keys = (0..self.read_keys as u64)
            .map(|offset| chooser.at(address.wrapping_mul(self.read_keys as u64).wrapping_add(offset)))
            .collect();
        Pick::Read { table, keys }
    }

    /// Choose among read, insert and the supplied kinds, for a workload that names one
    ///
    /// Returns the supplied operation when one is chosen, and none when read or insert is, so
    /// the caller goes on to choose its table and keys. One draw over every weight, or none when
    /// only one kind has any.
    ///
    /// # Arguments
    ///
    /// * `draw` - Where the choice comes from
    fn supplied_at(&self, draw: &mut Seeded) -> Option<Pick> {
        // every weight, read and insert first
        let own = u64::from(self.read) + u64::from(self.insert);
        let total = own + self.supplied.iter().map(|(_, weight)| u64::from(*weight)).sum::<u64>();
        // with nothing but supplied weight and one kind, there is nothing to draw
        let point = if own == 0 && self.supplied.len() == 1 {
            0
        } else {
            draw.below(total)
        };
        // a point among read and insert leaves the choice to the caller
        if point < own && own > 0 {
            return None;
        }
        let mut running = own;
        for (kind, weight) in &self.supplied {
            running += u64::from(*weight);
            if point < running {
                return Some(Pick::Supplied {
                    kind: *kind,
                    seed: draw.next_u64(),
                });
            }
        }
        // unreachable while the weights sum to the total
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{Pick, Picker};
    use crate::dataset::Format;
    use crate::feed::TableScan;
    use crate::keys::KeyDistribution;
    use std::collections::BTreeMap;

    /// A scan with this many read keys and insert rows
    ///
    /// # Arguments
    ///
    /// * `table` - The table's name
    /// * `read_keys` - Keys to read
    /// * `insert_rows` - Rows to insert
    fn scan(table: &str, read_keys: u64, insert_rows: u64) -> TableScan {
        TableScan {
            table: table.to_string(),
            path: "x".into(),
            format: Format::Csv,
            sorted: false,
            rows: read_keys + insert_rows,
            parse_errors: 0,
            first_errors: Vec::new(),
            distinct_keys: read_keys + insert_rows,
            duplicate_rows: 0,
            bytes: 0,
            sha256: String::new(),
            preload_rows: read_keys,
            read_keys,
            insert_rows,
        }
    }

    /// The same arm makes the same choices, and another arm makes others
    #[test]
    fn choices_are_addressed_by_arm_worker_and_index() {
        let tables = [scan("A", 1000, 1000), scan("B", 3000, 0)];
        let refs: Vec<&TableScan> = tables.iter().collect();
        let workload = "rw50".parse().unwrap();
        let build = |stream| {
            Picker::new(&workload, &refs, &BTreeMap::new(), KeyDistribution::Zipfian, 2, 7, stream)
                .unwrap()
        };
        let first = build("rw50/b1/none/0");
        let again = build("rw50/b1/none/0");
        let other = build("rw50/b1/none/1");
        let picks = |picker: &Picker| (0..500).map(|index| picker.at(3, index)).collect::<Vec<_>>();
        assert_eq!(picks(&first), picks(&again));
        assert_ne!(picks(&first), picks(&other));
        // reads ask for two keys in range, inserts only go where there are rows
        for pick in picks(&first) {
            match pick {
                Pick::Read { table, keys } => {
                    assert_eq!(keys.len(), 2);
                    assert!(keys.iter().all(|key| *key < refs[table].read_keys));
                }
                Pick::Insert { table } => assert_eq!(table, 0),
                Pick::Supplied { .. } => panic!("no kind was supplied"),
            }
        }
    }

    /// The workload and the pools decide the shares, unless weights say otherwise
    #[test]
    fn shares_follow_the_workload_and_the_pools() {
        let tables = [scan("A", 1000, 100), scan("B", 3000, 100)];
        let refs: Vec<&TableScan> = tables.iter().collect();
        let read90 = "read90".parse().unwrap();
        let picker =
            Picker::new(&read90, &refs, &BTreeMap::new(), KeyDistribution::Uniform, 1, 7, "s")
                .unwrap();
        let (mut reads, mut b_reads) = (0, 0);
        for index in 0..10_000 {
            if let Pick::Read { table, .. } = picker.at(0, index) {
                reads += 1;
                b_reads += usize::from(table == 1);
            }
        }
        // about nine reads in ten, and three of four of them on the larger pool
        assert!((8_800..9_200).contains(&reads), "{reads}");
        let share = b_reads as f64 / reads as f64;
        assert!((0.72..0.78).contains(&share), "{share}");
        // a weight of zero takes a table out of the draw
        let weights = BTreeMap::from([("A".to_string(), 1)]);
        let picker =
            Picker::new(&read90, &refs, &weights, KeyDistribution::Uniform, 1, 7, "s").unwrap();
        assert!((0..1000).all(|index| match picker.at(0, index) {
            Pick::Read { table, .. } | Pick::Insert { table } => table == 0,
            Pick::Supplied { .. } => false,
        }));
    }

    /// A read and insert workload makes the choices it made before supplied kinds
    ///
    /// Two captures are one benchmark only if they send the same reads in the same order, so a
    /// workload that names no supplied kind has to draw exactly as it did before
    /// [F69](../../docs/src/features/driver-operation-kinds.md): the same draws, in the same
    /// order, for the same choices. Frozen on the tree before F69.
    #[test]
    fn picks_of_read_insert_workloads_are_unchanged() {
        use sha2::{Digest, Sha256};
        let tables = [scan("A", 1000, 1000), scan("B", 3000, 500)];
        let refs: Vec<&TableScan> = tables.iter().collect();
        let mut digests = Vec::new();
        for name in ["rw50", "read90", "read100", "insert100"] {
            let workload = name.parse().unwrap();
            let picker = Picker::new(
                &workload,
                &refs,
                &BTreeMap::new(),
                KeyDistribution::Zipfian,
                2,
                7,
                "frozen",
            )
            .unwrap();
            let picks: Vec<Pick> = (0..256).map(|index| picker.at(3, index)).collect();
            digests.push(crate::read::hex(&Sha256::digest(format!("{picks:?}").as_bytes())));
        }
        assert_eq!(
            digests,
            vec![
                "18f0e6fbd1f99ea7a55cf36fab72b9c67d026525337523e1843be9ddb588cea3",
                "e81ea0916d7ca12ec6165d84db58a2db84213ad5271a6732cfc97043eaa34442",
                "bc3241aeb04f7edf9a5d153ebdc4e7ccf5ae38c33a43907bd67e91261c99a724",
                "e444ddb1da26806b61b07a5fc232735155340388e808b727be19d26c021642af",
            ]
        );
    }

    /// A supplied kind is weighed beside read and insert, picked by its place, and seeded by the
    /// operation's address; one the driver was not handed is refused by name (F69)
    #[test]
    fn a_supplied_kind_is_weighed_and_picked() {
        let tables = [scan("A", 1000, 1000)];
        let refs: Vec<&TableScan> = tables.iter().collect();
        let build = |workload: &str, kinds: &[&str]| {
            Picker::new_with_kinds(
                &workload.parse().unwrap(),
                &refs,
                &BTreeMap::new(),
                KeyDistribution::Uniform,
                1,
                7,
                "s",
                kinds,
            )
        };
        // half reads, half the second of two supplied kinds
        let picker = build("read:50,lookup:50", &["scan", "lookup"]).unwrap();
        let picks: Vec<Pick> = (0..10_000).map(|index| picker.at(0, index)).collect();
        let lookups = picks
            .iter()
            .filter(|pick| matches!(pick, Pick::Supplied { kind: 1, .. }))
            .count();
        assert!((4_800..5_200).contains(&lookups), "{lookups}");
        assert!(picks.iter().all(|pick| !matches!(pick, Pick::Supplied { kind: 0, .. } | Pick::Insert { .. })));
        // the same operation gets the same seed, and two operations get two
        let seeds: Vec<u64> = picks
            .iter()
            .filter_map(|pick| match pick {
                Pick::Supplied { seed, .. } => Some(*seed),
                _ => None,
            })
            .collect();
        assert_eq!(picks, (0..10_000).map(|index| picker.at(0, index)).collect::<Vec<_>>());
        assert_ne!(seeds[0], seeds[1]);
        // a workload of one supplied kind alone picks nothing else
        let alone = build("lookup:1", &["lookup"]).unwrap();
        assert!((0..100).all(|index| matches!(alone.at(0, index), Pick::Supplied { kind: 0, .. })));
        // and a kind the driver was not handed is refused, naming it and what is supplied
        let refused = build("read:1,lookup:1", &["scan"]).unwrap_err();
        assert!(refused.contains("\"lookup\"") && refused.contains("scan"), "{refused}");
    }

    /// A workload with nothing to act on is refused before anything runs
    #[test]
    fn a_workload_with_nothing_to_act_on_is_refused() {
        let tables = [scan("A", 0, 100)];
        let refs: Vec<&TableScan> = tables.iter().collect();
        let read = "read100".parse().unwrap();
        let refused =
            Picker::new(&read, &refs, &BTreeMap::new(), KeyDistribution::Uniform, 1, 7, "s");
        assert!(refused.unwrap_err().contains("no table has preloaded rows"));
        let typo = BTreeMap::from([("Nope".to_string(), 1)]);
        let insert = "insert100".parse().unwrap();
        assert!(Picker::new(&insert, &refs, &typo, KeyDistribution::Uniform, 1, 7, "s").is_err());
    }
}
