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
use crate::spec::Mix;
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
}

impl Pick {
    /// What kind of operation this is
    #[must_use]
    pub fn kind(&self) -> OpKind {
        match self {
            Pick::Read { .. } => OpKind::Read,
            Pick::Insert { .. } => OpKind::Insert,
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
    /// * `mix` - The arm's mix
    /// * `tables` - What the scan found in each table, in dataset order
    /// * `weights` - Each table's weight by name; empty weighs each by its pool
    /// * `distribution` - Which read keys are asked for most
    /// * `read_keys` - How many keys one read asks for
    /// * `seed` - The spec's seed
    /// * `stream` - The arm and run, so two arms do not make the same choices
    ///
    /// # Errors
    ///
    /// When the mix reads and no table has keys to read, or inserts and no table has rows to
    /// insert, or a weight names a table the dataset does not have.
    pub fn new(
        mix: &Mix,
        tables: &[&TableScan],
        weights: &BTreeMap<String, u32>,
        distribution: KeyDistribution,
        read_keys: usize,
        seed: u64,
        stream: &str,
    ) -> Result<Self, String> {
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
        if mix.reads() && read_tables.is_empty() {
            return Err(format!(
                "the mix {} reads, and no table has preloaded rows to read",
                mix.name
            ));
        }
        if mix.writes() && insert_tables.is_empty() {
            return Err(format!(
                "the mix {} inserts, and no table has rows left after its preload",
                mix.name
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
            read: mix.read,
            insert: mix.insert,
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
        let mix = "rw50".parse().unwrap();
        let build = |stream| {
            Picker::new(&mix, &refs, &BTreeMap::new(), KeyDistribution::Zipfian, 2, 7, stream)
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
            }
        }
    }

    /// The mix and the pools decide the shares, unless weights say otherwise
    #[test]
    fn shares_follow_the_mix_and_the_pools() {
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
        }));
    }

    /// A mix with nothing to act on is refused before anything runs
    #[test]
    fn a_mix_with_nothing_to_act_on_is_refused() {
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
