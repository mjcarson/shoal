//! The operations every arm sends, as kinds shoal-loadgen's driver is handed
//!
//! The driver picks, times and reports a kind it is handed exactly as it does its own read and
//! insert, and knows nothing else about it: a kind builds one operation's query from that
//! operation's seed ([F69](../../docs/src/features/driver-operation-kinds.md)). Nothing is read
//! from a dataset, so a stripe row's bytes are made in the kind's build, on the stream that sends
//! them, as X13 found a benchmark's object bytes should be.
//!
//! A preloaded key is one of `0..rows`, written by the spike's own preload in [`crate::measure`];
//! a put's key is in the top half of `u64`, where no preloaded key is, so every put is a new row.

use std::sync::Arc;

use shoal::shared::dataset::OperationKind;
use shoal::shared::traits::QuerySupport;

use crate::bytes::{self, mix};
use crate::{SmallRow, SmallRowGet, StripeRow, StripeRowGet, StripesAsRowsClient};

/// A query of the spike's schema, in the form a client sends
pub type Query = <StripesAsRowsClient as QuerySupport>::QueryKinds;

/// The bit every put's key has set and no preloaded key does
pub const PUT_BIT: u64 = 1 << 63;

/// The text every small row carries, so a small row is the size of a typical table's
pub const NOTE: &str = "a small row beside the stripes, read and written lightly throughout";

/// The key a put's seed names: a new one, in the top half of `u64`
///
/// # Arguments
///
/// * `seed` - The operation's seed
#[must_use]
pub fn put_key(seed: u64) -> u64 {
    mix(seed) | PUT_BIT
}

/// The preloaded key an operation's seed names, chosen evenly from `0..rows`
///
/// # Arguments
///
/// * `seed` - The operation's seed
/// * `rows` - How many rows were preloaded
#[must_use]
pub fn preloaded_key(seed: u64, rows: u64) -> u64 {
    mix(seed) % rows.max(1)
}

/// The stripe row a key and a write make
///
/// # Arguments
///
/// * `key` - The row's key
/// * `seed` - The write that made its bytes
/// * `size` - How many bytes it holds
#[must_use]
pub fn stripe_row(key: u64, seed: u64, size: usize) -> StripeRow {
    // the bytes are the key's stream under this write
    StripeRow {
        key,
        bytes: bytes::make(seed, key, size),
    }
}

/// The small row a key and a write make
///
/// # Arguments
///
/// * `key` - The row's key
/// * `value` - The value the write sets
#[must_use]
pub fn small_row(key: u64, value: u64) -> SmallRow {
    SmallRow {
        key,
        value,
        note: NOTE.to_string(),
    }
}

/// A put of a new stripe row, its key and bytes from the seed
pub struct Put {
    /// How many bytes each row holds
    pub size: usize,
}

impl OperationKind<StripesAsRowsClient> for Put {
    /// Its name in a workload's weights and a window
    fn name(&self) -> &str {
        "put"
    }

    /// A put writes
    fn writes(&self) -> bool {
        true
    }

    /// An insert of a row no preload wrote
    ///
    /// # Arguments
    ///
    /// * `seed` - The operation's seed
    fn build(&self, seed: u64) -> Query {
        // a key in the top half, which no preloaded key is in
        stripe_row(put_key(seed), seed, self.size).into()
    }
}

/// An overwrite of a preloaded stripe row with new bytes: a stripe rewritten in place
pub struct Overwrite {
    /// How many bytes each row holds
    pub size: usize,
    /// How many rows were preloaded, keyed `0..rows`
    pub rows: u64,
}

impl OperationKind<StripesAsRowsClient> for Overwrite {
    /// Its name in a workload's weights and a window
    fn name(&self) -> &str {
        "overwrite"
    }

    /// An overwrite writes
    fn writes(&self) -> bool {
        true
    }

    /// An insert over a preloaded row, its bytes from this operation's seed
    ///
    /// # Arguments
    ///
    /// * `seed` - The operation's seed
    fn build(&self, seed: u64) -> Query {
        // a preloaded key, chosen evenly
        stripe_row(preloaded_key(seed, self.rows), seed, self.size).into()
    }
}

/// A get of one preloaded stripe row
pub struct Get {
    /// How many rows were preloaded, keyed `0..rows`
    pub rows: u64,
}

impl OperationKind<StripesAsRowsClient> for Get {
    /// Its name in a workload's weights and a window
    fn name(&self) -> &str {
        "get"
    }

    /// A get only reads
    fn writes(&self) -> bool {
        false
    }

    /// A get of a preloaded key, chosen evenly; its answer has to hold the row
    ///
    /// # Arguments
    ///
    /// * `seed` - The operation's seed
    fn build(&self, seed: u64) -> Query {
        StripeRowGet::new(vec![preloaded_key(seed, self.rows)]).into()
    }
}

/// A get of one small row, the paced stream's read
pub struct SmallGet {
    /// How many small rows were preloaded, keyed `0..rows`
    pub rows: u64,
}

impl OperationKind<StripesAsRowsClient> for SmallGet {
    /// Its name in a workload's weights and a window
    fn name(&self) -> &str {
        "small_get"
    }

    /// A get only reads
    fn writes(&self) -> bool {
        false
    }

    /// A get of a preloaded small row, chosen evenly
    ///
    /// # Arguments
    ///
    /// * `seed` - The operation's seed
    fn build(&self, seed: u64) -> Query {
        SmallRowGet::new(vec![preloaded_key(seed, self.rows)]).into()
    }
}

/// An overwrite of one small row, the paced stream's write
pub struct SmallPut {
    /// How many small rows were preloaded, keyed `0..rows`
    pub rows: u64,
}

impl OperationKind<StripesAsRowsClient> for SmallPut {
    /// Its name in a workload's weights and a window
    fn name(&self) -> &str {
        "small_put"
    }

    /// A put writes
    fn writes(&self) -> bool {
        true
    }

    /// An insert over a preloaded small row, its value the seed
    ///
    /// # Arguments
    ///
    /// * `seed` - The operation's seed
    fn build(&self, seed: u64) -> Query {
        small_row(preloaded_key(seed, self.rows), seed).into()
    }
}

/// Every kind the main load is handed, in the order a picker counts their places
///
/// # Arguments
///
/// * `size` - How many bytes a stripe row holds
/// * `rows` - How many stripe rows were preloaded
#[must_use]
pub fn main_kinds(size: usize, rows: u64) -> Vec<Arc<dyn OperationKind<StripesAsRowsClient>>> {
    vec![
        Arc::new(Put { size }),
        Arc::new(Get { rows }),
        Arc::new(Overwrite { size, rows }),
    ]
}

/// Every kind the paced stream is handed
///
/// # Arguments
///
/// * `rows` - How many small rows were preloaded
#[must_use]
pub fn paced_kinds(rows: u64) -> Vec<Arc<dyn OperationKind<StripesAsRowsClient>>> {
    vec![Arc::new(SmallGet { rows }), Arc::new(SmallPut { rows })]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A put's key is never a preloaded one, and an overwrite's and a get's always are
    #[test]
    fn keys_fall_where_they_should() {
        let rows = 3072;
        for seed in 0..10_000u64 {
            assert!(put_key(seed) >= rows);
            assert_ne!(put_key(seed) & PUT_BIT, 0);
            assert!(preloaded_key(seed, rows) < rows);
        }
        // the seeds spread over every preloaded key
        let hit: std::collections::BTreeSet<u64> = (0..100_000u64).map(|seed| preloaded_key(seed, rows)).collect();
        assert_eq!(hit.len() as u64, rows);
    }

    /// A stripe row holds the size it was asked for, its bytes the key's stream under the write
    #[test]
    fn a_stripe_row_holds_its_size() {
        let row = stripe_row(5, 11, 65536);
        assert_eq!(row.key, 5);
        assert_eq!(row.bytes.len(), 65536);
        assert_eq!(row.bytes, bytes::make(11, 5, 65536));
        // an overwrite of the same key under another write sends other bytes
        assert_ne!(stripe_row(5, 12, 64).bytes, stripe_row(5, 11, 64).bytes);
    }

    /// Every kind's name is one a workload can name, and none is the driver's own
    #[test]
    fn kind_names_are_allowed() {
        let mut names: Vec<String> = main_kinds(1024, 10)
            .iter()
            .chain(paced_kinds(10).iter())
            .map(|kind| kind.name().to_string())
            .collect();
        for name in &names {
            assert!(name.chars().all(|c| c.is_ascii_lowercase() || c == '_'), "{name}");
            assert!(name != "read" && name != "insert");
        }
        names.sort();
        names.dedup();
        assert_eq!(names.len(), 5);
    }
}
