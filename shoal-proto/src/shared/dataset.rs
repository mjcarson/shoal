//! What a table needs to be loaded from a file and benchmarked by name ([F66](../../../docs/src/features/dataset-benchmarks.md))
//!
//! A benchmark is written once, over a schema's client type, and every schema has a different
//! set of row types. Nothing written once can name them, so the table derives emit these traits
//! for a table that asks for them with `#[shoal_table(db = "...", dataset)]` and `#[shoal::db]`
//! emits a dispatch from a table's name to its row type. A benchmark then reads a file named
//! after a table by handing a [`DatasetVisitor`] to [`DatasetSupport::visit_table`], and the
//! visitor is called back with the row type as a type parameter.
//!
//! None of this reaches the wire: the schema fingerprint never sees whether a table opted in, so
//! a client built with the attribute talks to a server built without it.

use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::hash::Hash;

use super::traits::QuerySupport;

/// A row that can be read from a dataset file and turned into the queries a benchmark sends
///
/// It is generic over the query kinds of the database the row belongs to, so the derive that
/// emits it never has to name that database's types: `K` is whatever implements `From` for the
/// row and for its get.
pub trait DatasetRow<K>: DeserializeOwned + Clone + Send + Sync + 'static {
    /// What a read of exactly this row is keyed by
    ///
    /// The partition key for an unsorted table, and the partition key paired with the sort key
    /// for a sorted one.
    type ReadKey: Clone + Debug + Hash + Eq + Send + Sync + 'static;

    /// Whether this row belongs to a sorted table
    const SORTED: bool;

    /// Get the key that reads exactly this row back
    fn read_key(&self) -> Self::ReadKey;

    /// Build the query that inserts this row
    fn insert_query(self) -> K;

    /// Build one get that reads every row the given keys name
    ///
    /// A sorted table's get names partitions and sort keys separately, so a get of several keys
    /// from different partitions also returns any row whose sort key matches in another one of
    /// the named partitions.
    ///
    /// # Arguments
    ///
    /// * `keys` - The keys of the rows to read, at least one
    fn read_query(keys: &[Self::ReadKey]) -> K;
}

/// Something called back with the row type of a table named at runtime
pub trait DatasetVisitor<K> {
    /// What the visit produces
    type Output;

    /// Visit the row type of the named table
    ///
    /// # Arguments
    ///
    /// * `table` - The name of the table whose row type this is
    fn visit<R: DatasetRow<K>>(self, table: &'static str) -> Self::Output;
}

/// A table's answer to whether it can be loaded from a dataset
///
/// Every table derive emits this. One that opted in hands a visitor its row type; one that did
/// not refuses by name, which is what lets a schema without serde on its rows still compile.
pub trait DatasetTable<K> {
    /// Whether this table opted in to being loaded from a dataset
    const DATASET: bool;

    /// Call a visitor back with this table's row type, if it opted in
    ///
    /// # Arguments
    ///
    /// * `visitor` - The visitor to call back
    ///
    /// # Errors
    ///
    /// When this table did not opt in.
    fn accept<V: DatasetVisitor<K>>(visitor: V) -> Result<V::Output, DatasetError>;
}

/// A database whose tables can be found by name and loaded from a dataset
///
/// `#[shoal::db]` emits this for every client, whether or not any of its tables opted in.
pub trait DatasetSupport: QuerySupport {
    /// Every table in this database and whether it opted in, in field order
    fn dataset_tables() -> &'static [(&'static str, bool)];

    /// Call a visitor back with the row type of the table with this name
    ///
    /// # Arguments
    ///
    /// * `table` - The name of the table, exactly as `QuerySupport::table_names` reports it
    /// * `visitor` - The visitor to call back
    ///
    /// # Errors
    ///
    /// When no table has this name, or the table with it did not opt in.
    fn visit_table<V: DatasetVisitor<Self::QueryKinds>>(
        table: &str,
        visitor: V,
    ) -> Result<V::Output, DatasetError>;
}

/// Why a table could not be loaded from a dataset
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetError {
    /// No table in the database has this name
    UnknownTable {
        /// The name that was asked for
        table: String,
        /// Every table the database does have
        known: Vec<&'static str>,
    },
    /// The table exists but did not opt in with `#[shoal_table(dataset)]`
    NotOptedIn {
        /// The table's name
        table: &'static str,
    },
}

impl DatasetError {
    /// Build the error for a name no table has
    ///
    /// # Arguments
    ///
    /// * `table` - The name that was asked for
    /// * `known` - Every table the database has, with whether each opted in
    #[must_use]
    pub fn unknown(table: &str, known: &[(&'static str, bool)]) -> Self {
        DatasetError::UnknownTable {
            table: table.to_string(),
            known: known.iter().map(|(name, _)| *name).collect(),
        }
    }
}

impl std::fmt::Display for DatasetError {
    /// Say which table and why, in the words an operator acts on
    ///
    /// # Arguments
    ///
    /// * `f` - The formatter to write to
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // each refusal names the table and what to do about it
        match self {
            DatasetError::UnknownTable { table, known } => write!(
                f,
                "no table is named {table:?}; this database has {}",
                known.join(", ")
            ),
            DatasetError::NotOptedIn { table } => write!(
                f,
                "table {table:?} cannot be loaded from a dataset: add `dataset` to its \
                 #[shoal_table(...)] and derive serde::Deserialize on it"
            ),
        }
    }
}

impl std::error::Error for DatasetError {}
