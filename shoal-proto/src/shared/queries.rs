//! The types needed for querying the database

use rkyv::{Archive, Deserialize, Serialize};
use tracing::instrument;
use uuid::Uuid;

pub mod parser;
mod sorted;
mod unsorted;

use crate::client::ShqlParseError;

use super::traits::{QuerySupport, RkyvSupport};

pub use sorted::*;
pub use unsorted::*;

/// Put a queries sort keys in sort order and drop any it named twice
///
/// A sort key names a single row, so the tables below this seek each key instead of walking
/// the partition looking for it. That makes the order the keys are in the order the rows come
/// back in, and a sorted table answers a partition in sort key order - so the keys are sorted
/// here rather than trusted to arrive that way. It also means `IN ('b', 'a')` and
/// `IN ('a', 'b')` are the same query, which is what an unordered set of keys should be.
///
/// A key named twice is kept once. Seeking it twice would hand back its row twice.
///
/// This is done as a query enters the server rather than as it is built, because a query
/// arriving over the wire is deserialized straight into its struct and never passes through
/// a constructor that could have done it.
///
/// # Arguments
///
/// * `sort_keys` - The sort keys to normalize
pub(crate) fn normalize_sort_keys<S: Ord + Clone>(sort_keys: &[S]) -> Vec<S> {
    // copy the keys we were given so the query we were handed is left alone
    let mut normalized = sort_keys.to_vec();
    // put them in the order the rows they name are held in
    normalized.sort_unstable();
    // drop any key that was named more than once
    normalized.dedup();
    normalized
}

/// A bundle of different query kinds
#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct Queries<S: QuerySupport> {
    /// The id for this query
    pub id: Uuid,
    /// The individual queries to execute
    pub queries: Vec<S::QueryKinds>,
    /// The base index for these queries
    ///
    /// This is used for stream queries to correctly increment our index values
    /// across streamed queries.
    pub base_index: usize,
}

impl<S: QuerySupport> Queries<S> {
    /// Add a new query onto this queries bundle
    ///
    /// # Arguments
    ///
    /// * `query` - The query to add
    #[must_use]
    pub fn add<Q: Into<S::QueryKinds>>(mut self, query: Q) -> Self {
        // add our query
        self.queries.push(query.into());
        self
    }

    /// Add a new query onto this queries bundle by a mutable reference
    ///
    /// # Arguments
    ///
    /// * `query` - The query to add
    pub fn add_mut<Q: Into<S::QueryKinds>>(&mut self, query: Q) {
        // add our query
        self.queries.push(query.into());
    }

    /// Add queries from an iterator onto this queries bundle
    ///
    /// # Arguments
    ///
    /// * `queries` - An iterator of queries to add
    #[must_use]
    pub fn add_from_iter<Q, I>(mut self, queries: I) -> Self
    where
        Q: Into<S::QueryKinds>,
        I: IntoIterator<Item = Q>,
    {
        self.queries.extend(queries.into_iter().map(Into::into));
        self
    }

    /// Add a query by parsing a query string
    ///
    /// This only works for select queries and is only recommended to be
    /// used if build the get queries directly is not possible.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to string to parse and add
    #[must_use]
    pub fn parse(mut self, query: &str) -> Result<Self, ShqlParseError> {
        // try to parse this query string
        let parsed = S::parse(query)?;
        // add our parsed query
        self.queries.push(parsed);
        Ok(self)
    }

    /// Load our queries
    #[instrument(name = "Queries<S>::access", skip_all, err(Debug))]
    pub fn access(raw: &[u8]) -> Result<&ArchivedQueries<S>, rkyv::rancor::Error>
    where
        for<'a> <Self as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
    {
        <Self as RkyvSupport>::access(raw)
    }

    ///  Get the number of sub queries in this query bundle
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    /// Returns true if this query bundle has no queries
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }
}

impl<S: QuerySupport> Default for Queries<S> {
    fn default() -> Self {
        Queries {
            id: Uuid::new_v4(),
            queries: Vec::with_capacity(1),
            base_index: 0,
        }
    }
}

impl<S: QuerySupport> RkyvSupport for Queries<S> {}

#[cfg(test)]
mod tests {
    use super::{normalize_sort_keys, SortRange, SortSelect};
    use std::ops::Bound;

    #[test]
    /// Sort keys are put in sort order so the rows they name come back in it
    fn sort_keys_are_put_in_sort_order() {
        // a query can name its keys in any order it likes
        let normalized = normalize_sort_keys(&["c", "a", "b"]);
        // the rows they name come back in sort order, so the keys are sought in it
        assert_eq!(normalized, vec!["a", "b", "c"]);
    }

    #[test]
    /// A sort key named twice is only sought once
    ///
    /// Seeking a key twice would hand its row back twice, which is the same reason
    /// `group_by_shard` deduplicates partition keys.
    fn a_repeated_sort_key_is_only_kept_once() {
        // a query naming the same row twice still names one row
        let normalized = normalize_sort_keys(&["b", "a", "b"]);
        assert_eq!(normalized, vec!["a", "b"]);
    }

    #[test]
    /// A query naming no sort keys still names none
    ///
    /// An empty list is how a get asks for every row in its partitions, so normalizing
    /// one must not turn it into anything else.
    fn no_sort_keys_stay_no_sort_keys() {
        // an empty list of keys is left empty
        let normalized = normalize_sort_keys::<&str>(&[]);
        assert!(normalized.is_empty());
    }

    #[test]
    /// Normalizing a selection of keys sorts and deduplicates them
    ///
    /// This is the arm that `normalize_sort_keys` is for, reached the way a query reaches it.
    fn a_key_selection_is_normalized() {
        // a query can name its keys in any order it likes, and repeat them
        let normalized = SortSelect::Keys(vec!["c", "a", "c", "b"]).normalized();
        // the keys are sought in the order the rows they name come back in, once each
        assert_eq!(normalized.keys(), Some(["a", "b", "c"].as_slice()));
    }

    #[test]
    /// Normalizing a range leaves its bounds exactly as they were
    ///
    /// A range is already an ordered pair, so there is nothing to sort and nothing to drop.
    /// Normalization must not quietly widen or tighten what was asked for.
    fn a_range_survives_normalization() {
        // build a range bounded at both ends
        let range = SortRange::new(Bound::Excluded("b"), Bound::Included("d"));
        // normalize the selection holding it
        let normalized = SortSelect::Range(range).normalized();
        // both bounds came through untouched
        let normalized = normalized.range().expect("a range normalized into something else");
        assert!(matches!(normalized.start, Bound::Excluded("b")));
        assert!(matches!(normalized.end, Bound::Included("d")));
    }

    #[test]
    /// Normalizing "every row" still means every row
    ///
    /// `All` is the only arm that means the whole partition, so nothing may turn it into a
    /// selection that names none.
    fn all_rows_stay_all_rows() {
        // normalize a selection asking for the whole partition
        let normalized = SortSelect::<&str>::All.normalized();
        // it still asks for the whole partition
        assert!(matches!(normalized, SortSelect::All));
        assert!(normalized.keys().is_none());
        assert!(normalized.range().is_none());
    }

    #[test]
    /// A range with a bound at only one end can never be empty
    ///
    /// An unbounded end cannot cross the other one, so the emptiness check must not read a
    /// missing bound as a crossing.
    fn a_half_open_range_is_never_empty() {
        // a range open at its upper end
        assert!(!SortRange::after("z").is_empty());
        assert!(!SortRange::starting_at("z").is_empty());
        // a range open at its lower end
        assert!(!SortRange::before("a").is_empty());
        assert!(!SortRange::ending_at("a").is_empty());
        // a range open at both ends
        assert!(!SortRange::<&str>::default().is_empty());
    }

    #[test]
    /// A range whose start is past its end holds nothing
    ///
    /// `BTreeMap::range` panics when it is handed one of these, so the scans ask this first.
    fn an_inverted_range_is_empty() {
        // a range running backwards names no rows
        assert!(SortRange::new(Bound::Included("z"), Bound::Included("a")).is_empty());
        assert!(SortRange::new(Bound::Excluded("z"), Bound::Excluded("a")).is_empty());
    }

    #[test]
    /// A range over one key is empty unless both of its ends include that key
    ///
    /// This is the other shape `BTreeMap::range` panics on, and the easier of the two to
    /// write by accident.
    fn a_point_range_needs_both_ends_to_include_it() {
        // both ends including one key names that one row
        assert!(!SortRange::new(Bound::Included("m"), Bound::Included("m")).is_empty());
        // any exclusion at either end leaves nothing between them
        assert!(SortRange::new(Bound::Excluded("m"), Bound::Excluded("m")).is_empty());
        assert!(SortRange::new(Bound::Included("m"), Bound::Excluded("m")).is_empty());
        assert!(SortRange::new(Bound::Excluded("m"), Bound::Included("m")).is_empty());
    }

    #[test]
    /// A range says which keys fall inside it, respecting whether each end includes its own
    fn a_range_knows_which_keys_it_holds() {
        // a range excluding its lower bound and including its upper one
        let range = SortRange::new(Bound::Excluded("b"), Bound::Included("d"));
        // the excluded lower bound is outside it and the included upper one is inside
        assert!(!range.contains(&"b"));
        assert!(range.contains(&"c"));
        assert!(range.contains(&"d"));
        // anything past either end is outside it
        assert!(!range.contains(&"a"));
        assert!(!range.contains(&"e"));
    }
}
