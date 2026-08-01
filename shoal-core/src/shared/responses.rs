//! A response from a set of queries
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    client::{Errors, QuerySuceededOpts},
    shared::traits::{PartitionKeySupport, ShoalSortedTable},
};

/// The different response kind types
#[derive(Debug, Archive, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub enum ResponseActionNames {
    /// A response to an insert query
    Insert,
    /// A response to a get query
    Get,
    /// A response to a delete query
    Delete,
    /// A response to an update query
    Update,
    /// A response to an exists query
    Exists,
}

/// The different response kinds from a query
#[derive(Debug, Archive, Serialize, Deserialize)]
pub enum ResponseAction<T> {
    /// Whether an insert was successful or not
    Insert(bool),
    /// The response from a get query
    Get(Option<Vec<T>>),
    /// The response from a delete
    Delete(bool),
    /// The response from an update
    Update(bool),
    /// The response from an exists query - true if data exists
    Exists(bool),
}

impl<T> ResponseAction<T> {
    /// Merge another shards share of this queries answer into ours
    ///
    /// Only a get and an exists can be split across shards, since every other query
    /// names a single partition and so has a single owner. A write that somehow got
    /// here keeps the answer it already had rather than inventing one.
    ///
    /// # Arguments
    ///
    /// * `other` - The other shards share of this queries answer
    pub fn merge(&mut self, other: Self) {
        // combine our two shares by the kind of query they answer
        match (self, other) {
            // a get is the union of the rows each shard found
            (ResponseAction::Get(ours), ResponseAction::Get(theirs)) => {
                // pull out the rows they found, if they found any
                let Some(theirs) = theirs else {
                    return;
                };
                // add their rows to ours, or take theirs if we found none
                match ours {
                    Some(ours) => ours.extend(theirs),
                    None => *ours = Some(theirs),
                }
            }
            // a row exists if any shard we asked found it
            (ResponseAction::Exists(ours), ResponseAction::Exists(theirs)) => *ours |= theirs,
            // every other query names a single partition, so it is never split
            _ => (),
        }
    }

    /// Drop any rows past this queries limit
    ///
    /// A limit is applied on each shard as it scans, so every share that arrives here
    /// is already no longer than the limit. Their union can still be longer, which is
    /// what this trims.
    ///
    /// # Arguments
    ///
    /// * `limit` - The most rows this query asked for
    pub fn truncate(&mut self, limit: usize) {
        // only a get returns rows that could be over a limit
        if let ResponseAction::Get(Some(rows)) = self {
            // drop everything past our limit
            rows.truncate(limit);
            // an emptied get answers None, the same as a get that found nothing
            if rows.is_empty() {
                *self = ResponseAction::Get(None);
            }
        }
    }
}

impl<T: PartitionKeySupport> ResponseAction<T> {
    /// Put our rows back into the order the query named their partitions in
    ///
    /// Each shard answers with the rows of its own partitions, in the order the query named
    /// them, but the shares are merged in whatever order they arrive. Sorting by where each
    /// rows partition was named undoes that, and leaves a get whose answer depends only on
    /// the query and not on which shard happened to reply first.
    ///
    /// The sort is stable, so rows within one partition keep the order their shard gave them,
    /// which for a sorted table is their sort key order.
    ///
    /// # Arguments
    ///
    /// * `order` - The partitions this query named, in the order it named them
    pub fn order_by_partitions(&mut self, order: &[u64]) {
        // only a get comes back as rows there is an order to
        let ResponseAction::Get(Some(rows)) = self else {
            return;
        };
        // a query naming one partition has nothing to interleave
        if order.len() < 2 {
            return;
        }
        // build the rank of each partition this query named
        let ranks: HashMap<u64, usize> = order
            .iter()
            .enumerate()
            .map(|(rank, key)| (*key, rank))
            .collect();
        // sort our rows by where their partition was named, hashing each row's key once
        //
        // a row from a partition this query did not name cannot happen, but sorting it last
        // keeps this total rather than panicking on a key we cannot place
        rows.sort_by_cached_key(|row| {
            ranks
                .get(&row.get_partition_key())
                .copied()
                .unwrap_or(usize::MAX)
        });
    }
}

/// A response from a query
#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct Response<T> {
    /// The id for the query we are responding too
    pub id: Uuid,
    /// This response index in the queries vec
    pub index: usize,
    /// The response data
    pub data: ResponseAction<T>,
    /// Whether this is the last response for a query or not
    pub end: bool,
}

impl<T> Response<T> {
    /// Merge another shards share of this queries answer into ours
    ///
    /// # Arguments
    ///
    /// * `other` - The other shards share of this queries answer
    pub fn merge(&mut self, other: Self) {
        self.data.merge(other.data);
    }

    /// Drop any rows past this queries limit
    ///
    /// # Arguments
    ///
    /// * `limit` - The most rows this query asked for
    pub fn truncate(&mut self, limit: usize) {
        self.data.truncate(limit);
    }
}

impl<T: PartitionKeySupport> Response<T> {
    /// Put our rows back into the order the query named their partitions in
    ///
    /// # Arguments
    ///
    /// * `order` - The partitions this query named, in the order it named them
    pub fn order_by_partitions(&mut self, order: &[u64]) {
        self.data.order_by_partitions(order);
    }
}

/// Check if a response succeeded or not
///
/// # Arguments
///
/// * `should_check` - Whether this response should be checked
/// * `value` - The value to check
/// * `kind` - The kind of query that is being checked
macro_rules! check_response {
    ($should_check:expr, $value:expr, $kind:expr) => {
        // only check queries that we are requried too
        if $should_check {
            // check if this query failed or not
            if $should_check == $value {
                return Ok(());
            }
            // this query failed so return its kind name
            $kind
        } else {
            return Ok(());
        }
    };
}

impl<T: Archive> ArchivedResponse<T> {
    /// Check if this query succeeded according to our criteria
    pub fn succeeded(&self, opts: QuerySuceededOpts) -> Result<(), Errors> {
        // check if this query failed or not
        let kind = match &self.data {
            ArchivedResponseAction::Insert(inserted) => {
                check_response!(opts.insert, *inserted, ResponseActionNames::Insert)
            }
            ArchivedResponseAction::Get(got) => {
                check_response!(opts.get, got.is_some(), ResponseActionNames::Get)
            }
            ArchivedResponseAction::Update(updated) => {
                check_response!(opts.update, *updated, ResponseActionNames::Update)
            }
            ArchivedResponseAction::Delete(deleted) => {
                check_response!(opts.delete, *deleted, ResponseActionNames::Delete)
            }
            ArchivedResponseAction::Exists(exists) => {
                check_response!(opts.exists, *exists, ResponseActionNames::Exists)
            }
        };
        // this query failed and it was required to succeed
        // build a nice descriptive error for it
        let error = Errors::QueryDidNotSucceed {
            id: self.id,
            index: self.index.to_native() as usize,
            kind,
            end: self.end,
        };
        Err(error)
    }

    /// Get the kind of query this is a response to
    pub fn kind(&self) -> ResponseActionNames {
        match &self.data {
            ArchivedResponseAction::Insert(_) => ResponseActionNames::Insert,
            ArchivedResponseAction::Get(_) => ResponseActionNames::Get,
            ArchivedResponseAction::Update(_) => ResponseActionNames::Update,
            ArchivedResponseAction::Delete(_) => ResponseActionNames::Delete,
            ArchivedResponseAction::Exists(_) => ResponseActionNames::Exists,
        }
    }

    /// Mark this response as the last one
    pub fn end(&mut self) {
        self.end = true;
    }

    /// Get the exists result if this is an Exists response
    ///
    /// Returns `Some(bool)` if this is an Exists response, `None` otherwise
    pub fn get_exists(&self) -> Option<bool> {
        match &self.data {
            ArchivedResponseAction::Exists(exists) => Some(*exists),
            _ => None,
        }
    }
}

/// The responses from a set of queries
#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct Responses<D: ShoalSortedTable> {
    /// The responses for our queries
    pub responses: Vec<Response<D>>,
}

impl<D: ShoalSortedTable> Responses<D> {
    /// Create an empty responses object of the correct size
    ///
    /// # Arguments
    ///
    /// * `len` - The number of queries we are responding too
    pub fn with_capacity(len: usize) -> Self {
        // build our responses bundle
        Responses {
            responses: Vec::with_capacity(len),
        }
    }

    /// A new response to this response bundle
    ///
    /// # Arguments
    ///
    /// * `response` - The response to add
    pub fn add(&mut self, response: Response<D>) {
        // add this response
        self.responses.push(response);
    }
}
