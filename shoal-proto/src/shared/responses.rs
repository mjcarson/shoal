//! A response from a set of queries
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    client::{Errors, QuerySuceededOpts},
    shared::protocol::error::ErrorCode,
    shared::traits::{PartitionKeySupport, ShoalSortedTable},
};

/// The different response kind types
///
/// `Error` is appended rather than inserted, and every variant after it must be too. These are
/// derived by rkyv, so the order of this enum is its wire representation.
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
    /// A query that failed rather than answering
    Error,
}

/// A failure the server is answering a query with
///
/// The code is held as the raw number it is written as rather than as an [`ErrorCode`], because
/// this type is archived and `ErrorCode` lives in a module that has no rkyv dependency and must
/// not gain one — the protocol module exists to frame the serialization format, so it cannot
/// depend on it. Read the class of failure with [`ResponseError::code`] rather than by comparing
/// the field, which also gets a code a newer server knows about and this build does not read back
/// as [`ErrorCode::Unknown`] instead of as a number nothing matches.
#[derive(Debug, Archive, Serialize, Deserialize, Clone)]
pub struct ResponseError {
    /// What class of failure this is, as the number it is written as
    pub code: u16,
    /// What the server said about it, in one line, for a person
    pub msg: String,
}

impl ResponseError {
    /// Create a new failure to answer a query with
    ///
    /// # Arguments
    ///
    /// * `code` - What class of failure this is
    /// * `msg` - What to say about it
    pub fn new<M: Into<String>>(code: ErrorCode, msg: M) -> Self {
        ResponseError {
            code: code.as_u16(),
            msg: msg.into(),
        }
    }

    /// Get what class of failure this is
    pub fn code(&self) -> ErrorCode {
        ErrorCode::from_u16(self.code)
    }
}

impl ArchivedResponseError {
    /// Get what class of failure this is
    pub fn code(&self) -> ErrorCode {
        ErrorCode::from_u16(self.code.to_native())
    }

    /// Get what the server said about this failure
    pub fn msg(&self) -> &str {
        &self.msg
    }
}

/// The different response kinds from a query
///
/// `Error` is appended rather than inserted, for the same reason it is in [`ResponseActionNames`]:
/// rkyv derives this enum's wire representation from its order.
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
    /// This query failed rather than answering
    ///
    /// This is what separates "the row is not there" from "the copy of it could not be read".
    /// Before it existed both were `Get(None)` and the client had no way to tell them apart.
    Error(ResponseError),
}

impl<T> ResponseAction<T> {
    /// Merge another shards share of this queries answer into ours
    ///
    /// Only a get and an exists can be split across shards, since every other query
    /// names a single partition and so has a single owner. A write that somehow got
    /// here keeps the answer it already had rather than inventing one.
    ///
    /// **A failed share makes the whole answer a failure.** This is the one place where three
    /// shards worth of rows can hide a fourth shard's failure, and presenting a partial answer as
    /// a complete one is precisely the defect the error channel exists to remove — the client
    /// cannot tell "these are all the rows" from "these are the rows we could read" unless the
    /// merge refuses to make that choice for it.
    ///
    /// # Arguments
    ///
    /// * `other` - The other shards share of this queries answer
    pub fn merge(&mut self, other: Self) {
        // combine our two shares by the kind of query they answer
        match (self, other) {
            // a share that failed takes over the answer, whatever we had found so far
            (ours, ResponseAction::Error(theirs)) => *ours = ResponseAction::Error(theirs),
            // and an answer that has already failed stays failed, whatever anyone else found
            (ResponseAction::Error(_), _) => (),
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
    /// A failure is left alone: a limit bounds rows, and a failure has none. It cannot reach here
    /// as anything but the whole answer anyway, since a failed share wins the merge.
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
    /// A failure is left alone, for the same reason a [`ResponseAction::truncate`] leaves one
    /// alone: there are no rows to put in an order.
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
    ///
    /// A query that failed is never a success, whatever the options say. The options decide which
    /// *outcomes* count as success — whether an insert that inserted nothing is a problem, whether
    /// a get that matched nothing is — and a failure is not an outcome. A caller that passed
    /// `get: false` is saying it can live with an empty answer, not that it wants an unreadable
    /// partition reported as one.
    ///
    /// # Arguments
    ///
    /// * `opts` - Which kinds of query have to have done something to count as succeeding
    pub fn succeeded(&self, opts: QuerySuceededOpts) -> Result<(), Errors> {
        // check if this query failed or not
        let kind = match &self.data {
            // a failure short circuits every option, and answers with what the server said
            ArchivedResponseAction::Error(error) => {
                return Err(Errors::Server {
                    query_id: Some(self.id),
                    index: Some(self.index.to_native() as usize),
                    code: error.code(),
                    msg: error.msg().to_owned(),
                })
            }
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
            ArchivedResponseAction::Error(_) => ResponseActionNames::Error,
        }
    }

    /// Get the failure this query answered with, if it failed
    ///
    /// Returns `Some` only for a query that failed, so a caller can tell a failure from a result
    /// without going through [`ArchivedResponse::succeeded`] and its options.
    pub fn error(&self) -> Option<&ArchivedResponseError> {
        match &self.data {
            ArchivedResponseAction::Error(error) => Some(error),
            _ => None,
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

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{
        ArchivedResponse, ErrorCode, QuerySuceededOpts, Response, ResponseAction, ResponseError,
    };
    use crate::client::Errors;

    /// Archive a response so the checks that only exist on the archived side can be run against it
    ///
    /// # Arguments
    ///
    /// * `data` - The answer to wrap in a response and archive
    fn archived(data: ResponseAction<u64>) -> rkyv::util::AlignedVec {
        // build a response around this answer, at an index that is not zero so a lost index shows
        let response = Response {
            id: Uuid::from_u128(0x1234),
            index: 3,
            data,
            end: false,
        };
        rkyv::to_bytes::<rkyv::rancor::Error>(&response).unwrap()
    }

    /// A share that failed takes over the whole answer
    ///
    /// This is the one place rows from three shards can hide a fourth shard's failure. A merge
    /// that let the rows win would hand the client a partial answer with nothing to say it was
    /// partial, which is the defect the error channel exists to remove.
    #[test]
    fn an_error_share_wins_a_merge() {
        // a share that found rows, merged with a share that failed
        let mut ours = ResponseAction::Get(Some(vec![1u64, 2, 3]));
        ours.merge(ResponseAction::Error(ResponseError::new(
            ErrorCode::StorageRead,
            "could not read partition 7",
        )));
        assert!(matches!(ours, ResponseAction::Error(_)));
        // and the other way round, so the answer stays failed however the shares arrive
        let mut ours = ResponseAction::Error(ResponseError::new(
            ErrorCode::StorageRead,
            "could not read partition 7",
        ));
        ours.merge(ResponseAction::Get(Some(vec![1u64, 2, 3])));
        assert!(matches!(ours, ResponseAction::Error(_)));
        // an exists is merged by the same rule, since it is the other query that can be split
        let mut ours = ResponseAction::<u64>::Exists(true);
        ours.merge(ResponseAction::Error(ResponseError::new(
            ErrorCode::ArchiveMissing,
            "archive is gone",
        )));
        assert!(matches!(ours, ResponseAction::Error(_)));
    }

    /// A query that failed is never a success, whatever the options say
    ///
    /// The options say which outcomes count as success. A failure is not an outcome, so a caller
    /// that turned every check off must still be told that its query did not work.
    #[test]
    fn an_error_response_never_succeeds_whatever_the_opts_say() {
        // archive a response that failed
        let buff = archived(ResponseAction::Error(ResponseError::new(
            ErrorCode::CorruptArchive,
            "partition 7 of TestRecord could not be read",
        )));
        let response = rkyv::access::<ArchivedResponse<u64>, rkyv::rancor::Error>(&buff).unwrap();
        // turn every check off, which is the most permissive thing a caller can ask for
        let permissive = QuerySuceededOpts {
            insert: false,
            update: false,
            get: false,
            delete: false,
            exists: false,
        };
        // it still fails, and it fails with what the server said rather than with a kind name
        match response.succeeded(permissive) {
            Err(Errors::Server { code, msg, index, .. }) => {
                assert_eq!(code, ErrorCode::CorruptArchive);
                assert!(msg.contains("partition 7"));
                assert_eq!(index, Some(3));
            }
            other => panic!("a failed query was not reported as a failure: {other:?}"),
        }
        // and the same response says so through the accessor a caller can ask directly
        assert_eq!(
            response.error().map(super::ArchivedResponseError::code),
            Some(ErrorCode::CorruptArchive)
        );
        // while a get that merely found nothing is still the other kind of answer
        let buff = archived(ResponseAction::Get(None));
        let response = rkyv::access::<ArchivedResponse<u64>, rkyv::rancor::Error>(&buff).unwrap();
        assert!(response.error().is_none());
        assert!(response.succeeded(permissive).is_ok());
    }

    /// A limit trims rows and leaves a failure alone
    #[test]
    fn a_truncate_leaves_an_error_alone() {
        // a failure has no rows to trim, so a limit cannot turn it into an empty get
        let mut failed = ResponseAction::<u64>::Error(ResponseError::new(
            ErrorCode::StorageRead,
            "could not read partition 7",
        ));
        failed.truncate(0);
        assert!(matches!(failed, ResponseAction::Error(_)));
        // while a get really is trimmed, so this test is not passing by doing nothing
        let mut rows = ResponseAction::Get(Some(vec![1u64, 2, 3]));
        rows.truncate(2);
        assert!(matches!(rows, ResponseAction::Get(Some(ref rows)) if rows.len() == 2));
    }
}
