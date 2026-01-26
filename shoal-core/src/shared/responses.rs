//! A response from a set of queries
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    client::{Errors, QuerySuceededOpts},
    shared::traits::ShoalSortedTable,
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
