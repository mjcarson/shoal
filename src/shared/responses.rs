//! A response from a set of queries
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

use crate::shared::traits::ShoalTable;

/// The different response kinds from a query
#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum ResponseKinds<D: ShoalTable> {
    /// Whether an insert was successful or not
    Insert(bool),
    /// The response from a get query
    Get(Option<Vec<D>>),
}

/// A response from a query
#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct Response<T: ShoalTable> {
    /// The id for the query we are responding too
    pub id: Uuid,
    /// This response index in the queries vec
    pub index: usize,
    /// The response data
    pub data: ResponseKinds<T>,
    /// Whether this is the last response for a query or not
    pub end: bool,
}

impl<T: ShoalTable> Response<T> {
    /// Mark this response as the last one
    pub fn end(&mut self) {
        self.end = true;
    }
}

/// The responses from a set of queries
#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct Responses<D: ShoalTable> {
    /// The responses for our queries
    pub responses: Vec<Response<D>>,
}

impl<D: ShoalTable> Responses<D> {
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
