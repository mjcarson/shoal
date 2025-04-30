//! The types needed for querying the database

use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

mod sorted;
mod unsorted;

use super::traits::{QuerySupport, RkyvSupport};

pub use sorted::*;
pub use unsorted::*;

/// A bundle of different query kinds
#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct Queries<S: QuerySupport> {
    /// The id for this query
    pub id: Uuid,
    /// The individual queries to execute
    pub queries: Vec<S::QueryKinds>,
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
}

impl<S: QuerySupport> Default for Queries<S> {
    fn default() -> Self {
        Queries {
            id: Uuid::new_v4(),
            queries: Vec::default(),
        }
    }
}

impl<S: QuerySupport> RkyvSupport for Queries<S> {}
