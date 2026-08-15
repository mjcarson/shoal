//! What a client needs to read a response, independent of how it read the bytes
//!
//! These types are named by `QuerySupport` and by `shared::responses`, so they cannot live in
//! the client crate without making the protocol depend on it. Nothing here touches a socket or
//! a runtime.

use rkyv::Archive;
use rkyv::option::ArchivedOption;
use rkyv::vec::ArchivedVec;

use crate::shared::traits::QuerySupport;

mod errors;

pub use errors::{ChannelError, ConnectError, Errors, ShqlParseError};

/// Allow types to be retrieved from a [`ShoalStream`]
pub trait FromShoal<S: QuerySupport>: Sized + Archive {
    /// The response kinds to deserialize from
    type ResponseKinds: std::fmt::Debug;

    /// Retrieve a type from a [`ShoalStream`]
    ///
    /// # Arguments
    ///
    /// * `kind` - The response kind to try to cast
    fn retrieve(
        archived: &<S::ResponseKinds as Archive>::Archived,
    ) -> Result<&ArchivedOption<ArchivedVec<<Self as Archive>::Archived>>, Errors>;
}

/// The options for determining if a query suceeded or not
///
/// This will default to requiring every kind of query to succeed
#[derive(Debug, Archive, Clone, Copy)]
pub struct QuerySuceededOpts {
    /// Whether to check if inserts actually inserted data
    pub insert: bool,
    /// Whether to check if updates actually inserted data
    pub update: bool,
    /// Whether to check if gets actually goted data
    pub get: bool,
    /// Whether to check if deletes actually deleted data
    pub delete: bool,
    /// Whether to check if exists actually found data
    pub exists: bool,
}

impl Default for QuerySuceededOpts {
    /// Default to requiring all queries to have actaully inserted data
    fn default() -> Self {
        QuerySuceededOpts {
            insert: true,
            update: true,
            get: true,
            delete: true,
            exists: true,
        }
    }
}
