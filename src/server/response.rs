//! The responses to queries

use std::marker::PhantomData;

use rkyv::AlignedVec;

use crate::ShoalRow;

pub struct Responses<'a, R: ShoalRow> {
    /// The responses from our queries
    pub data: Vec<Option<Vec<&'a AlignedVec>>>,
    /// The type we are returning
    return_type: PhantomData<R>,
}

impl<'a, R: ShoalRow> Responses<'a, R> {
    /// Set the correct capacity for this new responses object
    ///
    /// # Arguments
    ///
    /// * `capacity` - The number of responses we expect to return
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Responses {
            data: Vec::with_capacity(capacity),
            return_type: PhantomData::default(),
        }
    }
}
