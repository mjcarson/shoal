//! The different types of cursors in Shoal

use std::{collections::BTreeMap, marker::PhantomData};

use rkyv::AlignedVec;

/// A cursor for crawling data in this table
pub struct TableCursor<'c, R> {
    /// The current composite to start listing data at
    next: u64,
    /// The data to return in this cursor
    pub data: &'c BTreeMap<u64, AlignedVec>,
    /// The type of rows we are storing
    row_type: PhantomData<R>,
}

impl<'c, R> TableCursor<'c, R> {
    /// create a new table cursor
    pub fn new(data: &'c BTreeMap<u64, AlignedVec>) -> Option<Self> {
        // get our first key in this btreemap
        if let Some(next) = data.keys().next() {
            Some(TableCursor {
                next: *next,
                data,
                row_type: PhantomData::default(),
            })
        } else {
            None
        }
    }
}
