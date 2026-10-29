//! What an object arm drives: one endpoint of an object store ([F94](../../../docs/src/features/bench-object-endpoints.md))
//!
//! An endpoint is a Shoal member's client or another store's S3 endpoint. The driver spreads its
//! streams over its endpoints round robin and times every operation around one call of this trait,
//! so a Shoal capture and a capture of another store are made by the same loop, the same bytes and
//! the same windows. Only the call differs.
//!
//! # Invariants
//!
//! **An endpoint never retries on the driver's behalf** past what its own client does unasked: a
//! failure is reported once, with its code, where it happened. **An endpoint never names a
//! schema**: a dataset's bucket is reached by its name.

use std::future::Future;

use bytes::Bytes;

use super::ObjectDataset;
use crate::spec::ReadLevel;

/// How an endpoint's failure is read by the driver
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Nothing is at the path: a miss for a read, a lost object for a read back
    NotFound,
    /// The bytes did not hold the checksum they were answered with, so they were misread on the way
    Corrupt,
    /// A failure the store says to try again, such as an S3 `SlowDown`; counted as a failure,
    /// since the driver retries nothing
    Retriable,
    /// Anything else
    Other,
}

/// One failed operation, as an endpoint reports it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointError {
    /// How the driver reads it
    pub kind: ErrorKind,
    /// The code it is counted under in a window
    pub code: String,
    /// What it said, which the driver cuts to a line
    pub message: String,
}

impl EndpointError {
    /// A failure of some kind, by its code and what it said
    ///
    /// # Arguments
    ///
    /// * `kind` - How the driver reads it
    /// * `code` - The code it is counted under
    /// * `message` - What it said
    #[must_use]
    pub fn new(kind: ErrorKind, code: impl Into<String>, message: impl Into<String>) -> Self {
        EndpointError {
            kind,
            code: code.into(),
            message: message.into(),
        }
    }

    /// The refusal of an operation an endpoint does not have, by its name
    ///
    /// # Arguments
    ///
    /// * `operation` - The operation
    #[must_use]
    pub fn unsupported(operation: &str) -> Self {
        EndpointError::new(
            ErrorKind::Other,
            "Unsupported",
            format!("this endpoint has no {operation}"),
        )
    }

    /// The refusal of a read at a level an endpoint has no way to ask for
    ///
    /// # Arguments
    ///
    /// * `level` - The level asked for
    #[must_use]
    pub fn no_level(level: ReadLevel) -> Self {
        EndpointError::unsupported(&format!("read level ({level:?})"))
    }
}

/// One endpoint of the store an object arm measures
pub trait ObjectEndpoint: Send + Sync + 'static {
    /// Whether it writes part of an object in place, which no S3 operation does
    fn writes_in_place(&self) -> bool;

    /// Put a whole object
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    /// * `bytes` - Its bytes, which the caller keeps until it has taken the time
    fn put(
        &self,
        dataset: &ObjectDataset,
        path: &str,
        bytes: Bytes,
    ) -> impl Future<Output = Result<(), EndpointError>> + Send;

    /// Read a whole object
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    /// * `level` - The level it is read at; an endpoint with no levels refuses any but the
    ///   default by name ([F95](../../../docs/src/features/bench-object-read-levels.md))
    fn get(
        &self,
        dataset: &ObjectDataset,
        path: &str,
        level: ReadLevel,
    ) -> impl Future<Output = Result<Bytes, EndpointError>> + Send;

    /// Read what is known of an object without its bytes
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    /// * `level` - The level it is read at, as for a get
    fn stat(
        &self,
        dataset: &ObjectDataset,
        path: &str,
        level: ReadLevel,
    ) -> impl Future<Output = Result<(), EndpointError>> + Send;

    /// Delete an object, which the driver does only between arms
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    fn delete(
        &self,
        dataset: &ObjectDataset,
        path: &str,
    ) -> impl Future<Output = Result<(), EndpointError>> + Send;

    /// Write part of an object in place; an endpoint that cannot refuses by name
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    /// * `offset` - Where in it the bytes go
    /// * `bytes` - The bytes
    fn write_at(
        &self,
        dataset: &ObjectDataset,
        path: &str,
        offset: u64,
        bytes: Bytes,
    ) -> impl Future<Output = Result<(), EndpointError>> + Send {
        // nothing is sent: the refusal is the endpoint's own
        let _ = (dataset, path, offset, bytes);
        async { Err(EndpointError::unsupported("write in place")) }
    }
}
