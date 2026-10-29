//! A Shoal member's client as an object endpoint ([F94](../../../docs/src/features/bench-object-endpoints.md))
//!
//! Each operation is the one call the driver made before F94, through the client's untyped
//! [`Objects`](shoal::client::Objects) of the dataset's bucket, and each failure is counted under
//! the code it was counted under then, so a capture of a Shoal cluster measures what it did.

use bytes::Bytes;
use shoal::client::{Errors, Shoal};
use shoal::shared::identity::BucketId;
use shoal::shared::protocol::object::ObjectOutcome;
use shoal::shared::traits::QuerySupport;

use super::endpoint::{EndpointError, ErrorKind, ObjectEndpoint};
use super::ObjectDataset;
use crate::spec::ReadLevel;

/// The identity of a dataset's bucket, from its marker's name
///
/// # Arguments
///
/// * `dataset` - The dataset
fn bucket_of(dataset: &ObjectDataset) -> BucketId {
    BucketId::of(&dataset.bucket)
}

/// The code an error is counted under
///
/// # Arguments
///
/// * `error` - The error
fn code_of(error: &Errors) -> String {
    match error {
        Errors::Server { code, .. } => format!("{code:?}"),
        Errors::Object { outcome, .. } => outcome.name().to_string(),
        other => format!("{other:?}").chars().take(40).collect(),
    }
}

/// A client's error as the driver reads it
///
/// # Arguments
///
/// * `error` - The error
fn failure(error: &Errors) -> EndpointError {
    // a path with nothing at it, and bytes that did not hold their checksum, are read apart
    let kind = match error {
        Errors::Object {
            outcome: ObjectOutcome::NotFound,
            ..
        } => ErrorKind::NotFound,
        Errors::Object {
            outcome: ObjectOutcome::ChecksumMismatch,
            ..
        } => ErrorKind::Corrupt,
        _ => ErrorKind::Other,
    };
    EndpointError::new(kind, code_of(error), format!("{error:?}"))
}

impl<S> ObjectEndpoint for Shoal<S>
where
    S: QuerySupport + Send + Sync + 'static,
{
    /// A Shoal object is written in place ([F86](../../../docs/src/features/replicated-pools.md))
    fn writes_in_place(&self) -> bool {
        true
    }

    /// Put a whole object through its bucket
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    /// * `bytes` - Its bytes
    async fn put(&self, dataset: &ObjectDataset, path: &str, bytes: Bytes) -> Result<(), EndpointError> {
        // what the object came to be is the cluster's business, not the bench's
        self.objects(bucket_of(dataset))
            .put(path, &bytes)
            .await
            .map(|_| ())
            .map_err(|error| failure(&error))
    }

    /// Read a whole object through its bucket, at a read level
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    /// * `level` - The level it is read at, the server's default unless the spec names one
    async fn get(&self, dataset: &ObjectDataset, path: &str, level: ReadLevel) -> Result<Bytes, EndpointError> {
        // the bytes the client checked against the checksum it was answered with, all of them
        self.objects(bucket_of(dataset))
            .read_range(path, 0, u64::MAX, level.wire())
            .await
            .map(|(_, bytes)| Bytes::from(bytes))
            .map_err(|error| failure(&error))
    }

    /// Stat an object through its bucket, at a read level
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    /// * `level` - The level it is read at, as for a get
    async fn stat(&self, dataset: &ObjectDataset, path: &str, level: ReadLevel) -> Result<(), EndpointError> {
        // the entry, without its bytes
        self.objects(bucket_of(dataset))
            .stat_at(path, level.wire())
            .await
            .map(|_| ())
            .map_err(|error| failure(&error))
    }

    /// Delete an object through its bucket
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    async fn delete(&self, dataset: &ObjectDataset, path: &str) -> Result<(), EndpointError> {
        // a path that held nothing is deleted all the same
        self.objects(bucket_of(dataset))
            .delete(path)
            .await
            .map(|_| ())
            .map_err(|error| failure(&error))
    }

    /// Write part of an object in place through its bucket
    ///
    /// # Arguments
    ///
    /// * `dataset` - The bucket's dataset
    /// * `path` - The object's path
    /// * `offset` - Where in it the bytes go
    /// * `bytes` - The bytes
    async fn write_at(&self, dataset: &ObjectDataset, path: &str, offset: u64, bytes: Bytes) -> Result<(), EndpointError> {
        // staged, committed and applied, or ridden in its commit under the pool's threshold
        self.objects(bucket_of(dataset))
            .write_at(path, offset, &bytes)
            .await
            .map(|_| ())
            .map_err(|error| failure(&error))
    }
}
