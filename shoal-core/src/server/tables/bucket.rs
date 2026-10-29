//! A bucket: the two tables its object metadata lives in
//!
//! A schema declares a bucket as a field, `posters: Bucket<Posters>`, and `#[shoal::db]` rewrites
//! that field to `Bucket<Posters, Media, MediaTableNames>` and generates the two rows the bucket's
//! tables hold ([S2](../../../../docs/src/object-storage/buckets.md),
//! [F79](../../../../docs/src/features/bucket-schema.md)). This is the type the field holds on a
//! server: the bucket's `ObjectMeta` and `StripeMeta` tables, as ordinary persistent unsorted
//! tables under the ordinary storage engine, which the cluster replicates as it replicates any
//! table.
//!
//! # Invariants
//!
//! **A bucket holds its tables and does nothing with them.** The generated `ShoalDatabase` impl
//! reaches each table through its field, `self.posters.objects`, with the row types concrete, so
//! this type forwards nothing and repeats none of the bounds a table's impl carries.
//!
//! **A bucket has no `Drop`.** The generated `shutdown` moves each table out of its bucket to shut
//! it down, which a `Drop` impl would forbid. M14's handle to the node's device store is held here
//! too, and shuts down the same way.

use crate::server::database::ShoalDatabase;
use crate::server::tables::persistent::PersistentUnsortedTable;
use crate::server::tables::storage::FileSystem;
use crate::shared::objects::BucketRows;
use crate::shared::traits::TableNameSupport;

/// A bucket's tables, held by the field of the schema that declares it
///
/// # Examples
///
/// ```ignore
/// /// Names the bucket our posters are kept in
/// pub struct Posters;
///
/// /// The database our media service is built on
/// #[shoal::db]
/// pub struct Media {
///     /// The movies we know about
///     pub movies: PersistentUnsortedTable<Movie, FileSystem>,
///     /// The posters for those movies
///     pub posters: Bucket<Posters>,
/// }
/// ```
pub struct Bucket<M: BucketRows, D: ShoalDatabase, N: TableNameSupport> {
    /// The bucket's object metadata: an entry for every object, keyed by its path's hash
    pub objects: PersistentUnsortedTable<M::ObjectMeta, FileSystem<D>, N>,
    /// The bucket's stripe metadata: a row for every stripe written in place
    pub stripes: PersistentUnsortedTable<M::StripeMeta, FileSystem<D>, N>,
}

impl<M: BucketRows, D: ShoalDatabase, N: TableNameSupport> Bucket<M, D, N> {
    /// Assemble a bucket from its two tables, each already built
    ///
    /// The generated `ShoalDatabase::new` builds each table as it builds any other and hands them
    /// here, so a bucket's tables start, recover and load exactly as a declared table's do.
    ///
    /// # Arguments
    ///
    /// * `objects` - The bucket's object metadata table
    /// * `stripes` - The bucket's stripe metadata table
    pub fn assemble(
        objects: PersistentUnsortedTable<M::ObjectMeta, FileSystem<D>, N>,
        stripes: PersistentUnsortedTable<M::StripeMeta, FileSystem<D>, N>,
    ) -> Self {
        Bucket { objects, stripes }
    }
}
