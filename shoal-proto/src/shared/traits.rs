//! The root traits that shoal is built upon that are shared between the client and server

use deepsize2::DeepSizeOf;
use rkyv::de::Pool;
use rkyv::rancor::{Error, Strategy};
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Serialize};
use tracing::instrument;
use uuid::Uuid;

mod sorted;
mod unsorted;

use crate::client::{Errors, QuerySuceededOpts, ShqlParseError};
use crate::shared::queries::parser::{FieldInfo, FieldRole, TypeValidator};
use crate::shared::responses::{ArchivedResponseError, ResponseActionNames};

pub use sorted::ShoalSortedTable;
pub use unsorted::ShoalUnsortedTable;

/// Marker trait for Exists queries
///
/// This trait is used to constrain the `exists` method on the Shoal client
/// to only accept Exists queries at compile time, preventing incorrect usage.
pub trait ExistsQuery {}

impl RkyvSupport for String {}

pub trait RkyvSupport: Archive
    + for<'a> Serialize<Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>>
    + Sized
{
    /// Archive this type to an aligned vec
    fn serialize(&self) -> AlignedVec {
        rkyv::to_bytes::<Error>(self).unwrap()
    }

    /// Load our archived type from a slice
    ///
    /// # Arguments
    ///
    /// * `raw` - The raw bytes to load an archive from
    #[instrument(name = "RkyvSupport::access", skip_all, err(Debug))]
    fn access(raw: &[u8]) -> Result<&<Self as Archive>::Archived, rkyv::rancor::Error>
    where
        for<'a> <Self as Archive>::Archived: rkyv::bytecheck::CheckBytes<
            Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'a>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
    {
        // load an archived type from a slice
        rkyv::access::<<Self as Archive>::Archived, rkyv::rancor::Error>(raw)
    }

    /// Deserialize our archived type
    fn deserialize(archived: &<Self as Archive>::Archived) -> Result<Self, rkyv::rancor::Error>
    where
        <Self as Archive>::Archived: rkyv::Deserialize<Self, Strategy<Pool, rkyv::rancor::Error>>,
    {
        rkyv::deserialize::<Self, rkyv::rancor::Error>(archived)
    }
}

/// The traits for queries in shoal
pub trait ShoalQuerySupport: std::fmt::Debug + RkyvSupport + Sized + Send + Clone {
    /// Deserialize our response types
    ///
    /// # Arguments
    ///
    /// * `buff` - The buffer to deserialize into a response
    fn response_query_id(buff: &[u8]) -> Result<&Uuid, rkyv::rancor::Error>;

    /// Get the most rows this query asked for, if it set a limit
    ///
    /// A query split across shards has its limit applied on each of them as they scan,
    /// so this is what the shard collecting those shares trims their union back down
    /// to.
    fn limit(&self) -> Option<usize>;

    /// Get the partitions this query named, in the order it named them
    ///
    /// The rows of a get come back in this order, so the shard collecting the shares of a
    /// split query uses it to put them back together. A query that returns no rows names no
    /// partitions here.
    fn partition_keys(&self) -> &[u64];
}

/// The traits ror responses from shoal
pub trait ShoalResponseSupport: std::fmt::Debug + RkyvSupport + Sized + Send {
    /// Get the index of a single [`<Self::ResponseKinds as Archive>::Archived`]
    fn get_index_archived(archived: &<Self as Archive>::Archived) -> usize;

    /// Get whether this is the last response in a response stream
    fn is_end_of_stream(archived: &<Self as Archive>::Archived) -> bool;

    /// Get the query id from the response
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived type to get our query id from
    fn get_query_id(archived: &<Self as Archive>::Archived) -> Uuid;

    /// Merge another shards share of one queries answer into this one
    ///
    /// Both shares answer the same query, so they are always the same variant. A share
    /// of a different table's response would mean a query was routed to two different
    /// tables, which cannot happen, so it is ignored rather than guessed at.
    ///
    /// # Arguments
    ///
    /// * `other` - The other shards share of this queries answer
    fn merge(&mut self, other: Self);

    /// Put our rows back into the order the query named their partitions in
    ///
    /// Shares are merged in the order they arrive, which is whichever shard answered first.
    /// This is what makes the answer to a split query depend only on the query, and it has to
    /// run before the limit is applied or the rows kept would be the wrong ones.
    ///
    /// # Arguments
    ///
    /// * `order` - The partitions this query named, in the order it named them
    fn order_by_partitions(&mut self, order: &[u64]);

    /// Drop any rows past this queries limit
    ///
    /// # Arguments
    ///
    /// * `limit` - The most rows this query asked for
    fn truncate(&mut self, limit: usize);
}

pub trait QuerySupport: 'static + Sized {
    /// A hash over every part of this database's schema that can reach the wire
    ///
    /// The two peers exchange this when a connection opens and refuse each other if it differs.
    /// It lives on this trait rather than on `ShoalDatabase` because this is the only trait both
    /// halves see: the client is generic over it, and the server reaches the identical constant
    /// through `<D as ShoalDatabase>::ClientType`.
    ///
    /// The strongest compile time guarantee available to this system is a runtime handshake
    /// field. No amount of client side typing helps when the peer was built from a different
    /// schema, because both sides are individually consistent and only their agreement is wrong.
    const SCHEMA_FINGERPRINT: u64;

    /// The different tables or types of queries we will handle
    type QueryKinds: ShoalQuerySupport;

    /// The different tables we can get responses from
    type ResponseKinds: ShoalResponseSupport;

    /// The different tables in this database
    type TableNames: TableNameSupport;

    /// Make sure queries have succeeded based on some critiera
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived query to check
    /// * `opts` - The options to use when validating query responses
    fn succeeded(
        archived: &<Self::ResponseKinds as Archive>::Archived,
        opts: QuerySuceededOpts,
    ) -> Result<(), Errors>;

    /// Get the kind of query this is a response to
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived query to get the query kind for
    fn kind(archived: &<Self::ResponseKinds as Archive>::Archived) -> ResponseActionNames;

    /// Get the exists result from an Exists response
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived response to get the exists result from
    fn get_exists(archived: &<Self::ResponseKinds as Archive>::Archived) -> Option<bool>;

    /// Get the failure this query answered with, if it failed
    ///
    /// This is separate from [`QuerySupport::succeeded`] on purpose. `succeeded` answers "may I
    /// use this response", which depends on what the caller asked for, while this answers "did
    /// this query work", which does not.
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived response to get the failure from
    fn error(
        archived: &<Self::ResponseKinds as Archive>::Archived,
    ) -> Option<&ArchivedResponseError>;

    /// Parse a SHQL (Shoal Query Language) string into a query
    ///
    /// SHQL supports SQL-like SELECT queries for reading data from tables.
    /// Currently only GET queries are supported.
    ///
    /// # Syntax
    ///
    /// ```text
    /// SELECT * FROM <table_name> WHERE <partition_key> = '<value>';
    /// ```
    ///
    /// # Arguments
    ///
    /// * `query` - The SHQL query string to parse
    fn parse(query: &str) -> Result<Self::QueryKinds, ShqlParseError>;

    /// Get the names of every table in this database
    ///
    /// These are the names that can follow `FROM` in a query.
    fn table_names() -> &'static [&'static str];

    /// Get the name of every projection in this database, paired with the table it projects
    ///
    /// These are the names that can stand in place of the `*` in a query. They are not scoped
    /// to a table here because the projection is parsed before the `FROM` clause that names
    /// the table, so completion has to offer all of them and binding is what rejects a
    /// projection of the wrong table.
    fn projection_names() -> &'static [(&'static str, &'static str)];

    /// Get the fields for a table and the role each one plays in a query
    ///
    /// # Arguments
    ///
    /// * `table` - The name of the table to get fields for
    fn table_fields(table: &str) -> Option<Vec<FieldInfo>>;

    /// Get the type validator for a single field in a table
    ///
    /// # Arguments
    ///
    /// * `table` - The name of the table this field is in
    /// * `field` - The name of the field to get a validator for
    fn table_field_validator(table: &str, field: &str) -> Option<TypeValidator>;

    /// Get the table name from a query
    ///
    /// # Arguments
    ///
    /// * `query` - The query to get the table name for
    fn query_table_name(query: &Self::QueryKinds) -> Self::TableNames;

    /// Get the table name from an archived response
    ///
    /// # Arguments
    ///
    /// * `archived` - The archived response to get the table name for
    fn response_table_name(
        archived: &<Self::ResponseKinds as Archive>::Archived,
    ) -> Self::TableNames;

    /// Format an archived response into column headers and row values
    ///
    /// Returns `Some((headers, rows))` for Get responses with data,
    /// `None` for non-Get responses or empty results.
    fn format_response(
        archived: &<Self::ResponseKinds as Archive>::Archived,
    ) -> Option<(Vec<&'static str>, Vec<Vec<String>>)>;
}

pub trait TableNameSupport:
    std::fmt::Display + std::fmt::Debug + PartialEq + Eq + Ord + std::hash::Hash + Clone + Copy + Send
{
}

pub trait PartitionKeySupport: std::fmt::Debug + Clone + RkyvSupport + Sized {
    /// The partition key type for this data
    type PartitionKey;

    /// The name of this table
    fn name() -> &'static str;

    /// Get this rows partition key
    fn get_partition_key(&self) -> u64;

    /// Calculate the partition key for this row for this rows sort key
    ///
    /// # Arguments
    ///
    /// * `sort` - The sort key to build our partition key from
    fn get_partition_key_from_values(sort: &Self::PartitionKey) -> u64;

    /// Get the partition key for this row from an archived value
    fn get_partition_key_from_archived_insert(intent: &<Self as Archive>::Archived) -> u64;
}

/// Schema information for a table
pub trait TableSchemaSupport {
    /// A hash over this table's name, fields, types and the roles they play in a query
    ///
    /// This is one input to the `SCHEMA_FINGERPRINT` the two peers exchange when a connection
    /// opens. It is required rather than defaulted on purpose: a default would let a hand written
    /// implementation silently opt out of the only check that catches a peer built from a
    /// different but structurally similar schema.
    const SCHEMA_FINGERPRINT: u64;

    /// Get the type validator for a given field name
    fn get_field_validator(field_name: &str) -> Option<TypeValidator>;

    /// Get the role of a field (partition, sort, or filter)
    fn get_field_role(field_name: &str) -> Option<FieldRole>;

    /// Get all valid field names
    fn field_names() -> Vec<&'static str>;

    /// Get every field in this table paired with the role it plays in a query
    ///
    /// Fields with no role cannot be used in a where clause, so they come back with a role of
    /// `None` rather than being skipped. Callers that are building a list of usable fields
    /// should filter those out.
    fn fields() -> Vec<FieldInfo> {
        // pair each of our field names with its role
        Self::field_names()
            .into_iter()
            .map(|name| FieldInfo {
                name,
                role: Self::get_field_role(name),
            })
            .collect()
    }
}

/// Support for formatting table rows as displayable strings
///
/// This trait is implemented for archived table types to enable
/// formatting query results as ASCII tables.
pub trait TableRowFormat {
    /// Get the column headers for this table
    fn headers() -> Vec<&'static str>;

    /// Convert this row's field values to strings for display
    fn row_values(&self) -> Vec<String>;
}

/// The core traits that all tables require
pub trait ShoalTableSupport:
    std::fmt::Debug
    + Clone
    + RkyvSupport
    + PartitionKeySupport
    + Sized
    + DeepSizeOf
    + TableSchemaSupport
{
    /// The updates that can be applied to this table
    type Update: RkyvSupport + std::fmt::Debug + Clone;

    /// The server facing updates that can be applied to this table (just the updates no keys)
    type UpdateData: RkyvSupport + std::fmt::Debug + Clone;

    /// Any filters to apply when listing/crawling rows
    type Filters: rkyv::Archive + std::fmt::Debug + Clone;

    /// The subsets of this tables rows that a get can ask to be answered with
    ///
    /// This is a unit enum naming every projection declared for this table, plus the whole
    /// row itself. It is what a get carries over the wire, and what the database matches on
    /// to pick the row type a scan builds. Its default is the whole row, so a get that never
    /// names a projection behaves exactly as it did before projections existed.
    type Projection: RkyvSupport + std::fmt::Debug + Clone + Copy + Default + PartialEq + Eq + Send;

    /// Determine if a row should be filtered
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters to apply
    /// * `row` - The row to filter
    fn is_filtered(filter: &Self::Filters, row: &Self) -> bool;

    /// Determine if a row should be filtered
    ///
    /// # Arguments
    ///
    /// * `filters` - The filters to apply
    /// * `row` - The row to filter
    fn is_filtered_archived(
        filter: &Self::Filters,
        row: &<Self as rkyv::Archive>::Archived,
    ) -> bool;
}

/// A subset of a tables row that a get can be answered with instead of the whole row
///
/// A projection is built from a row rather than read from one, so the two ways a partition can
/// be held each get their own constructor: a resident partition holds rows, and a partition
/// still in the archive it was read from holds their archived forms. Building from the archived
/// form is the reason projections are worth having at all, since it copies only the fields the
/// projection named instead of every field the row has.
///
/// A whole row is the identity projection of itself, which is what keeps a projected get and an
/// unprojected one the same code path. `from_row` on a row is a clone and `from_archived` on a
/// row is the deserialize a get has always done, so the unprojected path costs what it always
/// did once these are inlined.
///
/// A projection has to carry its rows partition key, because the shard collecting the shares of
/// a split get puts the rows back in the order the query named their partitions in, and it asks
/// each row which partition it came from to do it.
pub trait ShoalProjection:
    std::fmt::Debug + Clone + RkyvSupport + PartitionKeySupport + Sized + Send + 'static
{
    /// The table whose rows this projects
    type Row: ShoalTableSupport;

    /// A hash over this projection's name, fields, types and order
    ///
    /// A projection reaches the wire because each one adds a variant to the generated response
    /// kinds enum, and an archived enum's discriminants are wire state. So a projection that was
    /// added, removed or reshaped has to move the database's fingerprint the same way a table
    /// does.
    const SCHEMA_FINGERPRINT: u64;

    /// Which of its tables projections this type is
    ///
    /// This is what a get carries over the wire, and what the database matches on to pick this
    /// type back up when the query reaches the shard that answers it.
    const PROJECTION: <Self::Row as ShoalTableSupport>::Projection;

    /// Build this projection from a resident row
    ///
    /// # Arguments
    ///
    /// * `row` - The row to project
    fn from_row(row: &Self::Row) -> Self;

    /// Build this projection from a row that is still in the archive it was read from
    ///
    /// Only the fields this projection named are read out of the archive, so a projection of a
    /// wide row skips deserializing every field it left out.
    ///
    /// # Arguments
    ///
    /// * `row` - The archived row to project
    fn from_archived(row: &<Self::Row as Archive>::Archived) -> Self;
}
