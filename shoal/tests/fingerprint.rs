//! Tests for the schema fingerprint the two peers compare when a connection opens
//!
//! Every one of these is a compile time constant compared against another compile time constant,
//! so none of them needs a server and all of them run instantly. What they establish is that the
//! fingerprint actually moves when the schema moves — a constant that is the same for two
//! different schemas would pass the handshake and hand two mismatched peers to each other, which
//! is the exact failure the fingerprint exists to prevent.
//!
//! The schemas below are deliberately as close to each other as they can be while still being
//! different, because a fingerprint that only catches obviously different schemas catches nothing
//! worth catching. Two peers built from wildly different schemas fail on the first `bytecheck`;
//! two built from schemas that differ by a field order are the ones that corrupt silently.
//!
//! Each schema lives in a module of its own because `#[db]` mints a projection enum named after
//! each of its rows, so two schemas holding a row of the same name collide in one namespace. That
//! is also what lets `base` and `projected` declare a row that is identical down to its name: the
//! two are different types with the same shape, so their tables fingerprint the same and the only
//! thing left to tell their databases apart is the projection.

use shoal_core::shared::protocol::fingerprint;
use shoal_core::shared::traits::{QuerySupport, ShoalProjection, TableSchemaSupport};

/// A schema with one sorted table and nothing else
mod base {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal_core::tables::EphemeralSortedTable;
    use shoal_derive::{db, ShoalSortedTable};

    /// The row this schema's only table holds
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Wire")]
    pub struct Row {
        /// The partition this row belongs to
        #[shoal(partition)]
        pub partition_key: String,
        /// The key this row is sorted by within its partition
        #[shoal(sort)]
        pub sort_key: String,
        /// The payload
        #[shoal(update)]
        pub data: String,
    }

    /// The schema every other one in this file is compared against
    #[db]
    pub struct Wire {
        /// The only table in this schema
        pub rows: EphemeralSortedTable<Row>,
    }
}

/// The same schema, with one field added to its row
mod added_field {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal_core::tables::EphemeralSortedTable;
    use shoal_derive::{db, ShoalSortedTable};

    /// The row this schema's only table holds, which has a field the base one does not
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Wire")]
    pub struct Row {
        /// The partition this row belongs to
        #[shoal(partition)]
        pub partition_key: String,
        /// The key this row is sorted by within its partition
        #[shoal(sort)]
        pub sort_key: String,
        /// The payload
        #[shoal(update)]
        pub data: String,
        /// A field the base schema does not have at all
        #[shoal(filter)]
        pub extra: String,
    }

    /// A schema whose row gained a field
    #[db]
    pub struct Wire {
        /// The only table in this schema
        pub rows: EphemeralSortedTable<Row>,
    }
}

/// The same schema, with its row's fields in a different order
mod reordered {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal_core::tables::EphemeralSortedTable;
    use shoal_derive::{db, ShoalSortedTable};

    /// The base row's fields, declared in a different order
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Wire")]
    pub struct Row {
        /// The key this row is sorted by within its partition
        #[shoal(sort)]
        pub sort_key: String,
        /// The partition this row belongs to
        #[shoal(partition)]
        pub partition_key: String,
        /// The payload
        #[shoal(update)]
        pub data: String,
    }

    /// A schema whose row was reordered
    #[db]
    pub struct Wire {
        /// The only table in this schema
        pub rows: EphemeralSortedTable<Row>,
    }
}

/// The base schema again, with a projection declared on its table
mod projected {
    use deepsize2::DeepSizeOf;
    use rkyv::{Archive, Deserialize, Serialize};
    use shoal_core::tables::EphemeralSortedTable;
    use shoal_derive::{db, ShoalProjection, ShoalSortedTable};

    /// The base row, declared identically down to its name
    #[derive(
        Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
    )]
    #[rkyv(derive(Debug))]
    #[shoal_table(db = "Wire")]
    pub struct Row {
        /// The partition this row belongs to
        #[shoal(partition)]
        pub partition_key: String,
        /// The key this row is sorted by within its partition
        #[shoal(sort)]
        pub sort_key: String,
        /// The payload
        #[shoal(update)]
        pub data: String,
    }

    /// A projection of that row which leaves its payload behind
    #[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalProjection, PartialEq, Eq)]
    #[rkyv(derive(Debug))]
    #[shoal_projection(table = "Row")]
    pub struct RowKeys {
        /// The partition this row belonged to
        #[shoal(partition)]
        pub partition_key: String,
        /// The key this row was sorted by within its partition
        pub sort_key: String,
    }

    /// A schema identical to the base one except that its table answers a projection
    #[db]
    pub struct Wire {
        /// The only table in this schema
        #[shoal(projections(RowKeys))]
        pub rows: EphemeralSortedTable<Row>,
    }
}

/// A schema is fingerprinted as something other than the value it started from
///
/// A constant that came out as the seed, or as zero, would mean the fold never ran, and every
/// schema in the world would agree with every other one.
#[test]
fn a_fingerprint_is_not_the_value_it_started_from() {
    // walk every schema in this file
    for schema in [
        base::WireClient::SCHEMA_FINGERPRINT,
        added_field::WireClient::SCHEMA_FINGERPRINT,
        reordered::WireClient::SCHEMA_FINGERPRINT,
        projected::WireClient::SCHEMA_FINGERPRINT,
    ] {
        assert_ne!(schema, 0, "a schema fingerprinted as zero");
        assert_ne!(
            schema,
            fingerprint::SEED,
            "a schema fingerprinted as the seed, so nothing was folded in"
        );
    }
}

/// A row with one more field fingerprints differently
#[test]
fn an_added_field_changes_the_schema_fingerprint() {
    // the two rows differ only in that the second has a field the first does not
    assert_ne!(
        <base::Row as TableSchemaSupport>::SCHEMA_FINGERPRINT,
        <added_field::Row as TableSchemaSupport>::SCHEMA_FINGERPRINT,
        "a row that gained a field fingerprinted the same"
    );
    // and the schemas holding them differ for the same reason
    assert_ne!(
        base::WireClient::SCHEMA_FINGERPRINT,
        added_field::WireClient::SCHEMA_FINGERPRINT,
        "a schema whose row gained a field fingerprinted the same"
    );
}

/// The same fields in a different order fingerprint differently
///
/// This is the case that motivates the whole exchange. Both peers archive the same field names and
/// the same types, so nothing about either archive is malformed and `bytecheck` has nothing to
/// object to — the bytes are simply read back as the wrong fields.
#[test]
fn a_reordered_row_changes_the_schema_fingerprint() {
    assert_ne!(
        <base::Row as TableSchemaSupport>::SCHEMA_FINGERPRINT,
        <reordered::Row as TableSchemaSupport>::SCHEMA_FINGERPRINT,
        "a row whose fields were reordered fingerprinted the same"
    );
    assert_ne!(
        base::WireClient::SCHEMA_FINGERPRINT,
        reordered::WireClient::SCHEMA_FINGERPRINT,
        "a schema whose row was reordered fingerprinted the same"
    );
}

/// A projection declared on a table changes the schema fingerprint
///
/// The two schemas hold a table that fingerprints identically, which is what makes this test about
/// the projection and nothing else. They still have to disagree, because a projection adds a
/// variant to the generated response kinds enum and an archived enum's discriminants are wire
/// state.
#[test]
fn a_declared_projection_changes_the_schema_fingerprint() {
    // the tables are the same down to their constants, so nothing below is about the row
    assert_eq!(
        <base::Row as TableSchemaSupport>::SCHEMA_FINGERPRINT,
        <projected::Row as TableSchemaSupport>::SCHEMA_FINGERPRINT,
        "two identically declared rows fingerprinted differently"
    );
    assert_ne!(
        base::WireClient::SCHEMA_FINGERPRINT,
        projected::WireClient::SCHEMA_FINGERPRINT,
        "declaring a projection left the schema fingerprint where it was"
    );
}

/// A whole row is fingerprinted as the table it is
///
/// Every row is the identity projection of itself, and that projection borrows its table's
/// constant rather than folding the same fields a second time. If the two ever disagreed, a row
/// and its own projection would describe different schemas.
#[test]
fn the_identity_projection_borrows_its_tables_fingerprint() {
    assert_eq!(
        <base::Row as ShoalProjection>::SCHEMA_FINGERPRINT,
        <base::Row as TableSchemaSupport>::SCHEMA_FINGERPRINT
    );
}

/// A projection of a row fingerprints as something other than the row
#[test]
fn a_projection_does_not_fingerprint_as_its_row() {
    assert_ne!(
        <projected::RowKeys as ShoalProjection>::SCHEMA_FINGERPRINT,
        <projected::Row as ShoalProjection>::SCHEMA_FINGERPRINT,
        "a projection that drops a field fingerprinted as the whole row"
    );
}
