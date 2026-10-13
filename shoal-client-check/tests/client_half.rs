//! The client half of `#[shoal::db]` is complete, in a crate with no engine
//!
//! That this crate compiles at all is the structural half of the check. These are the
//! behavioural half: each one touches a different emission, so a `db(client)` that silently
//! skipped one more than it meant to cannot pass by producing nothing.
//!
//! None of it needs a server, and none of it can start one - `shoal::ShoalPool` does not exist
//! in this build.

use shoal::traits::{QuerySupport, RkyvSupport, ShoalQuerySupport};

use shoal_client_check::{CheckDbClient, CheckDbQueryKinds, Movie, Posters, Release};

/// A schema parses SHQL with no server anywhere in the process
///
/// Exercises the `parse` arm of the generated `QuerySupport` impl, which is the largest single
/// emission the client half keeps.
#[test]
fn a_schema_parses_shql_without_a_server() {
    // parse a get against the unsorted table
    let query = <CheckDbClient as QuerySupport>::parse("SELECT * FROM Movie WHERE id = 1;")
        .expect("a well formed select parses");
    // it came back as the variant naming the table it selected from
    assert!(matches!(query, CheckDbQueryKinds::Movie(_)));
}

/// A malformed query is refused rather than accepted
///
/// The error type is `ShqlParseError`, which lives in the protocol crate - a client that could
/// not name its own parse failures would not be much of a client.
#[test]
fn a_malformed_query_is_refused() {
    // a table nobody declared cannot be selected from
    let failed = <CheckDbClient as QuerySupport>::parse("SELECT * FROM Nonexistent WHERE id = 1;");
    assert!(failed.is_err(), "an unknown table parsed");
}

/// A query round trips through the wire format inside a crate with no runtime
///
/// Serialize, access and read the id back out, which is every step the client takes before a
/// socket is involved. Exercises `RkyvSupport` and `ShoalQuerySupport::response_query_id`'s
/// sibling machinery on the query side.
#[test]
fn a_query_round_trips_through_the_wire_format() {
    // build a query the same way the parser does
    let query = <CheckDbClient as QuerySupport>::parse("SELECT * FROM Movie WHERE id = 7;")
        .expect("a well formed select parses");
    // put it on the wire
    let bytes = RkyvSupport::serialize(&query).expect("a query this test built archives");
    assert!(!bytes.is_empty(), "a query serialized to nothing");
    // and read it back, which is a validating access rather than a cast - this is the step that
    // would fail if the archived layout and the reader disagreed across the new crate boundary
    <CheckDbQueryKinds as RkyvSupport>::access(&bytes)
        .expect("what we just serialized is readable");
    // and the query still knows which partition it asked for
    assert_eq!(query.partition_keys().len(), 1);
}

/// Every table and projection the schema declared is named
///
/// Exercises `table_names` and `projection_names`, and so the `TableNames` enum and the
/// projection enums, which are separate emissions from the client struct.
#[test]
fn every_table_and_projection_is_named() {
    // every table is named, in field order, a bucket's two in its place
    assert_eq!(
        <CheckDbClient as QuerySupport>::table_names(),
        &[
            "Movie",
            "MovieByKeyword",
            "Release",
            "PostersObjectMeta",
            "PostersStripeMeta"
        ]
    );
    // and the projection declared on the first of them is named against the table it projects
    let projections = <CheckDbClient as QuerySupport>::projection_names();
    assert!(
        projections.contains(&("MovieTitle", "Movie")),
        "the projection was not named: {projections:?}"
    );
}

/// The schema fingerprint is a real constant
///
/// Exercises `traits/fingerprint.rs`, which is the only emission that reaches into
/// `shoal::shared::protocol` - so this is what fails if the protocol paths are wrong in a client
/// build specifically.
#[test]
fn the_fingerprint_is_a_constant() {
    // a fingerprint of zero would mean nothing was mixed in
    assert_ne!(<CheckDbClient as QuerySupport>::SCHEMA_FINGERPRINT, 0);
}

/// The client half's schema id and fingerprint are frozen
///
/// The server half of a schema is checked by `shoal/tests/fingerprint.rs`; this is the same pin
/// taken from a crate that never sees the server half, so a change to how the client half walks
/// a schema is caught where it is built. Taken before the macro was refactored for buckets, which
/// moved neither value, and taken again when CheckDb declared its bucket, which moved both (F79).
#[test]
fn the_client_halfs_ids_are_frozen() {
    // the structural id two nodes compare
    assert_eq!(
        <CheckDbClient as QuerySupport>::SCHEMA_ID,
        13_871_431_005_877_877_605,
        "CheckDb's schema id moved"
    );
    // and the fingerprint a client's hello carries
    assert_eq!(
        <CheckDbClient as QuerySupport>::SCHEMA_FINGERPRINT,
        9_900_772_859_821_495_696,
        "CheckDb's fingerprint moved"
    );
}

/// A schema with a bucket compiles, generates its tables and ids, and needs no engine
///
/// S2's `client_half_with_a_bucket_links_no_engine`. That this crate builds at all with a bucket
/// in its schema is the structural half, and `cargo check -p shoal-client-check
/// --no-default-features` is the stronger form, since it links nothing a workspace build unifies
/// in. This is the behavioural half: the bucket's name and identity, its two generated tables and
/// theirs, and SHQL refusing both by name, all in a build with no engine
/// ([F79](../../docs/src/features/bucket-schema.md)).
#[test]
fn client_half_with_a_bucket_links_no_engine() {
    use shoal::shared::identity::{BucketId, TableId};
    use shoal::shared::objects::BucketRows;
    use shoal::shared::traits::{BucketNameSupport, TableNameSupport};
    use shoal_client_check::{CheckDbBuckets, CheckDbTableNames, PostersStripeMeta};
    // the bucket is named after its marker, and identified by the hash of that name
    assert!(<CheckDbClient as QuerySupport>::HAS_BUCKETS);
    assert_eq!(
        <CheckDbClient as QuerySupport>::bucket_names(),
        &["Posters"]
    );
    assert_eq!(
        <CheckDbClient as QuerySupport>::bucket_ids(),
        vec![("Posters", BucketId::of("Posters"))]
    );
    assert_eq!(<Posters as BucketRows>::NAME, "Posters");
    assert_eq!(<Posters as BucketRows>::id(), BucketId::of("Posters"));
    assert_eq!(CheckDbBuckets::Posters.bucket_id(), BucketId::of("Posters"));
    assert_eq!(CheckDbBuckets::Posters.to_string(), "Posters");
    // its two tables are generated, each identified as any table is, and each says its bucket
    assert_eq!(
        <CheckDbClient as QuerySupport>::generated_tables(),
        &[
            ("PostersObjectMeta", "Posters"),
            ("PostersStripeMeta", "Posters")
        ]
    );
    assert_eq!(
        CheckDbTableNames::PostersObjectMeta.table_id(),
        TableId::of("PostersObjectMeta")
    );
    assert_eq!(
        CheckDbTableNames::PostersStripeMeta.bucket(),
        Some("Posters")
    );
    assert_eq!(CheckDbTableNames::Movie.bucket(), None);
    // a grant can name the declared tables and the bucket, and never a generated table
    let served = <CheckDbClient as QuerySupport>::served();
    assert_eq!(served.tables, ["Movie", "MovieByKeyword", "Release"]);
    assert_eq!(served.buckets, ["Posters"]);
    // the generated rows are real rows: one hashes its own three field key
    use shoal::traits::PartitionKeySupport;
    let stripe = PostersStripeMeta {
        consumer: 1,
        object: 2,
        stripe: 3,
        sequence: 0,
        labels: Vec::new(),
        length: 0,
        stamp: 0,
        fence: 0,
        tombstone: false,
        pending: None,
    };
    assert_eq!(
        stripe.get_partition_key(),
        PostersStripeMeta::get_partition_key_from_values(&(1, 2, 3))
    );
    // and SHQL refuses both by name, saying they are the bucket's, never "unknown table"
    for table in ["PostersObjectMeta", "PostersStripeMeta"] {
        let refused = <CheckDbClient as QuerySupport>::parse(&format!(
            "SELECT * FROM {table} WHERE key = 1;"
        ))
        .expect_err("a query of a bucket's table parsed");
        let message = refused.to_string();
        assert!(
            message.contains("bucket Posters generated"),
            "{table} was refused as something else: {message}"
        );
    }
    // a bucket's tables are no benchmark's to load or drive
    use shoal::shared::dataset::DatasetSupport;
    let datasets: Vec<&str> = <CheckDbClient as DatasetSupport>::dataset_tables()
        .iter()
        .map(|(name, _)| *name)
        .collect();
    assert_eq!(datasets, ["Movie", "MovieByKeyword", "Release"]);
}

/// A row's fields can be described without a server
///
/// Exercises `TableSchemaSupport`, which is what the shql completion and the type validator are
/// built on, and which a TUI needs before it has connected to anything.
#[test]
fn a_tables_fields_are_described() {
    // the unsorted table describes the four fields it declared
    let fields = <CheckDbClient as QuerySupport>::table_fields("Movie")
        .expect("a declared table describes itself");
    let names: Vec<&str> = fields.iter().map(|field| field.name).collect();
    assert!(
        names.contains(&"id"),
        "the partition key is missing: {names:?}"
    );
    assert!(names.contains(&"title"), "a filter is missing: {names:?}");
    // and a table nobody declared describes nothing
    assert!(<CheckDbClient as QuerySupport>::table_fields("Nonexistent").is_none());
}

/// A row still hashes its own partition key
///
/// This is `PartitionKeySupport`, which is why `gxhash` cannot be gated behind the engine: a
/// client works out which partition a row belongs to without asking anybody.
#[test]
fn a_row_hashes_its_own_partition_key() {
    use shoal::traits::PartitionKeySupport;
    // two rows with the same partition key hash to the same partition
    let first = Movie {
        id: 42,
        title: "one".to_string(),
        watched: false,
        data: String::new(),
    };
    let second = Movie {
        id: 42,
        title: "another".to_string(),
        watched: true,
        data: "different".to_string(),
    };
    assert_eq!(first.get_partition_key(), second.get_partition_key());
    // and a different key lands somewhere else
    let third = Movie {
        id: 43,
        ..first.clone()
    };
    assert_ne!(first.get_partition_key(), third.get_partition_key());
}

/// A row keyed by several fields hashes to the partition its key's values name
///
/// The row is hashed field by field and the key as a tuple, and the client hashes both: a row to
/// learn where it is going, and a key to learn where to read it from. The two have to agree, and
/// this is the client build of that check
/// ([Resolved #92, #198](../../docs/src/appendix/resolved/composite-partition-key.md)).
#[test]
fn a_composite_key_hashes_the_same_from_a_row_and_from_its_values() {
    use shoal::traits::PartitionKeySupport;
    // a row and the tuple of its three key fields
    let row = Release {
        year: 1999,
        month: 3,
        id: 603,
        title: "The Matrix".to_string(),
    };
    assert_eq!(
        row.get_partition_key(),
        Release::get_partition_key_from_values(&(1999, 3, 603))
    );
    // and the same values in another order are another partition
    assert_ne!(
        row.get_partition_key(),
        Release::get_partition_key_from_values(&(603, 3, 1999))
    );
}

/// SHQL cannot name a composite key yet, and says so rather than guessing at one
///
/// The table compiles and every typed query reaches it, but the generated parse arm turns one
/// partition condition's literal into the whole key, and no literal is a tuple
/// ([item 41](../../docs/src/appendix/known-issues.md)). Until that is fixed the parse is
/// refused; this pins that it is refused, and that it does not panic.
#[test]
fn shql_refuses_a_composite_key() {
    // every field of the key named, which is what a fix would accept
    let parsed = <CheckDbClient as QuerySupport>::parse(
        "SELECT * FROM Release WHERE year = 1999 AND month = 3 AND id = 603;",
    );
    assert!(parsed.is_err(), "a composite key parsed: {parsed:?}");
}
