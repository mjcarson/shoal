//! The client half of `#[shoal::db]` is complete, in a crate with no engine
//!
//! That this crate compiles at all is the structural half of the check. These are the
//! behavioural half: each one touches a different emission, so a `db(client)` that silently
//! skipped one more than it meant to cannot pass by producing nothing.
//!
//! None of it needs a server, and none of it can start one - `shoal::ShoalPool` does not exist
//! in this build.

use shoal::traits::{QuerySupport, RkyvSupport, ShoalQuerySupport};

use shoal_client_check::{CheckDbClient, CheckDbQueryKinds, Movie};

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
    let bytes = RkyvSupport::serialize(&query);
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
    // both tables are named, in field order
    assert_eq!(
        <CheckDbClient as QuerySupport>::table_names(),
        &["Movie", "MovieByKeyword"]
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
    assert!(names.contains(&"id"), "the partition key is missing: {names:?}");
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
