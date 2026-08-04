//! Tests for binding a parsed SHQL query to a concrete table
//!
//! The parser itself is covered by unit tests in `shoal-core`. These tests cover the second
//! stage, where the generated `QuerySupport::parse` impl matches a parsed query against a real
//! schema, type checks its literals, and turns its conditions into a get query. None of this
//! needs a running server, so these are plain synchronous tests.

use deepsize2::DeepSizeOf;
use rkyv::{Archive, Deserialize, Serialize};
use shoal_core::shared::queries::parser::{FieldRole, Suggestion, SuggestionKind};
use shoal_core::shared::queries::{SortSelect, SortedQuery, UnsortedQuery};
use shoal_core::shared::traits::{PartitionKeySupport, QuerySupport};
use shoal_core::storage::FileSystem;
use shoal_core::tables::{PersistentSortedTable, PersistentUnsortedTable};
use shoal_derive::{db, ShoalProjection, ShoalSortedTable, ShoalUnsortedTable};
use std::ops::Bound;

/// An unsorted table with a partition key and two filterable fields
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalUnsortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "ShqlDb")]
pub struct Movie {
    /// The partition key for this movie
    #[shoal(partition)]
    pub id: u64,
    /// The title of this movie, which can be filtered on
    #[shoal(filter)]
    pub title: String,
    /// Whether this movie has been watched, which can be filtered on
    #[shoal(filter)]
    pub watched: bool,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

/// A sorted table with a partition key, a sort key, and a filterable field
#[derive(
    Debug, Archive, Serialize, Deserialize, Clone, ShoalSortedTable, PartialEq, Eq, DeepSizeOf,
)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "ShqlDb")]
pub struct Review {
    /// The partition key for this review
    #[shoal(partition)]
    pub movie: String,
    /// The sort key ordering reviews within a partition
    #[shoal(sort)]
    pub reviewer: String,
    /// The source of this review, which can be filtered on
    #[shoal(filter)]
    pub source: String,
    /// Some data payload
    #[shoal(update)]
    pub data: String,
}

/// A projection of a movie holding only what a list of them needs
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalProjection, PartialEq, Eq)]
#[rkyv(derive(Debug))]
#[shoal_projection(table = "Movie")]
pub struct MovieSummary {
    /// The partition this movie was in
    #[shoal(partition)]
    pub id: u64,
    /// The title of this movie
    pub title: String,
}

/// A projection of a review, so a projection of the wrong table can be tested
#[derive(Debug, Archive, Serialize, Deserialize, Clone, ShoalProjection, PartialEq, Eq)]
#[rkyv(derive(Debug))]
#[shoal_projection(table = "Review")]
pub struct ReviewSource {
    /// The partition this review was in
    #[shoal(partition)]
    pub movie: String,
    /// Where this review came from
    pub source: String,
}

/// The test database schema
#[db]
pub struct ShqlDb {
    /// The unsorted movie table
    #[shoal(projections(MovieSummary))]
    pub movies: PersistentUnsortedTable<Movie, FileSystem>,
    /// The sorted review table
    #[shoal(projections(ReviewSource))]
    pub reviews: PersistentSortedTable<Review, FileSystem>,
}

/// Parse a query and unwrap it, failing the test with the parse error if it did not bind
///
/// # Arguments
///
/// * `query` - The query to parse
fn parse(query: &str) -> ShqlDbQueryKinds {
    match ShqlDbClient::parse(query) {
        Ok(parsed) => parsed,
        Err(error) => panic!("Failed to parse '{}': {}", query, error),
    }
}

/// Parse a query that is expected to fail and return its error message
///
/// # Arguments
///
/// * `query` - The query to parse
fn parse_err(query: &str) -> String {
    match ShqlDbClient::parse(query) {
        Ok(_) => panic!("Expected '{}' to fail but it parsed", query),
        Err(error) => error.message,
    }
}

/// Parse a query against the unsorted table and return its get query
///
/// # Arguments
///
/// * `query` - The query to parse
fn parse_movie(query: &str) -> shoal_core::shared::queries::UnsortedGet<Movie> {
    // parse this query and make sure it bound to the movie table
    match parse(query) {
        ShqlDbQueryKinds::Movie(UnsortedQuery::Get(get)) => get,
        other => panic!("Expected a movie get query but got {:?}", other),
    }
}

/// Parse a query against the sorted table and return its get query
///
/// # Arguments
///
/// * `query` - The query to parse
fn parse_review(query: &str) -> shoal_core::shared::queries::SortedGet<Review> {
    // parse this query and make sure it bound to the review table
    match parse(query) {
        ShqlDbQueryKinds::Review(SortedQuery::Get(get)) => get,
        other => panic!("Expected a review get query but got {:?}", other),
    }
}

#[test]
/// A query naming a table that is not in the schema is rejected
fn rejects_an_unknown_table() {
    // this table does not exist in our schema
    let message = parse_err("SELECT * FROM Nope WHERE id = 1");
    assert!(
        message.contains("Unknown table 'Nope'"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// Table names are matched against the row struct name, not the schema field name
fn matches_the_struct_name_not_the_field_name() {
    // the schema field is called 'movies' but the struct is called 'Movie'
    let message = parse_err("SELECT * FROM movies WHERE id = 1");
    assert!(
        message.contains("Unknown table 'movies'"),
        "unexpected message: {}",
        message
    );
    // using the struct name binds correctly
    parse_movie("SELECT * FROM Movie WHERE id = 1");
}

#[test]
/// A condition naming a field the table does not have is rejected
fn rejects_an_unknown_field() {
    // 'nope' is not a field on the movie table
    let message = parse_err("SELECT * FROM Movie WHERE nope = 1");
    assert!(
        message.contains("Unknown field 'nope'"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A literal that cannot deserialize into the field's type is rejected
fn rejects_a_type_mismatch() {
    // id is a u64 so a string literal cannot be used for it
    let message = parse_err("SELECT * FROM Movie WHERE id = 'not a number'");
    assert!(
        message.contains("Type mismatch for field 'id'"),
        "unexpected message: {}",
        message
    );
    // and a negative number does not fit a u64 either
    let negative = parse_err("SELECT * FROM Movie WHERE id = -1");
    assert!(
        negative.contains("Type mismatch for field 'id'"),
        "unexpected message: {}",
        negative
    );
}

#[test]
/// A query that does not constrain a partition key is rejected
fn rejects_a_missing_partition_key() {
    // filtering alone gives the server no partition to look in
    let message = parse_err("SELECT * FROM Movie WHERE title = 'Alien'");
    assert!(
        message.contains("Missing partition key"),
        "unexpected message: {}",
        message
    );
    // the same holds for the sorted table when only a sort key is given
    let sorted = parse_err("SELECT * FROM Review WHERE reviewer = 'ann'");
    assert!(
        sorted.contains("Missing partition key"),
        "unexpected message: {}",
        sorted
    );
}

#[test]
/// The partition key from an unsorted query is hashed the same way a typed query hashes it
fn binds_an_unsorted_partition_key() {
    // parse a query constraining the partition key
    let get = parse_movie("SELECT * FROM Movie WHERE id = 550");
    // the hash should match what a typed query would have produced
    assert_eq!(
        get.partition_keys,
        vec![Movie::get_partition_key_from_values(&550)]
    );
}

#[test]
/// An IN list names several partitions on an unsorted table
///
/// This used to be impossible to express: `id = 1 AND id = 2` bound only the first value
/// and the second was silently dropped.
fn binds_unsorted_partition_keys_from_in() {
    // parse a query naming two partitions
    let get = parse_movie("SELECT * FROM Movie WHERE id IN (550, 551)");
    // both partitions should be present, hashed, and in the order they were written
    assert_eq!(
        get.partition_keys,
        vec![
            Movie::get_partition_key_from_values(&550),
            Movie::get_partition_key_from_values(&551),
        ]
    );
}

#[test]
/// A sorted query collects every value its partition condition named, in order
fn binds_sorted_partition_keys() {
    // parse a query naming two partitions
    let get = parse_review("SELECT * FROM Review WHERE movie IN ('alien', 'aliens')");
    // both partitions should be present, hashed, and in order
    assert_eq!(
        get.partition_keys,
        vec![
            Review::get_partition_key_from_values(&"alien".to_string()),
            Review::get_partition_key_from_values(&"aliens".to_string()),
        ]
    );
}

#[test]
/// OR binds exactly the same way an IN list does
fn binds_or_the_same_as_in() {
    // parse the same partitions written both ways
    let with_or = parse_review("SELECT * FROM Review WHERE movie = 'alien' OR movie = 'aliens'");
    let with_in = parse_review("SELECT * FROM Review WHERE movie IN ('alien', 'aliens')");
    assert_eq!(with_or.partition_keys, with_in.partition_keys);
}

#[test]
/// Constraining a partition key twice with AND is rejected rather than answered as a union
///
/// This is the query that used to look like an intersection and behave like a union.
fn rejects_a_partition_key_constrained_twice() {
    // two values for one partition key have to be written as a union to mean one
    let message = parse_err("SELECT * FROM Review WHERE movie = 'alien' AND movie = 'aliens'");
    assert!(
        message.contains("'movie' is constrained twice by AND"),
        "unexpected message: {}",
        message
    );
    // and the error should say how to write what was meant
    assert!(
        message.contains("movie IN ('alien', 'aliens')"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A partition key cannot be OR'd with a filter
fn rejects_or_between_a_partition_key_and_a_filter() {
    // the filter side of this names rows in no partition we could read
    let message = parse_err("SELECT * FROM Movie WHERE id = 550 OR title = 'Alien'");
    assert!(
        message.contains("'id' cannot be OR'd with 'title'"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// Sort key conditions are collected onto the get query
fn binds_sort_keys() {
    // parse a query narrowing by sort key
    let get = parse_review("SELECT * FROM Review WHERE movie = 'alien' AND reviewer = 'ann'");
    // the sort key should have been picked up
    assert_eq!(get.sort_select.keys(), Some(["ann".to_string()].as_slice()));
}

#[test]
/// An IN list on the sort key names each of its rows
///
/// A sort key is a set like a partition key is, so `IN` is how a query names more than one
/// of the rows in a partition.
fn binds_sort_keys_from_an_in_list() {
    // parse a query naming several rows of one partition
    let get =
        parse_review("SELECT * FROM Review WHERE movie = 'alien' AND reviewer IN ('ann', 'bob')");
    // every value of the sort condition names a row to return
    assert_eq!(
        get.sort_select.keys(),
        Some(["ann".to_string(), "bob".to_string()].as_slice())
    );
}

#[test]
/// A query naming no sort key asks for every row of its partitions
///
/// `SortSelect::All` is the only thing that means every row, so a query that never mentions
/// the sort key has to produce it rather than an empty set of keys - which now means none.
fn binds_no_sort_keys_when_none_are_named() {
    // parse a query that only names its partition
    let get = parse_review("SELECT * FROM Review WHERE movie = 'alien'");
    // nothing narrows this get to a row, so it asks for the whole partition
    assert!(matches!(get.sort_select, SortSelect::All));
}

#[test]
/// A range on the sort key binds to the bounds it was written with
fn binds_a_sort_key_range() {
    // parse a query bounded below and left open above
    let get = parse_review("SELECT * FROM Review WHERE movie = 'alien' AND reviewer > 'ann'");
    let range = get.sort_select.range().expect("reviewer should be bounded");
    // the operator excluded the row it named, which is what makes it a cursor
    assert_eq!(range.start, Bound::Excluded("ann".to_string()));
    assert_eq!(range.end, Bound::Unbounded);
}

#[test]
/// Each range operator binds to the bound that matches it
fn binds_every_range_operator() {
    // every spelling and the bound it should produce
    let cases = [
        (">", Bound::Excluded("m".to_string()), Bound::Unbounded),
        (">=", Bound::Included("m".to_string()), Bound::Unbounded),
        ("<", Bound::Unbounded, Bound::Excluded("m".to_string())),
        ("<=", Bound::Unbounded, Bound::Included("m".to_string())),
    ];
    // check each of them binds to the end it names, with the right inclusivity
    for (operator, start, end) in cases {
        let query =
            format!("SELECT * FROM Review WHERE movie = 'alien' AND reviewer {operator} 'm'");
        let get = parse_review(&query);
        let range = get.sort_select.range().expect("reviewer should be bounded");
        assert_eq!(range.start, start, "wrong lower bound for '{}'", operator);
        assert_eq!(range.end, end, "wrong upper bound for '{}'", operator);
    }
}

#[test]
/// Two bounds on the sort key bind into one range closed at both ends
///
/// The parser folds the two conditions into one clause, so the binder still finds a single
/// sort condition and never has to search for a second.
fn binds_a_sort_key_range_from_both_ends() {
    // parse a query bounded from each end
    let get = parse_review(
        "SELECT * FROM Review WHERE movie = 'alien' AND reviewer >= 'ann' AND reviewer < 'zoe'",
    );
    let range = get.sort_select.range().expect("reviewer should be bounded");
    // each operator kept its own inclusivity
    assert_eq!(range.start, Bound::Included("ann".to_string()));
    assert_eq!(range.end, Bound::Excluded("zoe".to_string()));
}

#[test]
/// A range on a partition key is refused
///
/// A partition is located by hashing its exact key, so there is no ordering to bound it with
/// and no scan to answer a bounded one.
fn rejects_a_range_on_a_partition_key() {
    let message = parse_err("SELECT * FROM Review WHERE movie > 'alien'");
    assert!(
        message.contains("partition key and cannot be given a range") && message.contains("movie"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A range on a filter is refused
///
/// A filter is a membership test evaluated per row, not an ordering, so a bound on one has
/// nothing to mean.
fn rejects_a_range_on_a_filter() {
    let message = parse_err("SELECT * FROM Review WHERE movie = 'alien' AND source > 'imdb'");
    assert!(
        message.contains("filter and cannot be given a range") && message.contains("source"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A range on an unsorted table's filter is refused too
///
/// An unsorted table has no sort key at all, so every field of it is unbounded by definition.
fn rejects_a_range_on_an_unsorted_table() {
    let message = parse_err("SELECT * FROM Movie WHERE id = 550 AND title > 'Alien'");
    assert!(
        message.contains("filter and cannot be given a range"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A LIMIT clause reaches the get query
fn binds_the_limit() {
    // an unsorted query carries its limit through
    let unsorted = parse_movie("SELECT * FROM Movie WHERE id = 550 LIMIT 7");
    assert_eq!(unsorted.limit, Some(7));
    // and so does a sorted one
    let sorted = parse_review("SELECT * FROM Review WHERE movie = 'alien' LIMIT 3");
    assert_eq!(sorted.limit, Some(3));
    // a query with no limit leaves it unset
    assert_eq!(parse_movie("SELECT * FROM Movie WHERE id = 550").limit, None);
}

#[test]
/// Filter conditions reach the get query on an unsorted table
fn binds_unsorted_filters() {
    // parse a query filtering on top of a partition key
    let get = parse_movie("SELECT * FROM Movie WHERE id = 550 AND title = 'Alien'");
    // the filter should have been built and set
    let filters = get.filters.expect("expected filters to be set");
    assert_eq!(filters.title, Some(vec!["Alien".to_string()]));
    // the filter we did not name should be left unset
    assert_eq!(filters.watched, None);
}

#[test]
/// Several filter conditions can be set at once
fn binds_multiple_filters() {
    // parse a query filtering on both filterable fields
    let get = parse_movie("SELECT * FROM Movie WHERE id = 550 AND title = 'Alien' AND watched = true");
    // both filters should have been picked up
    let filters = get.filters.expect("expected filters to be set");
    assert_eq!(filters.title, Some(vec!["Alien".to_string()]));
    assert_eq!(filters.watched, Some(vec![true]));
}

#[test]
/// A filter may be given several values with IN, and a row matching any of them passes
fn binds_a_multi_value_filter() {
    // parse a query whose filter names two titles
    let get = parse_movie("SELECT * FROM Movie WHERE id = 550 AND title IN ('Alien', 'Aliens')");
    // both values should be on the filter, in the order they were written
    let filters = get.filters.expect("expected filters to be set");
    assert_eq!(
        filters.title,
        Some(vec!["Alien".to_string(), "Aliens".to_string()])
    );
    // and OR on a filter field means the same thing
    let with_or =
        parse_movie("SELECT * FROM Movie WHERE id = 550 AND title = 'Alien' OR title = 'Aliens'");
    assert_eq!(
        with_or.filters.expect("expected filters to be set").title,
        filters.title
    );
}

#[test]
/// Filter conditions reach the get query on a sorted table
fn binds_sorted_filters() {
    // parse a query filtering a sorted table
    let get = parse_review("SELECT * FROM Review WHERE movie = 'alien' AND source = 'imdb'");
    // the filter should have been built and set
    let filters = get.filters.expect("expected filters to be set");
    assert_eq!(filters.source, Some(vec!["imdb".to_string()]));
}

#[test]
/// A query with no filter conditions leaves the filters unset
fn leaves_filters_unset_when_none_are_given() {
    // an unsorted query constraining only the partition key has no filters
    assert!(parse_movie("SELECT * FROM Movie WHERE id = 550")
        .filters
        .is_none());
    // and neither does a sorted query constraining only keys
    assert!(
        parse_review("SELECT * FROM Review WHERE movie = 'alien' AND reviewer = 'ann'")
            .filters
            .is_none()
    );
}

#[test]
/// A filter whose literal has the wrong type is rejected before it reaches the get query
fn rejects_a_filter_type_mismatch() {
    // watched is a bool so a number cannot be used for it
    let message = parse_err("SELECT * FROM Movie WHERE id = 550 AND watched = 5");
    assert!(
        message.contains("Type mismatch for field 'watched'"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// Keywords stay case insensitive all the way through binding
fn binds_a_lowercase_query() {
    // the same query written in lower case binds identically
    let get = parse_movie("select * from Movie where id = 550 and title = 'Alien' limit 2");
    assert_eq!(
        get.partition_keys,
        vec![Movie::get_partition_key_from_values(&550)]
    );
    assert_eq!(get.limit, Some(2));
    assert_eq!(
        get.filters.expect("expected filters to be set").title,
        Some(vec!["Alien".to_string()])
    );
}

/// Suggest completions for a query with the cursor at the end of it
///
/// # Arguments
///
/// * `query` - The query to suggest completions for
fn suggest(query: &str) -> Vec<Suggestion> {
    shoal_core::shared::queries::parser::suggest::<ShqlDbClient>(query, query.len()).items
}

/// Suggest completions and return just their text, which is usually all a test cares about
///
/// # Arguments
///
/// * `query` - The query to suggest completions for
fn suggest_text(query: &str) -> Vec<String> {
    suggest(query)
        .into_iter()
        .map(|suggestion| suggestion.text)
        .collect()
}

#[test]
/// Every table in the schema is offered after FROM
fn suggests_every_table() {
    // with nothing typed yet both tables are on offer
    let tables = suggest_text("SELECT * FROM ");
    assert_eq!(tables, vec!["Movie", "Review"]);
    // and they are tables, not keywords
    assert!(suggest("SELECT * FROM ")
        .iter()
        .all(|suggestion| suggestion.kind == SuggestionKind::Table));
}

#[test]
/// Table names are fuzzy matched so case and gaps do not matter
fn fuzzy_matches_table_names() {
    // a lower case prefix still finds the canonical name
    assert_eq!(suggest_text("SELECT * FROM mov"), vec!["Movie"]);
    // and so does a subsequence of it
    assert_eq!(suggest_text("SELECT * FROM rvw"), vec!["Review"]);
    // a name that is in neither table matches nothing
    assert!(suggest_text("SELECT * FROM zzz").is_empty());
}

#[test]
/// Keywords are suggested in the case the user is typing them in
fn suggests_keywords_in_the_typed_case() {
    // an empty word gets the canonical upper case keyword
    assert_eq!(suggest_text(""), vec!["SELECT"]);
    // a lower case word keeps the query lower case
    assert_eq!(suggest_text("sel"), vec!["select"]);
    // an upper case word stays upper case
    assert_eq!(suggest_text("SEL"), vec!["SELECT"]);
    // and the same holds deeper into a query
    assert_eq!(suggest_text("select * from Movie wh"), vec!["where"]);
}

#[test]
/// Only fields that can be used in a where clause are offered
fn suggests_only_usable_fields() {
    // every field with a role is on offer
    let fields = suggest_text("SELECT * FROM Movie WHERE ");
    // the partition key comes first, then the filters in schema order
    assert_eq!(fields, vec!["id", "title", "watched"]);
    // 'data' is only marked as updatable, so it would be rejected by the parser
    assert!(
        !fields.contains(&"data".to_string()),
        "a field with no role should never be suggested"
    );
}

#[test]
/// Fields are annotated with the role they play and the type they hold
fn annotates_fields_with_their_role_and_type() {
    // pull the suggestions for the sorted table, which uses all three roles
    let fields = suggest("SELECT * FROM Review WHERE ");
    let details: Vec<(String, String)> = fields
        .into_iter()
        .map(|suggestion| (suggestion.text, suggestion.detail))
        .collect();
    // the partition key is offered first since a query cannot be built without one
    assert_eq!(
        details,
        vec![
            ("movie".to_string(), "partition String".to_string()),
            ("reviewer".to_string(), "sort String".to_string()),
            ("source".to_string(), "filter String".to_string()),
        ]
    );
}

#[test]
/// Fields are fuzzy matched the same way tables are
fn fuzzy_matches_field_names() {
    assert_eq!(suggest_text("SELECT * FROM Movie WHERE wat"), vec!["watched"]);
    assert!(suggest_text("SELECT * FROM Movie WHERE zzz").is_empty());
}

#[test]
/// A table that is not in the schema has no fields to offer
fn suggests_no_fields_for_an_unknown_table() {
    assert!(suggest_text("SELECT * FROM Nope WHERE ").is_empty());
}

#[test]
/// A boolean field is the only kind whose values we can offer in full
fn suggests_values_a_field_accepts() {
    // a bool field can only ever be true or false
    assert_eq!(
        suggest_text("SELECT * FROM Movie WHERE watched = "),
        vec!["true", "false"]
    );
    // a string field gets its opening quote
    assert_eq!(suggest_text("SELECT * FROM Movie WHERE title = "), vec!["'"]);
    // and there is nothing useful to offer for a number
    assert!(suggest_text("SELECT * FROM Movie WHERE id = ").is_empty());
}

#[test]
/// A partially typed value is matched against what the field accepts
fn narrows_values_as_they_are_typed() {
    assert_eq!(
        suggest_text("SELECT * FROM Movie WHERE watched = tr"),
        vec!["true"]
    );
}

#[test]
/// Once a condition is complete the query can be continued, limited, or ended
fn suggests_how_to_continue_a_query() {
    assert_eq!(
        suggest_text("SELECT * FROM Movie WHERE id = 550 "),
        vec!["AND", "OR", "LIMIT", ";"]
    );
    // after a limit only the terminator is left
    assert_eq!(
        suggest_text("SELECT * FROM Movie WHERE id = 550 LIMIT 10 "),
        vec![";"]
    );
    // and a finished query has nothing left to offer
    assert!(suggest_text("SELECT * FROM Movie WHERE id = 550;").is_empty());
}

#[test]
/// The range operators are offered on a sort key and on nothing else
///
/// Binding refuses a range on a partition key or a filter, so offering one there would walk
/// the user straight into an error the menu could have kept them out of.
fn suggests_range_operators_only_for_a_sort_key() {
    // a sort key can be matched or bounded
    assert_eq!(
        suggest_text("SELECT * FROM Review WHERE reviewer "),
        vec!["=", "IN", "<", "<=", ">", ">="]
    );
    // a partition key can only be matched
    assert_eq!(
        suggest_text("SELECT * FROM Review WHERE movie "),
        vec!["=", "IN"]
    );
    // and so can a filter
    assert_eq!(
        suggest_text("SELECT * FROM Review WHERE source "),
        vec!["=", "IN"]
    );
}

#[test]
/// A field can be followed by an IN list as well as an equals
fn suggests_in_alongside_equals() {
    // both operators are offered once a field has been named
    assert_eq!(
        suggest_text("SELECT * FROM Movie WHERE id "),
        vec!["=", "IN"]
    );
    // once IN has been typed the list has to be opened
    assert_eq!(
        suggest_text("SELECT * FROM Movie WHERE id IN "),
        vec!["("]
    );
}

#[test]
/// Inside an IN list the fields own values are offered, then the way to carry on
fn suggests_inside_an_in_list() {
    // the first value in a list is one of the fields own values
    assert_eq!(
        suggest_text("SELECT * FROM Movie WHERE watched IN ("),
        vec!["true", "false"]
    );
    // after a value the list can take another or be closed
    assert_eq!(
        suggest_text("SELECT * FROM Movie WHERE id IN (550 "),
        vec![",", ")"]
    );
    // and after the separator we are back to offering values
    assert_eq!(
        suggest_text("SELECT * FROM Movie WHERE watched IN (true, "),
        vec!["true", "false"]
    );
    // once the list is closed the query continues as any other condition would
    assert_eq!(
        suggest_text("SELECT * FROM Movie WHERE id IN (550) "),
        vec!["AND", "OR", "LIMIT", ";"]
    );
}

#[test]
/// An OR is followed by another field just like an AND is
fn suggests_a_field_after_or() {
    // both connectives put us back to naming a field
    let after_or = suggest_text("SELECT * FROM Movie WHERE id = 550 OR ");
    let after_and = suggest_text("SELECT * FROM Movie WHERE id = 550 AND ");
    assert_eq!(after_or, after_and);
    assert!(
        after_or.contains(&"id".to_string()),
        "unexpected suggestions: {:?}",
        after_or
    );
}

#[test]
/// Accepting a suggestion leaves the query ready for the next token
fn accepted_text_is_spaced_for_the_next_token() {
    // a table name is followed by another keyword, so it gets a trailing space
    let table = suggest("SELECT * FROM Mov").remove(0);
    assert_eq!(table.insert_text(), "Movie ");
    // an opening quote is followed by the string itself, so it does not
    let quote = suggest("SELECT * FROM Movie WHERE title = ").remove(0);
    assert_eq!(quote.insert_text(), "'");
}

#[test]
/// Suggestions replace the word under the cursor, not the whole query
fn replaces_only_the_word_under_the_cursor() {
    let query = "SELECT * FROM Mov";
    let completions =
        shoal_core::shared::queries::parser::suggest::<ShqlDbClient>(query, query.len());
    assert_eq!(completions.word_start, "SELECT * FROM ".len());
    assert_eq!(completions.word_end, query.len());
}

#[test]
/// Every table can be reached through the schema a client exposes
fn exposes_the_schema_to_a_client() {
    // the tables are named by their row struct
    assert_eq!(ShqlDbClient::table_names(), &["Movie", "Review"]);
    // every field comes back, including the ones with no role
    let fields = ShqlDbClient::table_fields("Movie").expect("expected the movie table");
    let names: Vec<&str> = fields.iter().map(|field| field.name).collect();
    assert_eq!(names, vec!["id", "title", "watched", "data"]);
    // and the roles are the ones the table was declared with
    assert_eq!(fields[0].role, Some(FieldRole::Partition));
    assert_eq!(fields[1].role, Some(FieldRole::Filter));
    assert_eq!(fields[3].role, None);
    // a table that is not in the schema has nothing to expose
    assert!(ShqlDbClient::table_fields("Nope").is_none());
}

#[test]
/// A star binds to the whole row
fn a_star_binds_to_the_whole_row() {
    // a query that wrote a star asked for every field of every row
    let get = parse_movie("SELECT * FROM Movie WHERE id = 1");
    assert_eq!(get.projection, MovieProjection::Full);
}

#[test]
/// A named projection binds to that projection
fn a_named_projection_binds_to_it() {
    // the name a query wrote is matched against the projections its table declared
    let get = parse_movie("SELECT MovieSummary FROM Movie WHERE id = 1");
    assert_eq!(get.projection, MovieProjection::MovieSummary);
    // and the rest of the query still binds the way it always did
    assert_eq!(get.partition_keys.len(), 1);
}

#[test]
/// A projection binds on a sorted table too
fn a_named_projection_binds_on_a_sorted_table() {
    // a sorted get carries its projection alongside its row selection
    let get = parse_review("SELECT ReviewSource FROM Review WHERE movie = 'fight club'");
    assert_eq!(get.projection, ReviewProjection::ReviewSource);
    // a query that never narrowed itself still wants every row
    assert!(matches!(get.sort_select, SortSelect::All));
}

#[test]
/// A projection no table declared is rejected, and says which ones exist
fn rejects_an_unknown_projection() {
    // a name that is not a projection of this table cannot be answered
    let message = parse_err("SELECT Nope FROM Movie WHERE id = 1");
    assert!(
        message.contains("is not a projection of Movie"),
        "unexpected message: {}",
        message
    );
    // and the error names the projections that would have worked
    assert!(
        message.contains("MovieSummary"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A projection of another table is rejected rather than silently answered
///
/// A projection is scoped to the table it projects, so naming one belonging to a different
/// table is the same mistake as naming one that does not exist.
fn rejects_a_projection_of_another_table() {
    // ReviewSource is a projection, but not one of the movie table
    let message = parse_err("SELECT ReviewSource FROM Movie WHERE id = 1");
    assert!(
        message.contains("is not a projection of Movie"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// Every projection in the database is offered where the star goes
///
/// The table is not known yet at that point in the query, so all of them are offered and
/// binding is what rejects a projection of the wrong table.
fn suggests_every_projection_in_place_of_the_star() {
    // a star and every projection are what can follow SELECT
    let offered = suggest_text("SELECT ");
    assert_eq!(offered, vec!["*", "MovieSummary", "ReviewSource"]);
    // a projection is suggested as a projection, not as a table or a field
    let kinds: Vec<SuggestionKind> = suggest("SELECT ")
        .iter()
        .skip(1)
        .map(|suggestion| suggestion.kind)
        .collect();
    assert_eq!(
        kinds,
        vec![SuggestionKind::Projection, SuggestionKind::Projection]
    );
}

#[test]
/// A partly typed projection narrows the projections offered
fn narrows_projections_as_they_are_typed() {
    // typing the start of a projection name leaves only the ones that could still match
    assert_eq!(suggest_text("SELECT Movie"), vec!["MovieSummary"]);
    // and a name no projection starts with offers nothing
    assert!(suggest_text("SELECT zzz").is_empty());
}

#[test]
/// Every projection is exposed through the schema a client exposes
fn exposes_projections_to_a_client() {
    // each projection is paired with the table it projects
    assert_eq!(
        ShqlDbClient::projection_names(),
        &[("MovieSummary", "Movie"), ("ReviewSource", "Review")]
    );
}
