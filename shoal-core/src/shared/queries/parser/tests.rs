//! Tests for the SHQL query parser

use super::*;
use crate::client::ShqlParseError;

/// Parse a query and unwrap it, failing the test with the parse error if it did not parse
///
/// # Arguments
///
/// * `query` - The query to parse
fn parse(query: &str) -> ParsedSelect {
    match ParsedSelect::new(query) {
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
    match ParsedSelect::new(query) {
        Ok(parsed) => panic!("Expected '{}' to fail but it parsed as {:?}", query, parsed),
        Err(error) => error.message,
    }
}

/// Parse a query with a single condition and return that condition
///
/// # Arguments
///
/// * `query` - The query to parse
fn parse_one(query: &str) -> WhereClause {
    // parse this query
    let mut parsed = parse(query);
    // make sure it only had a single condition
    assert_eq!(parsed.conditions.len(), 1, "expected one condition");
    // hand back the only condition
    parsed.conditions.remove(0)
}

/// Get the values a clause matched its field against, failing the test if it bounded it
///
/// # Arguments
///
/// * `clause` - The clause to read the values of
fn values_of(clause: &WhereClause) -> &[WhereValue] {
    clause
        .as_values()
        .unwrap_or_else(|| panic!("'{}' was bounded rather than matched", clause.field))
}

/// Get the range a clause bounded its field by, failing the test if it matched it
///
/// # Arguments
///
/// * `clause` - The clause to read the range of
fn range_of(clause: &WhereClause) -> &WhereRange {
    clause
        .as_range()
        .unwrap_or_else(|| panic!("'{}' was matched rather than bounded", clause.field))
}

#[test]
/// A full query with every clause parses into its parts
fn parses_a_complete_query() {
    // parse a query using every supported clause
    let parsed = parse("SELECT * FROM Movie WHERE id = 550 LIMIT 10;");
    // the table name comes from the identifier after FROM
    assert_eq!(parsed.table_name, "Movie");
    // the single where condition is kept
    assert_eq!(parsed.conditions.len(), 1);
    assert_eq!(parsed.conditions[0].field, "id");
    assert_eq!(*parsed.conditions[0].first(), Value::Number(550.into()));
    // and the limit is picked up
    assert_eq!(parsed.limit, Some(10));
}

#[test]
/// The LIMIT clause and the trailing semicolon are both optional
fn limit_and_semicolon_are_optional() {
    // parse a query with neither a limit nor a semicolon
    let parsed = parse("SELECT * FROM Movie WHERE id = 550");
    // no limit was given so none should be set
    assert_eq!(parsed.limit, None);
    // the same query with just a semicolon still parses
    let with_semicolon = parse("SELECT * FROM Movie WHERE id = 550;");
    assert_eq!(with_semicolon.limit, None);
    assert_eq!(with_semicolon.table_name, "Movie");
}

#[test]
/// Keywords are matched without regard to case
fn keywords_are_case_insensitive() {
    // parse the same query in lower, upper, and mixed case
    let lower = parse("select * from Movie where id = 550 limit 10");
    let upper = parse("SELECT * FROM Movie WHERE id = 550 LIMIT 10");
    let mixed = parse("SeLeCt * FrOm Movie WhErE id = 550 LiMiT 10");
    // all three should produce the same table name and limit
    for parsed in [&lower, &upper, &mixed] {
        assert_eq!(parsed.table_name, "Movie");
        assert_eq!(parsed.limit, Some(10));
    }
}

#[test]
/// The AND keyword is matched without regard to case
fn and_keyword_is_case_insensitive() {
    // parse a chained query using a lowercase and
    let parsed = parse("SELECT * FROM Movie WHERE id = 1 and title = 'Alien'");
    // both conditions should have been picked up
    assert_eq!(parsed.conditions.len(), 2);
}

#[test]
/// Table names are case sensitive even though keywords are not
fn table_names_are_case_sensitive() {
    // the identifier after FROM is kept exactly as it was written
    assert_eq!(parse("select * from Movie where id = 1").table_name, "Movie");
    assert_eq!(parse("select * from movie where id = 1").table_name, "movie");
}

#[test]
/// Arbitrary whitespace including newlines is allowed between clauses
fn handles_newlines_and_extra_whitespace() {
    // parse a query broken across several lines with ragged spacing
    let parsed = parse(
        "  SELECT   *   FROM   Movie\n  WHERE   id   =   550\n  AND   title   =   'Alien'\n  LIMIT   10  ;  ",
    );
    // every clause should still be found
    assert_eq!(parsed.table_name, "Movie");
    assert_eq!(parsed.conditions.len(), 2);
    assert_eq!(parsed.limit, Some(10));
}

#[test]
/// Identifiers may start with an underscore and contain digits
fn parses_underscore_and_digit_identifiers() {
    // parse a query whose table and field names use underscores and digits
    let parsed = parse("SELECT * FROM _my_table2 WHERE _field2 = 1");
    // both identifiers should have come through intact
    assert_eq!(parsed.table_name, "_my_table2");
    assert_eq!(parsed.conditions[0].field, "_field2");
}

#[test]
/// Identifiers follow UAX #31, so anything Rust accepts as a name parses here
fn parses_non_ascii_identifiers() {
    // parse a query whose table and field names both start with a non ascii character
    let parsed = parse("SELECT * FROM Ünsorted WHERE café = 1");
    // both identifiers should have come through intact
    assert_eq!(parsed.table_name, "Ünsorted");
    assert_eq!(parsed.conditions[0].field, "café");
    // names that are not latin at all are identifiers too
    let parsed = parse("SELECT * FROM 映画 WHERE 題名 = '君の名は'");
    assert_eq!(parsed.table_name, "映画");
    assert_eq!(parsed.conditions[0].field, "題名");
}

#[test]
/// An identifier cannot start with a digit
fn rejects_identifiers_starting_with_a_digit() {
    // a table name starting with a digit is not a valid identifier
    let message = parse_err("SELECT * FROM 2Movie WHERE id = 1");
    assert!(
        message.contains("SELECT * FROM"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// String literals are single quoted and may be empty
fn parses_string_literals() {
    // a normal string literal keeps its contents without the quotes
    let condition = parse_one("SELECT * FROM Movie WHERE title = 'Alien'");
    assert_eq!(*condition.first(), Value::String("Alien".to_string()));
    // an empty string literal is allowed
    let empty = parse_one("SELECT * FROM Movie WHERE title = ''");
    assert_eq!(*empty.first(), Value::String(String::new()));
    // strings may contain spaces and punctuation
    let spaced = parse_one("SELECT * FROM Movie WHERE title = 'A Space Odyssey: 2001'");
    assert_eq!(
        *spaced.first(),
        Value::String("A Space Odyssey: 2001".to_string())
    );
}

#[test]
/// Integers parse with an optional leading sign
fn parses_integer_literals() {
    // an unsigned integer
    let plain = parse_one("SELECT * FROM Movie WHERE id = 42");
    assert_eq!(plain.first().as_i64(), Some(42));
    // a negative integer
    let negative = parse_one("SELECT * FROM Movie WHERE id = -17");
    assert_eq!(negative.first().as_i64(), Some(-17));
    // an explicitly signed positive integer
    let positive = parse_one("SELECT * FROM Movie WHERE id = +100");
    assert_eq!(positive.first().as_i64(), Some(100));
}

#[test]
/// Floats parse with digits on both sides of the decimal point
fn parses_float_literals() {
    // a positive float
    let plain = parse_one("SELECT * FROM Movie WHERE rating = 8.5");
    assert_eq!(plain.first().as_f64(), Some(8.5));
    // a negative float
    let negative = parse_one("SELECT * FROM Movie WHERE rating = -0.5");
    assert_eq!(negative.first().as_f64(), Some(-0.5));
}

#[test]
/// Boolean and null literals parse without regard to case
fn parses_boolean_and_null_literals() {
    // booleans in either case
    assert_eq!(
        *parse_one("SELECT * FROM Movie WHERE watched = true").first(),
        Value::Bool(true)
    );
    assert_eq!(
        *parse_one("SELECT * FROM Movie WHERE watched = FALSE").first(),
        Value::Bool(false)
    );
    // and null in either case
    assert_eq!(
        *parse_one("SELECT * FROM Movie WHERE note = null").first(),
        Value::Null
    );
    assert_eq!(
        *parse_one("SELECT * FROM Movie WHERE note = NULL").first(),
        Value::Null
    );
}

#[test]
/// Each condition records byte offsets that slice back to its literal
fn tracks_value_positions() {
    // parse a query mixing a string and a number literal
    let query = "SELECT * FROM Movie WHERE title = 'Alien' AND id = 550";
    let parsed = parse(query);
    // the recorded span for the string should cover the quoted literal
    let title = &parsed.conditions[0];
    assert_eq!(&query[values_of(&title)[0].start..values_of(&title)[0].end], "'Alien'");
    // and the span for the number should cover just the digits
    let id = &parsed.conditions[1];
    assert_eq!(&query[values_of(&id)[0].start..values_of(&id)[0].end], "550");
}

#[test]
/// Conditions chained with AND are kept in the order they were written
fn parses_and_chains_in_order() {
    // parse a query chaining three conditions
    let parsed = parse("SELECT * FROM Movie WHERE a = 1 AND b = 2 AND c = 3");
    // collect the field names in the order they came back
    let fields: Vec<&str> = parsed
        .conditions
        .iter()
        .map(|condition| condition.field.as_str())
        .collect();
    // they should match the order in the query
    assert_eq!(fields, vec!["a", "b", "c"]);
}

#[test]
/// A query must start with SELECT
fn rejects_a_missing_select() {
    // a query that does not start with SELECT cannot be parsed
    let message = parse_err("FROM Movie WHERE id = 1");
    assert!(
        message.contains("SELECT * FROM"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// Only SELECT * is supported so a projection is rejected
fn rejects_a_projection() {
    // naming columns instead of using * is not supported
    let message = parse_err("SELECT id FROM Movie WHERE id = 1");
    assert!(
        message.contains("SELECT * FROM"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A query must name a table with FROM
fn rejects_a_missing_from() {
    // leaving out the FROM clause is a parse error
    let message = parse_err("SELECT * Movie WHERE id = 1");
    assert!(
        message.contains("SELECT * FROM"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A missing WHERE clause is reported as a missing WHERE clause
fn rejects_a_missing_where_clause_clearly() {
    // a query with no WHERE clause should say so rather than fail generically
    let message = parse_err("SELECT * FROM Movie;");
    assert!(
        message.contains("WHERE clause is required"),
        "unexpected message: {}",
        message
    );
    // the same holds when the query simply ends after the table name
    let bare = parse_err("SELECT * FROM Movie");
    assert!(
        bare.contains("WHERE clause is required"),
        "unexpected message: {}",
        bare
    );
}

#[test]
/// A field whose name merely starts with the WHERE keyword is not a WHERE clause
fn does_not_mistake_a_prefix_for_the_where_keyword() {
    // 'wherever' starts with 'where' but is not the keyword
    let message = parse_err("SELECT * FROM Movie wherever = 1");
    assert!(
        message.contains("WHERE clause is required"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// An operator that is not one we support is rejected, naming the ones that are
///
/// `!=` is the interesting one: its `=` would parse as equality if the operator scan ran
/// after the equality one rather than before it.
fn rejects_unsupported_operators() {
    // every comparison outside the supported set should fail, naming the field
    for query in [
        "SELECT * FROM Movie WHERE id != 1",
        "SELECT * FROM Movie WHERE id LIKE 1",
        "SELECT * FROM Movie WHERE id BETWEEN 1 AND 2",
    ] {
        let message = parse_err(query);
        assert!(
            message.contains("Expected an operator") && message.contains("id"),
            "unexpected message for '{}': {}",
            query,
            message
        );
    }
}

#[test]
/// Each range operator parses and lands on the end of the range it names
fn parses_each_range_operator() {
    // the two lower bounds, which differ only in whether they include their value
    let greater = parse_one("SELECT * FROM Movie WHERE title > 'a'");
    let lower = range_of(&greater).lower.as_ref().expect("a lower bound");
    assert!(!lower.inclusive);
    assert_eq!(lower.value.value, Value::String("a".to_string()));
    assert!(range_of(&greater).upper.is_none());
    let at_least = parse_one("SELECT * FROM Movie WHERE title >= 'a'");
    assert!(
        range_of(&at_least)
            .lower
            .as_ref()
            .expect("a lower bound")
            .inclusive
    );
    // and the two upper ones
    let less = parse_one("SELECT * FROM Movie WHERE title < 'z'");
    let upper = range_of(&less).upper.as_ref().expect("an upper bound");
    assert!(!upper.inclusive);
    assert_eq!(upper.value.value, Value::String("z".to_string()));
    assert!(range_of(&less).lower.is_none());
    let at_most = parse_one("SELECT * FROM Movie WHERE title <= 'z'");
    assert!(
        range_of(&at_most)
            .upper
            .as_ref()
            .expect("an upper bound")
            .inclusive
    );
}

#[test]
/// The two character operators are matched before the one character ones they contain
///
/// `>` is a prefix of `>=`, so a scan that checked the short one first would take the `=` as
/// the start of the value and fail on it - or worse, read `>= 5` as `> (= 5)`.
fn range_operators_prefer_their_longer_spelling() {
    // an inclusive lower bound is one operator and not two
    let clause = parse_one("SELECT * FROM Movie WHERE id >= 5");
    let lower = range_of(&clause).lower.as_ref().expect("a lower bound");
    assert!(lower.inclusive);
    assert_eq!(lower.value.value.as_i64(), Some(5));
}

#[test]
/// Two range conditions on one field fold into a single clause bounded at both ends
///
/// This is the one shape where `AND` may name a field twice, and folding them is what keeps
/// the one clause per field rule everything downstream looks a field up with.
fn two_bounds_on_one_field_fold_together() {
    // bound one field from each end
    let parsed = parse("SELECT * FROM Movie WHERE id = 1 AND title >= 'a' AND title < 'm'");
    // the two title conditions came back as one clause
    assert_eq!(parsed.conditions.len(), 2);
    assert_eq!(parsed.conditions[1].field, "title");
    // and that one clause holds both ends
    let range = range_of(&parsed.conditions[1]);
    assert!(range.lower.as_ref().expect("a lower bound").inclusive);
    assert!(!range.upper.as_ref().expect("an upper bound").inclusive);
}

#[test]
/// The order the two bounds are written in does not change the clause they fold into
fn bounds_fold_in_either_order() {
    // write the upper bound first
    let parsed = parse("SELECT * FROM Movie WHERE id = 1 AND title < 'm' AND title >= 'a'");
    let range = range_of(&parsed.conditions[1]);
    // each end still landed on its own side
    assert_eq!(
        range.lower.as_ref().expect("a lower bound").value.value,
        Value::String("a".to_string())
    );
    assert_eq!(
        range.upper.as_ref().expect("an upper bound").value.value,
        Value::String("m".to_string())
    );
}

#[test]
/// A field given the same end of a range twice is rejected
///
/// A range has one lower bound. Two of them means one of the pair, and shoal will not guess
/// which.
fn rejects_two_bounds_on_the_same_end() {
    // two lower bounds on one field
    let message = parse_err("SELECT * FROM Movie WHERE id = 1 AND title > 'a' AND title > 'b'");
    assert!(
        message.contains("two lower bounds") && message.contains("title"),
        "unexpected message: {}",
        message
    );
    // and the same for two upper ones
    let message = parse_err("SELECT * FROM Movie WHERE id = 1 AND title < 'a' AND title <= 'b'");
    assert!(
        message.contains("two upper bounds") && message.contains("title"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A field cannot be both matched against a value and bounded by a range
///
/// The two are different questions about one field, and answering both would mean deciding
/// which of them wins.
fn rejects_a_value_and_a_range_on_one_field() {
    // a value then a bound
    let message = parse_err("SELECT * FROM Movie WHERE id = 1 AND title = 'a' AND title < 'm'");
    assert!(
        message.contains("both a value and a range") && message.contains("title"),
        "unexpected message: {}",
        message
    );
    // and a bound then a value, which folds the other way round
    let message = parse_err("SELECT * FROM Movie WHERE id = 1 AND title < 'm' AND title = 'a'");
    assert!(
        message.contains("both a value and a range"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A range cannot be joined by OR
///
/// `OR` chooses between values for one field. Two ranges are a union, and there is no access
/// path that reads one.
fn rejects_a_range_joined_by_or() {
    // two ranges on one field
    let message = parse_err("SELECT * FROM Movie WHERE title > 'a' OR title < 'z'");
    assert!(
        message.contains("cannot be OR'd with a range"),
        "unexpected message: {}",
        message
    );
    // and a value OR'd with a range on the same field
    let message = parse_err("SELECT * FROM Movie WHERE title = 'a' OR title < 'z'");
    assert!(
        message.contains("cannot be OR'd with a range"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A range operator records the span of the value it bounds at
///
/// Errors raised while binding a range point at the literal that failed, so the offsets have
/// to survive the operator being parsed.
fn tracks_the_positions_of_range_values() {
    // parse a query bounded from both ends
    let query = "SELECT * FROM Movie WHERE id = 1 AND title >= 'Alien' AND title < 'Zodiac'";
    let parsed = parse(query);
    // both literals can be sliced back out of the query they came from
    let range = range_of(&parsed.conditions[1]);
    let lower = &range.lower.as_ref().expect("a lower bound").value;
    assert_eq!(&query[lower.start..lower.end], "'Alien'");
    let upper = &range.upper.as_ref().expect("an upper bound").value;
    assert_eq!(&query[upper.start..upper.end], "'Zodiac'");
}

#[test]
/// An unterminated string literal is a parse error
fn rejects_an_unterminated_string() {
    // a string with no closing quote cannot be parsed
    let message = parse_err("SELECT * FROM Movie WHERE title = 'Alien");
    assert!(
        message.contains("Expected a value for field 'title'"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// Input left over after the query is rejected rather than silently dropped
fn rejects_trailing_input() {
    // anything after the query should be reported
    let message = parse_err("SELECT * FROM Movie WHERE id = 1 DROP TABLE");
    assert!(
        message.contains("trailing input"),
        "unexpected message: {}",
        message
    );
    // this holds after a semicolon too
    let after_semicolon = parse_err("SELECT * FROM Movie WHERE id = 1; garbage");
    assert!(
        after_semicolon.contains("trailing input"),
        "unexpected message: {}",
        after_semicolon
    );
}

#[test]
/// An integer too large for an i64 is an error rather than a panic
fn rejects_an_out_of_range_integer() {
    // this literal is well past i64::MAX
    let message = parse_err("SELECT * FROM Movie WHERE id = 99999999999999999999999");
    assert!(
        message.contains("Expected a value for field 'id'"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A float too large for an f64 is an error rather than a panic
fn rejects_an_out_of_range_float() {
    // build a float literal with enough digits to overflow an f64
    let huge = "9".repeat(400);
    let query = format!("SELECT * FROM Movie WHERE rating = {}.0", huge);
    // it should fail to parse instead of panicking on an infinite value
    let message = parse_err(&query);
    assert!(
        message.contains("Expected a value for field 'rating'"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A limit too large for a usize is an error rather than a panic
fn rejects_an_out_of_range_limit() {
    // this limit is well past usize::MAX
    let message = parse_err("SELECT * FROM Movie WHERE id = 1 LIMIT 99999999999999999999999");
    assert!(
        message.contains("LIMIT clause"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A limit of zero is accepted by the parser
fn parses_a_zero_limit() {
    // zero is a valid usize so the parser keeps it
    let parsed = parse("SELECT * FROM Movie WHERE id = 1 LIMIT 0");
    assert_eq!(parsed.limit, Some(0));
}

#[test]
/// Constraining one field twice with AND is rejected and told how to say what it means
///
/// This spelling used to parse and then be answered as a union, so `id = 1 AND id = 2`
/// returned the rows of either partition rather than the rows in both.
fn rejects_a_field_constrained_twice_by_and() {
    // two conditions on one field cannot both be satisfied by a union
    let message = parse_err("SELECT * FROM Movie WHERE id = 1 AND id = 2");
    assert!(
        message.contains("'id' is constrained twice by AND"),
        "unexpected message: {}",
        message
    );
    // the error should suggest the IN list that was meant, quoting the literals as written
    assert!(
        message.contains("id IN (1, 2)"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A field constrained twice is caught even with another condition between the two
fn rejects_a_field_constrained_twice_out_of_order() {
    // the repeat is two conditions later rather than adjacent
    let message = parse_err("SELECT * FROM Movie WHERE a = 1 AND b = 2 AND a = 3");
    assert!(
        message.contains("'a' is constrained twice by AND"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// An IN list gives one field several values
fn parses_an_in_list() {
    // every value in the list belongs to the one condition
    let condition = parse_one("SELECT * FROM Movie WHERE id IN (1, 2, 3)");
    assert_eq!(condition.field, "id");
    // and they are kept in the order they were written
    let values: Vec<Option<i64>> = condition
        .as_values()
        .expect("this field was matched rather than bounded")
        .iter()
        .map(|found| found.value.as_i64())
        .collect();
    assert_eq!(values, vec![Some(1), Some(2), Some(3)]);
}

#[test]
/// An IN list of one value is the same as an equality
fn parses_a_single_value_in_list() {
    // a list with one value in it names one value
    let condition = parse_one("SELECT * FROM Movie WHERE id IN (1)");
    assert_eq!(values_of(&condition).len(), 1);
    assert_eq!(condition.first().as_i64(), Some(1));
}

#[test]
/// The IN keyword is matched without regard to case and does not need a space before its list
fn parses_in_without_regard_to_case_or_spacing() {
    // lowercase, uppercase, and a list pushed up against the keyword all parse
    for query in [
        "SELECT * FROM Movie WHERE id in (1, 2)",
        "SELECT * FROM Movie WHERE id IN (1, 2)",
        "SELECT * FROM Movie WHERE id In(1,2)",
    ] {
        let condition = parse_one(query);
        assert_eq!(values_of(&condition).len(), 2, "failed to parse '{}'", query);
    }
}

#[test]
/// An IN list may hold any literal the parser supports
fn parses_mixed_literals_in_an_in_list() {
    // strings, numbers, booleans, and null are all values
    let condition = parse_one("SELECT * FROM Movie WHERE note IN ('a', 1, true, null)");
    assert_eq!(values_of(&condition).len(), 4);
    assert_eq!(values_of(&condition)[0].value, Value::String("a".to_string()));
    assert_eq!(values_of(&condition)[2].value, Value::Bool(true));
    assert_eq!(values_of(&condition)[3].value, Value::Null);
}

#[test]
/// A field named twice by OR is folded into one condition
fn folds_or_into_one_condition() {
    // both values belong to the one field, so there is one condition and not two
    let condition = parse_one("SELECT * FROM Movie WHERE id = 1 OR id = 2");
    assert_eq!(condition.field, "id");
    assert_eq!(values_of(&condition).len(), 2);
    assert_eq!(values_of(&condition)[0].value.as_i64(), Some(1));
    assert_eq!(values_of(&condition)[1].value.as_i64(), Some(2));
}

#[test]
/// OR and IN are two spellings of the same query
fn or_and_in_parse_the_same() {
    // parse the same set of values written both ways
    let with_or = parse_one("SELECT * FROM Movie WHERE id = 1 OR id = 2 OR id = 3");
    let with_in = parse_one("SELECT * FROM Movie WHERE id IN (1, 2, 3)");
    // pull the literals out of each so they can be compared
    let values = |condition: &WhereClause| -> Vec<Value> {
        condition
            .as_values()
        .expect("this field was matched rather than bounded")
            .iter()
            .map(|found| found.value.clone())
            .collect()
    };
    assert_eq!(values(&with_or), values(&with_in));
}

#[test]
/// The OR keyword is matched without regard to case
fn or_keyword_is_case_insensitive() {
    // a lowercase or folds the same way an uppercase one does
    let condition = parse_one("SELECT * FROM Movie WHERE id = 1 or id = 2");
    assert_eq!(values_of(&condition).len(), 2);
}

#[test]
/// OR and AND can be mixed, with OR choosing values and AND constraining another field
fn mixes_or_and_and() {
    // the two ids belong to one condition and the title to another
    let parsed = parse("SELECT * FROM Movie WHERE id = 1 OR id = 2 AND title = 'Alien'");
    assert_eq!(parsed.conditions.len(), 2);
    assert_eq!(parsed.conditions[0].field, "id");
    assert_eq!(values_of(&parsed.conditions[0]).len(), 2);
    assert_eq!(parsed.conditions[1].field, "title");
    assert_eq!(values_of(&parsed.conditions[1]).len(), 1);
}

#[test]
/// OR across two different fields is rejected
///
/// The right hand side would name rows in no partition we asked for, and a get can only
/// read the partitions it names, so there is nothing to answer it with.
fn rejects_or_across_two_fields() {
    // a partition key OR'd with a filter is not answerable
    let message = parse_err("SELECT * FROM Movie WHERE id = 550 OR title = 'Alien'");
    assert!(
        message.contains("'id' cannot be OR'd with 'title'"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// An empty IN list is rejected rather than matching nothing
fn rejects_an_empty_in_list() {
    // a list with no values in it cannot match a row
    let message = parse_err("SELECT * FROM Movie WHERE id IN ()");
    assert!(
        message.contains("IN needs at least one value for field 'id'"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// A trailing comma in an IN list is rejected
fn rejects_a_trailing_comma_in_an_in_list() {
    // a comma with nothing after it is a mistake rather than an empty value
    let message = parse_err("SELECT * FROM Movie WHERE id IN (1, 2,)");
    assert!(
        message.contains("Trailing comma"),
        "unexpected message: {}",
        message
    );
}

#[test]
/// An unclosed IN list is rejected
fn rejects_an_unclosed_in_list() {
    // a list that runs off the end of the query cannot be parsed
    let message = parse_err("SELECT * FROM Movie WHERE id IN (1, 2");
    assert!(
        message.contains("Expected ',' or ')'"),
        "unexpected message: {}",
        message
    );
    // and neither can one whose values are not parenthesised at all
    let unparenthesised = parse_err("SELECT * FROM Movie WHERE id IN 1, 2");
    assert!(
        unparenthesised.contains("Expected '(' after IN"),
        "unexpected message: {}",
        unparenthesised
    );
}

#[test]
/// Each value in an IN list records its own span
fn tracks_positions_across_an_in_list() {
    // parse a list mixing a string and a number so the spans differ in width
    let query = "SELECT * FROM Movie WHERE title IN ('Alien', 'café')";
    let condition = parse_one(query);
    // each recorded span should slice back to the literal it came from
    let first = &values_of(&condition)[0];
    assert_eq!(&query[first.start..first.end], "'Alien'");
    let second = &values_of(&condition)[1];
    assert_eq!(&query[second.start..second.end], "'café'");
}

#[test]
/// A repeated value names one partition once
fn deduplicates_repeated_values() {
    // the same literal written twice would otherwise read the same partition twice
    let condition = parse_one("SELECT * FROM Movie WHERE id IN (1, 2, 1)");
    let values: Vec<Option<i64>> = condition
        .as_values()
        .expect("this field was matched rather than bounded")
        .iter()
        .map(|found| found.value.as_i64())
        .collect();
    assert_eq!(values, vec![Some(1), Some(2)]);
    // the same holds when the repeat was written with OR
    let with_or = parse_one("SELECT * FROM Movie WHERE id = 1 OR id = 1");
    assert_eq!(values_of(&with_or).len(), 1);
}

#[test]
/// A dangling OR with no comparison after it is rejected
fn rejects_a_dangling_or() {
    // an OR with nothing after it must not be silently dropped
    let message = parse_err("SELECT * FROM Movie WHERE id = 1 OR");
    assert!(
        message.contains("trailing input"),
        "unexpected message: {}",
        message
    );
    // when the OR is followed by whitespace we get as far as looking for the next field
    let trailing = parse_err("SELECT * FROM Movie WHERE id = 1 OR   ");
    assert!(
        trailing.contains("Expected a field name"),
        "unexpected message: {}",
        trailing
    );
}

#[test]
/// A field name that merely starts with a keyword is still a field name
fn does_not_mistake_a_prefix_for_a_connective() {
    // 'organisation' and 'android' both start with a connective keyword
    let parsed = parse("SELECT * FROM Movie WHERE id = 1 AND organisation = 'x'");
    assert_eq!(parsed.conditions[1].field, "organisation");
    let anded = parse("SELECT * FROM Movie WHERE id = 1 AND android = 'x'");
    assert_eq!(anded.conditions[1].field, "android");
    // and a field named 'inn' is not an IN list
    let inn = parse_one("SELECT * FROM Movie WHERE inn = 1");
    assert_eq!(inn.field, "inn");
}

#[test]
/// Multibyte input does not panic while an error is rendered
fn error_display_handles_multibyte_input() {
    // a span landing in the middle of a multibyte character falls back to the whole input
    let split = ShqlParseError::new("boom", 0, 1, "é");
    assert!(split.to_string().contains("boom"));
    // a span running past the end of the input does the same
    let past_end = ShqlParseError::new("boom", 0, 100, "abc");
    assert!(past_end.to_string().contains("boom"));
    // and a real parse failure over multibyte input renders without panicking
    let error = ParsedSelect::new("SELECT * FROM Movie WHERE title = 'café' garbage")
        .expect_err("expected trailing input to fail");
    assert!(!error.to_string().is_empty());
}

#[test]
/// A dangling AND with no condition after it is rejected
fn rejects_a_dangling_and() {
    // an AND with nothing after it must not be silently dropped
    let message = parse_err("SELECT * FROM Movie WHERE id = 1 AND");
    assert!(
        message.contains("trailing input"),
        "unexpected message: {}",
        message
    );
    // when the AND is followed by whitespace we get as far as looking for the next field
    let trailing = parse_err("SELECT * FROM Movie WHERE id = 1 AND   ");
    assert!(
        trailing.contains("Expected a field name"),
        "unexpected message: {}",
        trailing
    );
}

#[test]
/// Values containing multibyte characters keep spans that slice cleanly
fn tracks_positions_across_multibyte_values() {
    // parse a query whose literal contains multibyte characters
    let query = "SELECT * FROM Movie WHERE title = 'café'";
    let parsed = parse(query);
    // the recorded span should still slice back to the quoted literal
    let title = &parsed.conditions[0];
    assert_eq!(&query[values_of(&title)[0].start..values_of(&title)[0].end], "'café'");
}
