//! Tests for working out where a cursor sits in a partially typed query
//!
//! These cover [`analyze`] only, which knows nothing about any schema. Turning a position into
//! actual table and field names is covered by the tests in the `shoal` crate, where a real
//! database schema is available.

use super::{analyze, Expecting};

/// Analyze a query with the cursor at the end of it
///
/// # Arguments
///
/// * `query` - The query to analyze
fn at_end(query: &str) -> super::CompletionContext {
    analyze(query, query.len())
}

#[test]
fn empty_query_expects_select() {
    let context = at_end("");
    assert_eq!(context.expecting, Expecting::Select);
    assert_eq!(context.word, "");
    assert_eq!(context.word_start, 0);
}

#[test]
fn partial_select_expects_select() {
    let context = at_end("SEL");
    assert_eq!(context.expecting, Expecting::Select);
    assert_eq!(context.word, "SEL");
    assert_eq!(context.word_start, 0);
    assert_eq!(context.word_end, 3);
}

#[test]
fn walks_the_opening_keywords() {
    assert_eq!(at_end("SELECT ").expecting, Expecting::Star);
    assert_eq!(at_end("SELECT * ").expecting, Expecting::From);
    assert_eq!(at_end("SELECT * FROM ").expecting, Expecting::Table);
    assert_eq!(at_end("SELECT * FROM Movie ").expecting, Expecting::Where);
}

#[test]
fn keywords_are_case_insensitive() {
    assert_eq!(at_end("select * from ").expecting, Expecting::Table);
    assert_eq!(at_end("SeLeCt * FrOm Movie WhErE ").expecting, Expecting::Field);
}

#[test]
fn partial_table_name_is_the_word() {
    let context = at_end("SELECT * FROM Mov");
    assert_eq!(context.expecting, Expecting::Table);
    assert_eq!(context.word, "Mov");
    assert_eq!(context.word_start, 14);
}

#[test]
fn tracks_the_table_being_read() {
    let context = at_end("SELECT * FROM Movie WHERE ");
    assert_eq!(context.expecting, Expecting::Field);
    assert_eq!(context.table.as_deref(), Some("Movie"));
    assert_eq!(context.word, "");
}

#[test]
fn partial_field_name_is_the_word() {
    let context = at_end("SELECT * FROM Movie WHERE ti");
    assert_eq!(context.expecting, Expecting::Field);
    assert_eq!(context.table.as_deref(), Some("Movie"));
    assert_eq!(context.word, "ti");
}

#[test]
fn field_without_a_value_expects_equals() {
    let context = at_end("SELECT * FROM Movie WHERE id ");
    assert_eq!(context.expecting, Expecting::Equals);
}

#[test]
fn expects_a_value_after_equals() {
    let context = at_end("SELECT * FROM Movie WHERE watched = ");
    assert_eq!(
        context.expecting,
        Expecting::Value {
            field: "watched".to_string()
        }
    );
    assert_eq!(context.table.as_deref(), Some("Movie"));
}

#[test]
fn partial_value_keeps_the_field() {
    let context = at_end("SELECT * FROM Movie WHERE watched = tr");
    assert_eq!(
        context.expecting,
        Expecting::Value {
            field: "watched".to_string()
        }
    );
    assert_eq!(context.word, "tr");
}

#[test]
fn expects_a_continuation_after_a_value() {
    assert_eq!(
        at_end("SELECT * FROM Movie WHERE id = 550 ").expecting,
        Expecting::Continuation
    );
    assert_eq!(
        at_end("SELECT * FROM Movie WHERE title = 'Alien' ").expecting,
        Expecting::Continuation
    );
    assert_eq!(
        at_end("SELECT * FROM Movie WHERE watched = true ").expecting,
        Expecting::Continuation
    );
}

#[test]
fn expects_a_field_after_and() {
    let context = at_end("SELECT * FROM Movie WHERE id = 550 AND ");
    assert_eq!(context.expecting, Expecting::Field);
    assert_eq!(context.table.as_deref(), Some("Movie"));
}

#[test]
fn walks_a_limit_clause() {
    assert_eq!(
        at_end("SELECT * FROM Movie WHERE id = 550 LIMIT ").expecting,
        Expecting::LimitCount
    );
    assert_eq!(
        at_end("SELECT * FROM Movie WHERE id = 550 LIMIT 10 ").expecting,
        Expecting::End
    );
}

#[test]
fn a_finished_query_expects_nothing() {
    assert_eq!(
        at_end("SELECT * FROM Movie WHERE id = 550;").expecting,
        Expecting::Nothing
    );
}

#[test]
fn a_malformed_query_expects_nothing() {
    assert_eq!(at_end("DELETE * FROM Movie ").expecting, Expecting::Nothing);
    assert_eq!(at_end("SELECT id FROM Movie ").expecting, Expecting::Nothing);
}

#[test]
fn a_cursor_in_the_middle_only_sees_what_is_behind_it() {
    let query = "SELECT * FROM Movie WHERE id = 550";
    // put the cursor in the middle of the table name
    let cursor = "SELECT * FROM Mov".len();
    let context = analyze(query, cursor);
    assert_eq!(context.expecting, Expecting::Table);
    assert_eq!(context.word, "Mov");
    assert_eq!(context.word_start, 14);
    assert_eq!(context.word_end, cursor);
    // the table hasn't been fully typed yet as far as the cursor is concerned
    assert_eq!(context.table, None);
}

#[test]
fn a_cursor_inside_a_string_literal_suggests_nothing() {
    let context = at_end("SELECT * FROM Movie WHERE title = 'Ali");
    assert_eq!(context.expecting, Expecting::Nothing);
}

#[test]
fn handles_multibyte_identifiers() {
    let context = at_end("SELECT * FROM Ünsorted WHERE café");
    assert_eq!(context.expecting, Expecting::Field);
    assert_eq!(context.table.as_deref(), Some("Ünsorted"));
    assert_eq!(context.word, "café");
    // the word starts after "SELECT * FROM Ünsorted WHERE ", which is not its character count
    assert_eq!(&context.word_start, &"SELECT * FROM Ünsorted WHERE ".len());
}

#[test]
fn handles_a_multibyte_value_before_the_cursor() {
    let context = at_end("SELECT * FROM Movie WHERE title = 'café' ");
    assert_eq!(context.expecting, Expecting::Continuation);
}

#[test]
fn a_cursor_past_the_end_is_clamped() {
    let query = "SELECT * FROM ";
    let context = analyze(query, query.len() + 100);
    assert_eq!(context.expecting, Expecting::Table);
    assert_eq!(context.word_end, query.len());
}
