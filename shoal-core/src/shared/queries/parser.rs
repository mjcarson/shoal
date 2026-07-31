//! Parses SELECT queries from a string in shoal
//!
//! SHQL — Shoal Query Language — is a deliberately small SQL-like language for read queries. It
//! exists so that a human can type a query into `shoalctl` rather than building one out of the
//! generated `*Get` structs. It is not a general query language.
//!
//! # Grammar
//!
//! ```text
//! query      := ws "SELECT" ws1 "*" ws1 "FROM" ws1 identifier where [ws limit] [ws ";"] ws eof
//! where      := ws1 "WHERE" ws1 condition { ws "AND" ws1 condition }
//! condition  := identifier ws "=" ws value
//! limit      := "LIMIT" ws1 digits
//! value      := string | float | integer | boolean | null
//! string     := "'" { any character except "'" } "'"
//! float      := ["-" | "+"] digits "." digits
//! integer    := ["-" | "+"] digits
//! boolean    := "true" | "false"
//! null       := "null"
//! identifier := xid_start { xid_continue }
//! ```
//!
//! Keywords and the `true`/`false`/`null` literals are case-insensitive. Identifiers are not —
//! the name after `FROM` is matched against the table's Rust struct name.
//!
//! Identifiers follow the same rules as Rust identifiers, which is to say [Unicode Standard
//! Annex #31](https://www.unicode.org/reports/tr31/): they start with an `XID_Start` character
//! or an underscore and continue with `XID_Continue` characters. Table and field names in a
//! query are Rust identifiers, so anything Rust accepts must parse here too.
//!
//! # Parsing a query
//!
//! ```
//! use shoal_core::shared::queries::parser::ParsedSelect;
//!
//! let parsed = ParsedSelect::new("SELECT * FROM Movie WHERE id = 550 LIMIT 10")?;
//!
//! assert_eq!(parsed.table_name, "Movie");
//! assert_eq!(parsed.conditions.len(), 1);
//! assert_eq!(parsed.conditions[0].field, "id");
//! assert_eq!(parsed.limit, Some(10));
//! # Ok::<(), shoal_core::client::ShqlParseError>(())
//! ```
//!
//! Keywords are case-insensitive and the trailing semicolon is optional, so this is the same
//! query:
//!
//! ```
//! use shoal_core::shared::queries::parser::ParsedSelect;
//!
//! let parsed = ParsedSelect::new("select * from Movie where id = 550 limit 10;")?;
//!
//! assert_eq!(parsed.table_name, "Movie");
//! assert_eq!(parsed.limit, Some(10));
//! # Ok::<(), shoal_core::client::ShqlParseError>(())
//! ```
//!
//! Conditions are joined with `AND` and kept in the order they were written:
//!
//! ```
//! use shoal_core::shared::queries::parser::ParsedSelect;
//!
//! let parsed = ParsedSelect::new("SELECT * FROM Movie WHERE id = 550 AND title = 'Alien'")?;
//!
//! let fields: Vec<&str> = parsed.conditions.iter().map(|c| c.field.as_str()).collect();
//! assert_eq!(fields, vec!["id", "title"]);
//! # Ok::<(), shoal_core::client::ShqlParseError>(())
//! ```
//!
//! # Two stages
//!
//! [`ParsedSelect::new`] handles syntax only and knows nothing about any schema — it produces
//! field names paired with [`serde_json::Value`] literals. Binding those to a concrete table
//! (checking the field exists, that the literal fits the field's type, and sorting conditions
//! into partition keys, sort keys, and filters) happens in the `QuerySupport::parse` impl
//! generated per database by `shoal-derive`.
//!
//! Each [`WhereClause`] carries the byte offsets of its literal in the original query so that
//! errors raised during either stage can point at the offending value.
//!
//! # What is not supported
//!
//! - Only `SELECT *`. There is no projection.
//! - Only `=`. No `<`, `>`, `!=`, `LIKE`, `IN`, or `BETWEEN`.
//! - Only `AND`. No `OR` and no parentheses.
//! - No `ORDER BY`, `GROUP BY`, `JOIN`, or aggregates.
//! - No `INSERT`, `UPDATE`, or `DELETE` — writes must be built as typed queries.
//! - A `WHERE` clause is mandatory, and it must constrain a partition key.
//! - String literals have no escape syntax, so a string cannot contain a single quote.
//! - Floats need digits on both sides of the point and have no exponent form.

use serde_json::Value;
use std::any;
use winnow::ascii::{digit1, multispace0, multispace1};
use winnow::combinator::{alt, delimited, opt};
use winnow::prelude::*;
use winnow::token::{take_till, take_while};

use crate::client::ShqlParseError;

mod complete;

#[cfg(test)]
mod tests;

pub use complete::{CompletionContext, Expecting, analyze};

#[cfg(feature = "shql-complete")]
pub use complete::{Completions, Suggestion, SuggestionKind, suggest};

/// A where clause in a query (e.g. where <field> = <value>)
#[derive(Debug, Clone)]
pub struct WhereClause {
    /// The name of the field this clause is for
    pub field: String,
    /// The value to check against when performing this query
    pub value: Value,
    /// The start position of this value in the original query string
    pub value_start: usize,
    /// The end position of this value in the original query string
    pub value_end: usize,
}

/// The kind of field a where clause is restricting
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldRole {
    /// This where clause applies to a partition key
    Partition,
    /// This where clause applies to a sort key
    Sort,
    /// This where clause applies to a filter column
    Filter,
}

/// A single field in a table and the role it plays in queries
///
/// This is what a client uses to describe a table's schema to a user, so it carries the field
/// name as it must be typed in a query and the role that decides where a where clause on that
/// field ends up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldInfo {
    /// The name of this field
    pub name: &'static str,
    /// The role this field plays in a query, if it has one
    pub role: Option<FieldRole>,
}

/// A numeric literal that cannot be represented by the type it was parsed as
///
/// This is only ever handed to winnow to turn a numeric conversion failure into a recoverable
/// parse error, so it carries no detail of its own.
#[derive(Debug)]
struct InvalidNumber;

impl std::fmt::Display for InvalidNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "numeric literal is out of range")
    }
}

impl std::error::Error for InvalidNumber {}

/// Type validator for a field
pub type TypeValidator = fn(&Value) -> Result<String, String>;

/// Create a type validator for any type that implements serde::DeserializeOwned
///
/// This function returns a closure that can validate whether a Value can be
/// deserialized into the specified type T.
///
/// # Type Parameters
///
/// * `T` - The type to validate against, must implement serde::DeserializeOwned
///
/// # Returns
///
/// A TypeValidator function that takes a Value and returns Ok with the type name
/// if validation succeeds, or Err with an error message if validation fails
pub fn make_validator<T: serde::de::DeserializeOwned + 'static>() -> TypeValidator {
    |value: &Value| {
        serde_json::from_value::<T>(value.clone())
            .map(|_| any::type_name::<T>().to_string())
            .map_err(|e| format!("Cannot deserialize to {}: {}", any::type_name::<T>(), e))
    }
}

/// Parse whitespace (zero or more whitespace characters)
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// The parsed whitespace as a string slice on success, or a parse error
fn ws<'s>(input: &mut &'s str) -> winnow::Result<&'s str> {
    multispace0.parse_next(input)
}

/// Parse an identifier (table name, field name)
///
/// Identifiers must start with a letter or underscore, followed by any number
/// of alphanumeric characters or underscores.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// The parsed identifier as a String on success, or a parse error
fn identifier<'s>(input: &mut &'s str) -> winnow::Result<String> {
    (
        take_while(1, is_ident_start),
        take_while(0.., is_ident_continue),
    )
        .map(|(first, rest): (&str, &str)| format!("{}{}", first, rest))
        .parse_next(input)
}

/// Whether a character can start an identifier
///
/// Identifiers in shoal are Rust identifiers, so this follows [UAX
/// #31](https://www.unicode.org/reports/tr31/): an `XID_Start` character or an underscore.
///
/// # Arguments
///
/// * `c` - The character to check
pub fn is_ident_start(c: char) -> bool {
    c == '_' || unicode_ident::is_xid_start(c)
}

/// Whether a character can continue an identifier
///
/// Identifiers in shoal are Rust identifiers, so this follows [UAX
/// #31](https://www.unicode.org/reports/tr31/): any `XID_Continue` character.
///
/// # Arguments
///
/// * `c` - The character to check
pub fn is_ident_continue(c: char) -> bool {
    unicode_ident::is_xid_continue(c)
}

/// Parse a string literal (single-quoted)
///
/// Parses a string enclosed in single quotes ('string').
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// A Value::String containing the parsed string on success, or a parse error
fn string_literal<'s>(input: &mut &'s str) -> winnow::Result<Value> {
    delimited("'", take_till(0.., |c| c == '\''), "'")
        .map(|s: &str| Value::String(s.to_string()))
        .parse_next(input)
}

/// Parse a boolean literal
///
/// Parses the keywords 'true' or 'false' (case-insensitive).
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// A Value::Bool on success, or a parse error
fn boolean_literal(input: &mut &str) -> winnow::Result<Value> {
    alt((
        winnow::ascii::Caseless("true").map(|_| Value::Bool(true)),
        winnow::ascii::Caseless("false").map(|_| Value::Bool(false)),
    ))
    .parse_next(input)
}

/// Parse a null literal
///
/// Parses the keyword 'null' (case-insensitive).
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// A Value::Null on success, or a parse error
fn null_literal(input: &mut &str) -> winnow::Result<Value> {
    winnow::ascii::Caseless("null")
        .map(|_| Value::Null)
        .parse_next(input)
}

/// Parse a floating-point number
///
/// Parses a number with a decimal point (e.g., 3.14, -0.5, +42.0). A literal too large to be
/// represented as a finite `f64` is a parse error rather than a panic.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// A Value::Number containing the parsed float on success, or a parse error
fn float_number(input: &mut &str) -> winnow::Result<Value> {
    (opt(alt(("-", "+"))), digit1, ".", digit1)
        .try_map(
            |(sign, int_part, _, dec_part): (Option<&str>, &str, &str, &str)| {
                // rebuild the literal with its sign so we can parse it as one float
                let float_str = format!("{}{}.{}", sign.unwrap_or(""), int_part, dec_part);
                // parse the literal, failing this branch if it does not fit in an f64
                let value: f64 = float_str.parse().map_err(|_| InvalidNumber)?;
                // json has no representation for infinity or NaN so reject those too
                let number = serde_json::Number::from_f64(value).ok_or(InvalidNumber)?;
                Ok::<Value, InvalidNumber>(Value::Number(number))
            },
        )
        .parse_next(input)
}

/// Parse an integer number
///
/// Parses a whole number with optional sign (e.g., 42, -17, +100). A literal that does not fit
/// in an `i64` is a parse error rather than a panic.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// A Value::Number containing the parsed integer on success, or a parse error
fn integer_number(input: &mut &str) -> winnow::Result<Value> {
    (opt(alt(("-", "+"))), digit1)
        .try_map(|(sign, digits): (Option<&str>, &str)| {
            // rebuild the literal with its sign so we can parse it as one integer
            let int_str = format!("{}{}", sign.unwrap_or(""), digits);
            // parse the literal, failing this branch if it does not fit in an i64
            let value: i64 = int_str.parse().map_err(|_| InvalidNumber)?;
            Ok::<Value, InvalidNumber>(Value::Number(serde_json::Number::from(value)))
        })
        .parse_next(input)
}

/// Parse a number (try float first, then integer)
///
/// Attempts to parse a floating-point number first, falling back to integer if no decimal point is found.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// A Value::Number on success, or a parse error
fn number(input: &mut &str) -> winnow::Result<Value> {
    alt((float_number, integer_number)).parse_next(input)
}

/// Parse a value (string, number, boolean, or null)
///
/// Attempts to parse any valid JSON value type in order: string, number, boolean, or null.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// A Value on success, or a parse error
fn value(input: &mut &str) -> winnow::Result<Value> {
    alt((string_literal, number, boolean_literal, null_literal)).parse_next(input)
}

/// Check whether the input begins with a keyword followed by whitespace
///
/// This is a lookahead used to tell "the clause is absent" apart from "the clause is malformed"
/// so each case can get its own error message. The trailing whitespace requirement keeps a field
/// named `wherever` from being mistaken for a `WHERE` keyword.
///
/// # Arguments
///
/// * `input` - The remaining input to check
/// * `keyword` - The keyword to look for, matched case-insensitively
///
/// # Returns
///
/// True if the input starts with this keyword followed by whitespace
fn starts_with_keyword(input: &str, keyword: &str) -> bool {
    // grab exactly as many bytes as the keyword, bailing out if they are not a char boundary
    match input.get(..keyword.len()) {
        // the keyword matched so make sure whitespace follows it
        Some(found) if found.eq_ignore_ascii_case(keyword) => {
            input[keyword.len()..].starts_with(char::is_whitespace)
        }
        _ => false,
    }
}

/// Parse "SELECT * FROM <table>"
///
/// Parses the SELECT clause of a SHQL query.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// The table name as a String on success, or a parse error
fn select_from<'s>(input: &mut &'s str) -> winnow::Result<String> {
    (
        winnow::ascii::Caseless("SELECT"),
        multispace1,
        "*",
        multispace1,
        winnow::ascii::Caseless("FROM"),
        multispace1,
        identifier,
    )
        // we only care about the table name
        .map(|(_, _, _, _, _, _, table)| table)
        .parse_next(input)
}

/// Parse a single WHERE condition "<field> = <value>"
///
/// Parses a single condition in a WHERE clause and tracks the position of the value
/// in the original query string for error reporting.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
/// * `original` - The original complete query string (used to calculate positions)
///
/// # Returns
///
/// A WhereClause containing the field name, value, and position information on success,
/// or a parse error
fn where_conditions_helper<'a>(
    input: &mut &'a str,
    original: &'a str,
) -> Result<WhereClause, ShqlParseError> {
    // record where this condition starts so every error below can point at it
    let condition_start = original.len() - input.len();
    // get the name of the field that we are parsing a where condition for
    let field = identifier.parse_next(input).map_err(|_| {
        ShqlParseError::at_position("Expected a field name in the WHERE clause", condition_start, original)
    })?;
    // skip whitespace
    let _ = ws.parse_next(input);
    // right now we only support '=' signs
    let equals: winnow::Result<&str> = "=".parse_next(input);
    equals.map_err(|_| {
        ShqlParseError::at_position(
            format!(
                "Expected '=' after field '{}', SHQL only supports equality",
                field
            ),
            original.len() - input.len(),
            original,
        )
    })?;
    // skip whitespace
    let _ = ws.parse_next(input);
    // Track the position before and after parsing the value
    let value_start = original.len() - input.len();
    // parse the value we are checking
    let val = value.parse_next(input).map_err(|_| {
        ShqlParseError::at_position(
            format!(
                "Expected a value for field '{}', which must be a quoted string, a number, a boolean, or null",
                field
            ),
            value_start,
            original,
        )
    })?;
    // get the end position of this value for nice errors
    let value_end = original.len() - input.len();
    // build our where clause
    Ok(WhereClause {
        field,
        value: val,
        value_start,
        value_end,
    })
}

/// Parse multiple WHERE conditions separated by AND
///
/// Parses a WHERE clause containing one or more conditions joined by the AND keyword.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
/// * `original` - The original complete query string (used to calculate positions)
///
/// # Returns
///
/// The conditions in this WHERE clause, in the order they were written
fn where_conditions<'a>(
    input: &mut &'a str,
    original: &'a str,
) -> Result<Vec<WhereClause>, ShqlParseError> {
    // consume the whitespace and the WHERE keyword, which the caller has already checked for
    let keyword: winnow::Result<(&str, &str, &str)> =
        (multispace1, winnow::ascii::Caseless("WHERE"), multispace1).parse_next(input);
    keyword.map_err(|_| {
        ShqlParseError::at_position(
            "Expected a WHERE clause",
            original.len() - input.len(),
            original,
        )
    })?;
    // parse the first of any where conditions in this query
    let mut conditions = vec![where_conditions_helper(input, original)?];
    // continue to parse any where conditions chained by an 'and'
    //
    // this has to go through opt so that a failed match restores the input, otherwise a
    // dangling 'AND' would be consumed and then silently ignored
    loop {
        // look for an AND joining another condition onto this clause
        let chained: winnow::Result<Option<(&str, &str, &str)>> =
            opt((ws, winnow::ascii::Caseless("AND"), multispace1)).parse_next(input);
        // stop once there are no more conditions chained on
        if !matches!(chained, Ok(Some(_))) {
            break;
        }
        // parse this where condition
        conditions.push(where_conditions_helper(input, original)?);
    }
    Ok(conditions)
}

/// Parse a LIMIT clause "LIMIT <number>"
///
/// Parses an optional LIMIT clause that restricts the number of results returned.
/// Note: Expects whitespace before LIMIT to already be consumed by the caller.
///
/// A limit that does not fit in a `usize` is a parse error rather than a panic.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
fn limit_clause(input: &mut &str) -> winnow::Result<usize> {
    // parse the LIMIT keyword (case-insensitive)
    winnow::ascii::Caseless("LIMIT").parse_next(input)?;
    // skip any whitespace after LIMIT
    multispace1.parse_next(input)?;
    // parse the limit number, failing if it does not fit in a usize
    digit1
        .try_map(|digits: &str| digits.parse::<usize>().map_err(|_| InvalidNumber))
        .parse_next(input)
}

/// A parsed SELECT query
#[derive(Debug, Clone)]
pub struct ParsedSelect {
    /// The name of the table this query is for
    pub table_name: String,
    /// The uncategorized conditions in this query
    pub conditions: Vec<WhereClause>,
    /// Optional limit on the number of results to return
    pub limit: Option<usize>,
}

impl ParsedSelect {
    /// Parse a complete SHQL SELECT statement (returns intermediate representation)
    ///
    /// Parses a full SHQL SELECT query including the SELECT clause, optional WHERE clause,
    /// optional LIMIT clause, and optional semicolon terminator.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to parse
    ///
    /// # Returns
    ///
    /// A ParsedSelect containing the table name, WHERE conditions, and optional LIMIT on success, or a parse error
    pub fn new(query: &str) -> Result<Self, ShqlParseError> {
        // create a mutable str for winnow to consume as it parses
        let mut parsable = query;
        // parse and consume any leading whitespace before the SELECT keyword
        ws.parse_next(&mut parsable)
            .map_err(|e| ShqlParseError::at_position(format!("Parse error: {}", e), 0, query))?;
        // parse the SELECT * FROM <table> clause and extract the table name
        let table_name = select_from.parse_next(&mut parsable).map_err(|e| {
            ShqlParseError::at_position(format!("Expected SELECT * FROM <table>: {}", e), 0, query)
        })?;
        // bail out early with a descriptive error if there is no WHERE clause to parse, so a
        // missing clause does not get a generic failure from inside the WHERE parser
        if !starts_with_keyword(parsable.trim_start(), "WHERE") {
            return Err(ShqlParseError::at_position(
                "A WHERE clause is required, and it must constrain a partition key",
                query.len() - parsable.len(),
                query,
            ));
        }
        // parse the conditions making up the WHERE clause
        let conditions = where_conditions(&mut parsable, query)?;
        // parse any whitespace after the WHERE clause
        ws.parse_next(&mut parsable)
            .map_err(|e| ShqlParseError::at_position(format!("Parse error: {}", e), 0, query))?;
        // parse a LIMIT clause if one is present, reporting a malformed one rather than letting
        // it fall through and get reported as trailing input
        let limit = if starts_with_keyword(parsable, "LIMIT") {
            // record where the clause starts so we can point at it if it is malformed
            let limit_start = query.len() - parsable.len();
            // parse the clause, failing the whole query if the limit itself is unusable
            let parsed_limit = limit_clause(&mut parsable).map_err(|e| {
                ShqlParseError::at_position(
                    format!("Error parsing LIMIT clause: {}", e),
                    limit_start,
                    query,
                )
            })?;
            Some(parsed_limit)
        } else {
            None
        };
        // parse any whitespace after the LIMIT clause (or after WHERE if no LIMIT)
        let _ = ws.parse_next(&mut parsable);
        // optionally parse a semicolon terminator if present
        let _ = opt::<_, _, winnow::error::ContextError, _>(";").parse_next(&mut parsable);
        // parse any trailing whitespace after the optional semicolon
        let _ = ws.parse_next(&mut parsable);
        // reject anything left over so a malformed tail is not silently discarded
        if !parsable.is_empty() {
            return Err(ShqlParseError::at_position(
                format!("Unexpected trailing input: '{}'", parsable),
                query.len() - parsable.len(),
                query,
            ));
        }
        // return the successfully parsed query structure
        Ok(ParsedSelect {
            table_name,
            conditions,
            limit,
        })
    }
}
