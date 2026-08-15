//! Parses SELECT queries from a string in shoal
//!
//! SHQL — Shoal Query Language — is a deliberately small SQL-like language for read queries. It
//! exists so that a human can type a query into `shoalctl` rather than building one out of the
//! generated `*Get` structs. It is not a general query language.
//!
//! # Grammar
//!
//! ```text
//! query      := ws "SELECT" ws1 projection ws1 "FROM" ws1 identifier where [ws limit] [ws ";"] ws eof
//! projection := "*" | identifier
//! where      := ws1 "WHERE" ws1 condition { ws "AND" ws1 condition }
//! condition  := comparison { ws "OR" ws1 comparison }
//! comparison := identifier ws ( "=" ws value
//!                             | "IN" ws "(" ws value { ws "," ws value } ws ")"
//!                             | range_op ws value )
//! range_op   := ">=" | "<=" | ">" | "<"
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
//! use shoal_proto::shared::queries::parser::ParsedSelect;
//!
//! let parsed = ParsedSelect::new("SELECT * FROM Movie WHERE id = 550 LIMIT 10")?;
//!
//! assert_eq!(parsed.table_name, "Movie");
//! assert_eq!(parsed.conditions.len(), 1);
//! assert_eq!(parsed.conditions[0].field, "id");
//! assert_eq!(parsed.limit, Some(10));
//! # Ok::<(), shoal_proto::client::ShqlParseError>(())
//! ```
//!
//! # Choosing between values
//!
//! A field can be given several values at once with `IN`, which for a partition key means
//! reading every one of those partitions:
//!
//! ```
//! use shoal_proto::shared::queries::parser::ParsedSelect;
//!
//! let parsed = ParsedSelect::new("SELECT * FROM Movie WHERE id IN (550, 551)")?;
//!
//! assert_eq!(parsed.conditions.len(), 1);
//! assert_eq!(parsed.conditions[0].values().count(), 2);
//! # Ok::<(), shoal_proto::client::ShqlParseError>(())
//! ```
//!
//! `OR` is spelled differently and means the same thing, so it folds into the same clause:
//!
//! ```
//! use shoal_proto::shared::queries::parser::ParsedSelect;
//!
//! let with_or = ParsedSelect::new("SELECT * FROM Movie WHERE id = 550 OR id = 551")?;
//! let with_in = ParsedSelect::new("SELECT * FROM Movie WHERE id IN (550, 551)")?;
//!
//! assert_eq!(with_or.conditions.len(), with_in.conditions.len());
//! assert_eq!(
//!     with_or.conditions[0].values().count(),
//!     with_in.conditions[0].values().count(),
//! );
//! # Ok::<(), shoal_proto::client::ShqlParseError>(())
//! ```
//!
//! # Bounding a field
//!
//! A field can be bounded instead of matched, with `<`, `<=`, `>`, or `>=`. Binding refuses
//! this on anything but a sort key, since a partition is located by its exact key and a filter
//! is a membership test - but the grammar itself does not know about roles:
//!
//! ```
//! use shoal_proto::shared::queries::parser::ParsedSelect;
//!
//! let parsed = ParsedSelect::new("SELECT * FROM Review WHERE movie = 550 AND reviewer > 'a'")?;
//!
//! let range = parsed.conditions[1].as_range().expect("reviewer is bounded");
//! assert!(!range.lower.as_ref().expect("a lower bound").inclusive);
//! assert!(range.upper.is_none());
//! # Ok::<(), shoal_proto::client::ShqlParseError>(())
//! ```
//!
//! **A range is the one shape where `AND` may name a field twice.** The two comparisons bound
//! opposite ends and are folded into a single clause, so everything downstream still sees one
//! clause per field:
//!
//! ```
//! use shoal_proto::shared::queries::parser::ParsedSelect;
//!
//! let parsed = ParsedSelect::new(
//!     "SELECT * FROM Review WHERE movie = 550 AND reviewer >= 'a' AND reviewer < 'm'",
//! )?;
//!
//! assert_eq!(parsed.conditions.len(), 2);
//! let range = parsed.conditions[1].as_range().expect("reviewer is bounded");
//! assert!(range.lower.as_ref().expect("a lower bound").inclusive);
//! assert!(!range.upper.as_ref().expect("an upper bound").inclusive);
//! # Ok::<(), shoal_proto::client::ShqlParseError>(())
//! ```
//!
//! Keywords are case-insensitive and the trailing semicolon is optional, so this is the same
//! query:
//!
//! ```
//! use shoal_proto::shared::queries::parser::ParsedSelect;
//!
//! let parsed = ParsedSelect::new("select * from Movie where id = 550 limit 10;")?;
//!
//! assert_eq!(parsed.table_name, "Movie");
//! assert_eq!(parsed.limit, Some(10));
//! # Ok::<(), shoal_proto::client::ShqlParseError>(())
//! ```
//!
//! Conditions on different fields are joined with `AND` and kept in the order they were
//! written:
//!
//! ```
//! use shoal_proto::shared::queries::parser::ParsedSelect;
//!
//! let parsed = ParsedSelect::new("SELECT * FROM Movie WHERE id = 550 AND title = 'Alien'")?;
//!
//! let fields: Vec<&str> = parsed.conditions.iter().map(|c| c.field.as_str()).collect();
//! assert_eq!(fields, vec!["id", "title"]);
//! # Ok::<(), shoal_proto::client::ShqlParseError>(())
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
//! Each [`WhereValue`] carries the byte offsets of its literal in the original query, and each
//! [`WhereClause`] the offsets of its field name, so that errors raised during either stage can
//! point at whatever was written wrong.
//!
//! # What is not supported
//!
//! - A projection is a **named type**, not a column list. `SELECT *` asks for whole rows and
//!   `SELECT MovieSummary` asks for one of the projections that table declared. There is no
//!   `SELECT title, year`, because the rows come back as an archive of a concrete type and an
//!   arbitrary column list has no type to be.
//! - `=`, `IN`, and the range operators `<`, `<=`, `>`, `>=`. No `!=`, `LIKE`, or `BETWEEN`.
//! - A range is only bindable on a **sort key**. Ranges on a partition key or a filter parse
//!   and are then refused during binding, because a partition is located by its exact key and
//!   a filter is a membership test.
//! - `OR` only joins conditions on the same field, where it means the same thing as `IN`, and
//!   it cannot join a range at all. A partition key cannot be `OR`'d with a filter, because the
//!   only access path is by partition key and there is no scan to answer the other side with.
//!   There are no parentheses.
//! - A field cannot be constrained twice by `AND` unless the two conditions bound opposite ends
//!   of one range. Two values for one field are a union, not an intersection, so that has to be
//!   written with `IN` to say what it means.
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

/// One literal in a where clause, and the span it occupied in the query
#[derive(Debug, Clone)]
pub struct WhereValue {
    /// The value to check against when performing this query
    pub value: Value,
    /// The start position of this value in the original query string
    pub start: usize,
    /// The end position of this value in the original query string
    pub end: usize,
}

/// One end of a range, and whether it includes the value it names
#[derive(Debug, Clone)]
pub struct WhereBound {
    /// The value this end of the range is bounded at
    pub value: WhereValue,
    /// Whether this end includes the value it names, so `>=` rather than `>`
    pub inclusive: bool,
}

/// The bounds a range clause puts on its field
///
/// A comparison names one end, so `title > 'a'` bounds only the lower one. Both ends are set
/// when two comparisons on the same field are joined by `AND`, which is the one shape where a
/// field may be named twice.
#[derive(Debug, Clone, Default)]
pub struct WhereRange {
    /// The lower bound this field was given, from `>` or `>=`
    pub lower: Option<WhereBound>,
    /// The upper bound this field was given, from `<` or `<=`
    pub upper: Option<WhereBound>,
}

/// What a where clause constrains its field to
///
/// The two arms are different questions and are answered by different access paths, so every
/// consumer of a clause has to say which of them it can take. That is the point of the enum:
/// a range on a partition key or on a filter has no meaning, and this makes each of those a
/// place that has to decide rather than a case that falls through.
#[derive(Debug, Clone)]
pub enum WhereConstraint {
    /// One of a set of values, from `=`, from `IN`, or from an `OR` of the same field
    Values(Vec<WhereValue>),
    /// A range of values, from `<`, `<=`, `>`, or `>=`
    Range(WhereRange),
}

/// A field constrained by a where clause, and what it is constrained to
///
/// `field IN ('a', 'b')` and `field = 'a' OR field = 'b'` are two spellings of one query, so
/// both parse into a single clause holding both values. A field bounded from both ends is two
/// comparisons and still parses into a single clause, because the two are folded together
/// while the `WHERE` clause is being read.
///
/// A field is only ever named by one clause, which is what leaves `AND` meaning a conjunction
/// across fields and nothing else, and what lets everything downstream look a field up rather
/// than search for it.
#[derive(Debug, Clone)]
pub struct WhereClause {
    /// The name of the field this clause is for
    pub field: String,
    /// The start position of this field name in the original query string
    pub field_start: usize,
    /// The end position of this field name in the original query string
    pub field_end: usize,
    /// What this field is constrained to
    pub constraint: WhereConstraint,
}

impl WhereClause {
    /// Every literal this clause named, whichever way it constrained its field
    ///
    /// Type checking and error rendering care about the literals and not about what they
    /// mean, so both walk this rather than matching on the constraint.
    pub fn values(&self) -> impl Iterator<Item = &WhereValue> {
        // walk whichever set of literals this clause holds, in the order they were written
        match &self.constraint {
            WhereConstraint::Values(values) => Either::Values(values.iter()),
            WhereConstraint::Range(range) => {
                Either::Bounds(range.lower.iter().chain(range.upper.iter()))
            }
        }
    }

    /// The values this field may take, if it is matched against a set of them
    pub fn as_values(&self) -> Option<&[WhereValue]> {
        // only a set of values has a set of values to hand back
        match &self.constraint {
            WhereConstraint::Values(values) => Some(values),
            WhereConstraint::Range(_) => None,
        }
    }

    /// The bounds this field was given, if it is bounded by a range
    pub fn as_range(&self) -> Option<&WhereRange> {
        // only a range has bounds to hand back
        match &self.constraint {
            WhereConstraint::Range(range) => Some(range),
            WhereConstraint::Values(_) => None,
        }
    }

    /// The first value this clause named
    ///
    /// A comparison cannot parse without a value, so every clause holds at least one and this
    /// never panics. It is for the callers that only ever expect one value.
    pub fn first(&self) -> &Value {
        &self
            .values()
            .next()
            .expect("a where clause always names a value")
            .value
    }
}

/// One of the two ways a clause holds its literals
///
/// This exists only so that [`WhereClause::values`] can hand back one iterator for both arms
/// without boxing it, since the two walk different collections.
enum Either<V, B> {
    /// The literals of a clause matching its field against a set of values
    Values(V),
    /// The literals of a clause bounding its field by a range
    Bounds(B),
}

impl<'a, V, B> Iterator for Either<V, B>
where
    V: Iterator<Item = &'a WhereValue>,
    B: Iterator<Item = &'a WhereBound>,
{
    type Item = &'a WhereValue;

    fn next(&mut self) -> Option<Self::Item> {
        // take the next literal from whichever collection this clause holds
        match self {
            Either::Values(values) => values.next(),
            Either::Bounds(bounds) => bounds.next().map(|bound| &bound.value),
        }
    }
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

/// Parse what a query asked each row to come back as
///
/// A star asks for the whole row, and a name asks for one of the projections its table
/// declared. Which projections exist is a per table question that only the generated binder
/// can answer, so a name is carried out of here as written and checked there.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// The projection name if one was written, or None for a star
fn projection<'s>(input: &mut &'s str) -> winnow::Result<Option<String>> {
    // a star asks for every field, and anything else has to name a projection
    winnow::combinator::alt(("*".map(|_| None), identifier.map(Some))).parse_next(input)
}

/// Parse "SELECT <projection> FROM <table>"
///
/// Parses the SELECT clause of a SHQL query.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// The projection this query asked for and the table name, or a parse error
fn select_from<'s>(input: &mut &'s str) -> winnow::Result<(Option<String>, String)> {
    (
        winnow::ascii::Caseless("SELECT"),
        multispace1,
        projection,
        multispace1,
        winnow::ascii::Caseless("FROM"),
        multispace1,
        identifier,
    )
        // we only care about what was projected and the table it came from
        .map(|(_, _, projected, _, _, _, table)| (projected, table))
        .parse_next(input)
}

/// The keyword joining one comparison in a WHERE clause to the next
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Connective {
    /// `AND`, which constrains another field
    And,
    /// `OR`, which gives the field the previous comparison named another value it may take
    Or,
}

/// Whether the input begins with the `IN` keyword rather than an `=`
///
/// The keyword has to be followed by whitespace or by the opening paren of its list, so that a
/// field compared against something starting with the letters `in` is not mistaken for one.
///
/// # Arguments
///
/// * `input` - The remaining input to check
fn starts_with_in(input: &str) -> bool {
    // grab exactly as many bytes as the keyword, bailing out if they are not a char boundary
    match input.get(..2) {
        // the keyword matched so make sure a list can follow it
        Some(found) if found.eq_ignore_ascii_case("IN") => {
            input[2..].starts_with(|c: char| c.is_whitespace() || c == '(')
        }
        _ => false,
    }
}

/// Parse a single literal and record the span it occupied
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
/// * `original` - The original complete query string (used to calculate positions)
/// * `field` - The field this literal is being written for, named in any error
fn where_value<'a>(
    input: &mut &'a str,
    original: &'a str,
    field: &str,
) -> Result<WhereValue, ShqlParseError> {
    // track the position before and after parsing the value
    let start = original.len() - input.len();
    // parse the value we are checking
    let literal = value.parse_next(input).map_err(|_| {
        ShqlParseError::at_position(
            format!(
                "Expected a value for field '{}', which must be a quoted string, a number, a boolean, or null",
                field
            ),
            start,
            original,
        )
    })?;
    // get the end position of this value for nice errors
    let end = original.len() - input.len();
    Ok(WhereValue {
        value: literal,
        start,
        end,
    })
}

/// Parse the parenthesised value list of an `IN` comparison
///
/// The `IN` keyword has already been looked ahead for by the caller, so it is consumed here.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
/// * `original` - The original complete query string (used to calculate positions)
/// * `field` - The field this list is being written for, named in any error
fn value_list<'a>(
    input: &mut &'a str,
    original: &'a str,
    field: &str,
) -> Result<Vec<WhereValue>, ShqlParseError> {
    // consume the keyword we already know is there
    let _: winnow::Result<&str> = winnow::ascii::Caseless("IN").parse_next(input);
    // skip any whitespace between the keyword and its list
    let _ = ws.parse_next(input);
    // the values have to be wrapped in parens
    let open: winnow::Result<&str> = "(".parse_next(input);
    open.map_err(|_| {
        ShqlParseError::at_position(
            format!("Expected '(' after IN for field '{}'", field),
            original.len() - input.len(),
            original,
        )
    })?;
    // skip any whitespace before the first value
    let _ = ws.parse_next(input);
    // a list with nothing in it matches nothing, so it is a mistake rather than a query
    if input.starts_with(')') {
        return Err(ShqlParseError::at_position(
            format!("IN needs at least one value for field '{}'", field),
            original.len() - input.len(),
            original,
        ));
    }
    // parse the first value, which every list has to have
    let mut values = vec![where_value(input, original, field)?];
    // keep taking values for as long as they are separated by commas
    loop {
        // skip any whitespace before the separator
        let _ = ws.parse_next(input);
        // look for a comma joining another value onto this list
        let separator: winnow::Result<Option<&str>> = opt(",").parse_next(input);
        // stop once there are no more values in this list
        if !matches!(separator, Ok(Some(_))) {
            break;
        }
        // skip any whitespace after the separator
        let _ = ws.parse_next(input);
        // a comma with nothing after it is a trailing comma, which is worth saying plainly
        if input.starts_with(')') {
            return Err(ShqlParseError::at_position(
                format!("Trailing comma in the IN list for field '{}'", field),
                original.len() - input.len(),
                original,
            ));
        }
        // parse the value this comma joined on
        values.push(where_value(input, original, field)?);
    }
    // close the list
    let close: winnow::Result<&str> = ")".parse_next(input);
    close.map_err(|_| {
        ShqlParseError::at_position(
            format!("Expected ',' or ')' in the IN list for field '{}'", field),
            original.len() - input.len(),
            original,
        )
    })?;
    Ok(values)
}

/// The comparison operator bounding one end of a range
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeOp {
    /// `<`, which bounds the upper end without including the value it names
    Less,
    /// `<=`, which bounds the upper end including the value it names
    LessEqual,
    /// `>`, which bounds the lower end without including the value it names
    Greater,
    /// `>=`, which bounds the lower end including the value it names
    GreaterEqual,
}

impl RangeOp {
    /// Whether this operator bounds the lower end of a range rather than the upper one
    fn is_lower(self) -> bool {
        matches!(self, RangeOp::Greater | RangeOp::GreaterEqual)
    }

    /// Whether this operator includes the value it names
    fn is_inclusive(self) -> bool {
        matches!(self, RangeOp::LessEqual | RangeOp::GreaterEqual)
    }

    /// Turn this operator and the value it names into one end of a range
    ///
    /// # Arguments
    ///
    /// * `value` - The value this operator bounded its field at
    fn into_range(self, value: WhereValue) -> WhereRange {
        // build the end this operator names, remembering whether it includes its value
        let bound = WhereBound {
            value,
            inclusive: self.is_inclusive(),
        };
        // place that end at whichever side of the range this operator bounds
        if self.is_lower() {
            WhereRange {
                lower: Some(bound),
                upper: None,
            }
        } else {
            WhereRange {
                lower: None,
                upper: Some(bound),
            }
        }
    }
}

/// Consume a range comparison operator if the input begins with one
///
/// The two character operators are tried before the one character ones, because `>` is a
/// prefix of `>=` and matching it first would leave the `=` behind to be read as a value.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
fn range_operator(input: &mut &str) -> Option<RangeOp> {
    // check the longer operators before the shorter ones they contain
    for (text, operator) in [
        (">=", RangeOp::GreaterEqual),
        ("<=", RangeOp::LessEqual),
        (">", RangeOp::Greater),
        ("<", RangeOp::Less),
    ] {
        // consume this operator if it is the one that was written
        if let Some(rest) = input.strip_prefix(text) {
            *input = rest;
            return Some(operator);
        }
    }
    None
}

/// Parse a single comparison
///
/// Three spellings produce a set of values - `<field> = <value>`, `<field> IN (<values>)`, and
/// the `OR` of two comparisons the caller folds together - since a field allowed to take
/// several values is the same query however it was written. A fourth,
/// `<field> <operator> <value>`, bounds one end of a range instead.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
/// * `original` - The original complete query string (used to calculate positions)
///
/// # Returns
///
/// A WhereClause containing the field name, what it is constrained to, and the positions of
/// every literal on success, or a parse error
fn comparison<'a>(input: &mut &'a str, original: &'a str) -> Result<WhereClause, ShqlParseError> {
    // record where this comparison starts so every error below can point at it
    let field_start = original.len() - input.len();
    // get the name of the field that we are parsing a comparison for
    let field = identifier.parse_next(input).map_err(|_| {
        ShqlParseError::at_position(
            "Expected a field name in the WHERE clause",
            field_start,
            original,
        )
    })?;
    // record where the field name ended so an error can underline just the name
    let field_end = original.len() - input.len();
    // skip any whitespace between the field and its operator
    let _ = ws.parse_next(input);
    // each operator constrains its field differently, so pick the right parser for the one written
    let constraint = if starts_with_in(input) {
        // a list of values is written differently to a single one
        WhereConstraint::Values(value_list(input, original, &field)?)
    } else if let Some(operator) = range_operator(input) {
        // this operator bounds one end of a range rather than naming a value
        let _ = ws.parse_next(input);
        WhereConstraint::Range(operator.into_range(where_value(input, original, &field)?))
    } else {
        // the only operator left that we support is equality
        let equals: winnow::Result<&str> = "=".parse_next(input);
        equals.map_err(|_| {
            ShqlParseError::at_position(
                format!(
                    "Expected an operator after field '{}'. SHQL supports =, IN, and the range \
                     operators <, <=, >, and >= on a sort key",
                    field
                ),
                original.len() - input.len(),
                original,
            )
        })?;
        // skip any whitespace between the operator and its value
        let _ = ws.parse_next(input);
        WhereConstraint::Values(vec![where_value(input, original, &field)?])
    };
    // build our where clause
    Ok(WhereClause {
        field,
        field_start,
        field_end,
        constraint,
    })
}

/// Parse the keyword joining another comparison onto a WHERE clause
///
/// This goes through `opt` so that a failed match restores the input, otherwise a dangling
/// keyword would be consumed and then silently ignored.
///
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
///
/// # Returns
///
/// The connective that was consumed, or None if no comparison is chained on
fn connective(input: &mut &str) -> Option<Connective> {
    // look for a keyword joining another comparison onto this clause
    let chained: winnow::Result<Option<(&str, Connective, &str)>> = opt((
        ws,
        alt((
            winnow::ascii::Caseless("AND").map(|_| Connective::And),
            winnow::ascii::Caseless("OR").map(|_| Connective::Or),
        )),
        multispace1,
    ))
    .parse_next(input);
    // pull the keyword out of whatever we matched
    match chained {
        Ok(Some((_, found, _))) => Some(found),
        _ => None,
    }
}

/// Drop any value a clause names more than once
///
/// A field naming the same value twice names the same partition twice, which would scan it
/// twice and hand back each of its rows twice.
///
/// # Arguments
///
/// * `condition` - The condition to deduplicate the values of
fn dedup_values(condition: &mut WhereClause) {
    // only a set of values can hold the same literal twice
    let WhereConstraint::Values(values) = &mut condition.constraint else {
        return;
    };
    // remember every literal we have kept so far
    let mut seen: Vec<Value> = Vec::with_capacity(values.len());
    // keep the first write of each value and drop the rest
    values.retain(|found| {
        // a value we have already kept adds nothing to this query
        if seen.contains(&found.value) {
            return false;
        }
        seen.push(found.value.clone());
        true
    });
}

/// Fold the AND joined conditions naming one field into a single clause
///
/// **A field may only be named twice when the two conditions bound opposite ends of one
/// range**, and those two are folded together here so that everything downstream still sees
/// exactly one clause per field. That is what makes the binding stage a lookup rather than a
/// search, in the three separate places that do it.
///
/// Every other repeat is refused. Two conditions naming values on one field ask for the rows
/// satisfying both, which for a partition key means the rows present in every one of those
/// partitions. Shoal answers a get by reading each named partition and returning their union,
/// so that spelling would quietly hand back the rows in *any* of them.
///
/// # Arguments
///
/// * `conditions` - The conditions in this WHERE clause, in the order they were written
/// * `original` - The original complete query string (used to build the suggestion)
fn merge_field_clauses(
    conditions: Vec<WhereClause>,
    original: &str,
) -> Result<Vec<WhereClause>, ShqlParseError> {
    // build the one clause per field this WHERE clause comes down to
    let mut merged: Vec<WhereClause> = Vec::with_capacity(conditions.len());
    // fold each condition into the clause for the field it names, or start that clause
    for condition in conditions {
        // take this condition apart so its field can be looked up while its constraint moves
        let WhereClause {
            field,
            field_start,
            field_end,
            constraint,
        } = condition;
        // find whether an earlier condition already named this field
        let Some(index) = merged.iter().position(|earlier| earlier.field == field) else {
            // this is the first condition to name this field, so it starts its clause
            merged.push(WhereClause {
                field,
                field_start,
                field_end,
                constraint,
            });
            continue;
        };
        // decide what naming this field a second time meant
        match (&mut merged[index].constraint, constraint) {
            // two ranges bound one field from each end, which is the shape we allow
            (WhereConstraint::Range(existing), WhereConstraint::Range(next)) => {
                merge_ranges(existing, next, &field, field_start, field_end, original)?;
            }
            // two sets of values on one field ask for an intersection we cannot answer
            (WhereConstraint::Values(existing), WhereConstraint::Values(next)) => {
                // rebuild both sets of literals as they were written to suggest an IN list
                let literals: Vec<&str> = existing
                    .iter()
                    .chain(next.iter())
                    .map(|found| &original[found.start..found.end])
                    .collect();
                return Err(ShqlParseError::new(
                    format!(
                        "'{}' is constrained twice by AND. Several values for one field are a \
                         union in shoal, not an intersection, so write it as {} IN ({})",
                        field,
                        field,
                        literals.join(", ")
                    ),
                    field_start,
                    field_end,
                    original,
                ));
            }
            // a value and a range are two different questions about one field
            _ => {
                return Err(ShqlParseError::new(
                    format!(
                        "'{}' is constrained by both a value and a range. A field is either \
                         matched against values or bounded by a range, not both",
                        field
                    ),
                    field_start,
                    field_end,
                    original,
                ));
            }
        }
    }
    Ok(merged)
}

/// Fold one end of a range into the range a field already had
///
/// A comparison names exactly one end, so this succeeds when the two conditions named
/// different ends and fails when they named the same one twice.
///
/// # Arguments
///
/// * `existing` - The range this field already had
/// * `next` - The range the second condition on this field named
/// * `field` - The name of the field being bounded, named in any error
/// * `field_start` - The start position of that field name in the original query string
/// * `field_end` - The end position of that field name in the original query string
/// * `original` - The original complete query string (used to render the error)
fn merge_ranges(
    existing: &mut WhereRange,
    next: WhereRange,
    field: &str,
    field_start: usize,
    field_end: usize,
    original: &str,
) -> Result<(), ShqlParseError> {
    // fold in the lower bound this condition named, if it named one
    if let Some(lower) = next.lower {
        // a range has one lower bound, so two of them is a query that means one of the pair
        if existing.lower.is_some() {
            return Err(two_bounds_error(field, "lower", field_start, field_end, original));
        }
        existing.lower = Some(lower);
    }
    // fold in the upper bound this condition named, if it named one
    if let Some(upper) = next.upper {
        // a range has one upper bound, so two of them is a query that means one of the pair
        if existing.upper.is_some() {
            return Err(two_bounds_error(field, "upper", field_start, field_end, original));
        }
        existing.upper = Some(upper);
    }
    Ok(())
}

/// Build the error for a field given the same end of a range twice
///
/// # Arguments
///
/// * `field` - The name of the field that was bounded twice
/// * `end` - Which end of the range was given twice, named in the error
/// * `field_start` - The start position of that field name in the original query string
/// * `field_end` - The end position of that field name in the original query string
/// * `original` - The original complete query string (used to render the error)
fn two_bounds_error(
    field: &str,
    end: &str,
    field_start: usize,
    field_end: usize,
    original: &str,
) -> ShqlParseError {
    ShqlParseError::new(
        format!(
            "'{}' is given two {} bounds by AND. A range has one {} bound, so write the tighter \
             of the two",
            field, end, end
        ),
        field_start,
        field_end,
        original,
    )
}

/// Parse the conditions making up a WHERE clause
///
/// Comparisons joined by `OR` are folded into the clause for the field they name, so a field
/// allowed to take several values comes back as one condition however it was written.
/// Comparisons joined by `AND` each constrain their own field.
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
    // parse the first of any comparisons in this query
    let mut conditions = vec![comparison(input, original)?];
    // continue to parse any comparisons chained onto this clause
    while let Some(joined) = connective(input) {
        // parse the comparison that keyword joined on
        let next = comparison(input, original)?;
        // place this comparison according to the keyword that joined it
        match joined {
            // AND constrains another field, so this is a condition of its own
            Connective::And => conditions.push(next),
            // OR gives the field the previous comparison named another value it may take
            Connective::Or => {
                // the condition we just parsed is the one this OR extends
                let previous = conditions
                    .last_mut()
                    .expect("a where clause always has a first condition");
                // an OR across two fields asks for rows outside any partition we named, and
                // the only access path is by partition key, so there is nothing to answer
                // the other side of it with
                if previous.field != next.field {
                    return Err(ShqlParseError::new(
                        format!(
                            "'{}' cannot be OR'd with '{}'. OR only joins conditions on the \
                             same field, where it means the same thing as IN",
                            previous.field, next.field
                        ),
                        next.field_start,
                        next.field_end,
                        original,
                    ));
                }
                // OR chooses between values, and two ranges are a union with no access path
                let (WhereConstraint::Values(kept), WhereConstraint::Values(added)) =
                    (&mut previous.constraint, next.constraint)
                else {
                    return Err(ShqlParseError::new(
                        format!(
                            "'{}' cannot be OR'd with a range. OR chooses between values for one \
                             field, and a union of ranges is not something shoal can read - bound \
                             the field from each end with AND instead",
                            next.field
                        ),
                        next.field_start,
                        next.field_end,
                        original,
                    ));
                };
                kept.extend(added);
            }
        }
    }
    // fold the conditions naming one field together, refusing every repeat but a range
    let mut conditions = merge_field_clauses(conditions, original)?;
    // drop any repeated value so a query naming one partition twice only reads it once
    for condition in &mut conditions {
        dedup_values(condition);
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

/// The projection a query named in place of a star
///
/// Whether the name is one the table declared is a question only the generated binder can
/// answer, so the offsets are carried alongside it for the error it raises when it is not.
#[derive(Debug, Clone)]
pub struct ParsedProjection {
    /// The name of the projection this query asked for
    pub name: String,
    /// The byte offset the name starts at in the original query
    pub start: usize,
    /// The byte offset just past the end of the name in the original query
    pub end: usize,
}

/// A parsed SELECT query
#[derive(Debug, Clone)]
pub struct ParsedSelect {
    /// The name of the table this query is for
    pub table_name: String,
    /// The projection this query asked for, or None if it wrote a star
    pub projection: Option<ParsedProjection>,
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
        // remember where the SELECT keyword starts, so a bad projection can be pointed at
        //
        // the clause is parsed as a whole, so the offset of what it named is worked out from
        // what it consumed rather than being handed back by it
        let select_start = query.len() - parsable.len();
        // parse the SELECT <projection> FROM <table> clause
        let (projected, table_name) = select_from.parse_next(&mut parsable).map_err(|e| {
            ShqlParseError::at_position(
                format!("Expected SELECT <projection> FROM <table>: {}", e),
                0,
                query,
            )
        })?;
        // find where a named projection was written, so a name no table declared can be shown
        let projection = projected.map(|name| {
            // the projection follows the SELECT keyword and the whitespace after it
            let after_select = select_start + "SELECT".len();
            let leading = query[after_select..].len() - query[after_select..].trim_start().len();
            let start = after_select + leading;
            ParsedProjection {
                end: start + name.len(),
                start,
                name,
            }
        });
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
            projection,
            conditions,
            limit,
        })
    }
}
