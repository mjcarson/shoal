//! Parses select queries from a string in shoal

use serde_json::Value;
use std::any;
use std::collections::HashMap;
use std::marker::PhantomData;
use winnow::ascii::{alpha1, digit1, multispace0, multispace1};
use winnow::combinator::{alt, delimited, opt};
use winnow::prelude::*;
use winnow::token::{take_till, take_while};

use crate::client::ShqlParseError;
use crate::shared::traits::ShoalTableSupport;

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
        alt((alpha1, "_")),
        take_while(0.., |c: char| c.is_alphanumeric() || c == '_'),
    )
        .map(|(first, rest): (&str, &str)| format!("{}{}", first, rest))
        .parse_next(input)
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
/// Parses a number with a decimal point (e.g., 3.14, -0.5, +42.0).
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
        .map(
            |(sign, int_part, _, dec_part): (Option<&str>, &str, &str, &str)| {
                let float_str = format!("{}{}.{}", sign.unwrap_or(""), int_part, dec_part);
                let value: f64 = float_str.parse().unwrap();
                Value::Number(serde_json::Number::from_f64(value).unwrap())
            },
        )
        .parse_next(input)
}

/// Parse an integer number
///
/// Parses a whole number with optional sign (e.g., 42, -17, +100).
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
        .map(|(sign, digits): (Option<&str>, &str)| {
            let int_str = format!("{}{}", sign.unwrap_or(""), digits);
            let value: i64 = int_str.parse().unwrap();
            Value::Number(serde_json::Number::from(value))
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
) -> winnow::Result<WhereClause> {
    // get the name of the field that we are parsing a where condition for
    let field = identifier.parse_next(input)?;
    // skip whitespace
    ws.parse_next(input)?;
    // right now we only support '=' signs
    "=".parse_next(input)?;
    // skip whitespace
    ws.parse_next(input)?;
    // Track the position before and after parsing the value
    let value_start = original.len() - input.len();
    // parse the value we are checking
    let val = value.parse_next(input)?;
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
fn where_conditions<'a>(
    input: &mut &'a str,
    original: &'a str,
) -> winnow::Result<Vec<WhereClause>> {
    // skip any spaces between the table name and our where statement
    multispace1.parse_next(input)?;
    // a set of where conditions are always preceeded by 'where'
    winnow::ascii::Caseless("WHERE").parse_next(input)?;
    // skip any whitespace
    multispace1.parse_next(input)?;
    // parse the first of any where conditions in this query
    let mut conditions = vec![where_conditions_helper(input, original)?];
    // continue to parse any where conditions chained by an 'and'
    while let Ok(_) = (ws, winnow::ascii::Caseless("AND"), multispace1).parse_next(input) {
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
/// # Arguments
///
/// * `input` - Mutable reference to the input string slice being parsed
fn limit_clause(input: &mut &str) -> winnow::Result<usize> {
    // parse the LIMIT keyword (case-insensitive)
    winnow::ascii::Caseless("LIMIT").parse_next(input)?;
    // skip any whitespace after LIMIT
    multispace1.parse_next(input)?;
    // parse the limit number
    digit1
        .map(|digits: &str| digits.parse::<usize>().unwrap())
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
        // attempt to parse WHERE conditions if present
        let conditions = where_conditions(&mut parsable, query).map_err(|e| {
            ShqlParseError::at_position(format!("Error parsing WHERE clause: {}", e), 0, query)
        })?;
        // parse any whitespace after the WHERE clause
        ws.parse_next(&mut parsable)
            .map_err(|e| ShqlParseError::at_position(format!("Parse error: {}", e), 0, query))?;
        // optionally parse a LIMIT clause if present
        let limit = opt(limit_clause).parse_next(&mut parsable).ok().flatten();
        // parse any whitespace after the LIMIT clause (or after WHERE if no LIMIT)
        let _ = ws.parse_next(&mut parsable);
        // optionally parse a semicolon terminator if present
        let _ = opt::<_, _, winnow::error::ContextError, _>(";").parse_next(&mut parsable);
        // parse any trailing whitespace after the optional semicolon
        let _ = ws.parse_next(&mut parsable);
        // return the successfully parsed query structure
        Ok(ParsedSelect {
            table_name,
            conditions,
            limit,
        })
    }
}

/// All conditions for a query categorized by their kind
#[derive(Debug)]
struct Conditions<T: ShoalTableSupport> {
    /// The partition key conditions
    pub partition_keys: HashMap<String, Value>,
    /// The sort key conditions
    pub sort_keys: HashMap<String, Value>,
    /// The partition key conditions
    pub filters: HashMap<String, Value>,
    /// The table these conditions are for
    table_kind: PhantomData<T>,
}

impl<T: ShoalTableSupport> Conditions<T> {
    /// Categorize WHERE conditions into partition_keys, sort_keys, and filters based on schema
    ///
    /// Takes a list of WHERE conditions and organizes them into three HashMaps based on their
    /// field roles (partition, sort, or filter) as defined in the table schema.
    ///
    /// # Arguments
    ///
    /// * `where_clauses` - Vector of WHERE conditions to categorize
    /// * `query` - The original query string (for error reporting)
    fn new(where_clauses: Vec<WhereClause>, query: &str) -> Result<Self, ShqlParseError> {
        // start with default maps for each
        let mut conditions = Conditions::<T> {
            partition_keys: HashMap::default(),
            sort_keys: HashMap::default(),
            filters: HashMap::default(),
            table_kind: PhantomData,
        };
        // step over each condition and categorize it
        for clause in where_clauses {
            // Get the field role
            let role = T::get_field_role(&clause.field).ok_or_else(|| {
                // this is not a known field build a descriptive error
                ShqlParseError::new(
                    format!(
                        "Unknown field '{}'. Valid fields are: {}",
                        clause.field,
                        T::field_names().join(", ")
                    ),
                    clause.value_start,
                    clause.value_end,
                    query,
                )
            })?;
            // Insert into the appropriate HashMap based on role
            match role {
                // add this partition key
                FieldRole::Partition => {
                    conditions.partition_keys.insert(clause.field, clause.value);
                }
                // add this sort key
                FieldRole::Sort => {
                    conditions.sort_keys.insert(clause.field, clause.value);
                }
                // add this filter
                FieldRole::Filter => {
                    conditions.filters.insert(clause.field, clause.value);
                }
            }
        }
        Ok(conditions)
    }
}

/// Type check WHERE conditions against a table schema
///
/// Validates that all WHERE conditions reference valid fields and that the values
/// can be deserialized into the expected types for those fields.
///
/// # Type Parameters
///
/// * `T` - The table type that implements TableSchema
///
/// # Arguments
///
/// * `conditions` - Slice of WHERE conditions to validate
/// * `query` - The original query string (for error reporting with positions)
fn type_check_conditions<T: ShoalTableSupport>(
    conditions: &[WhereClause],
    query: &str,
) -> Result<(), ShqlParseError> {
    // check all of our conditions
    for condition in conditions {
        // get the validator for this field
        let validator = T::get_field_validator(&condition.field).ok_or_else(|| {
            // unknown field so build a descriptive error
            ShqlParseError::new(
                format!(
                    "Unknown field '{}'. Valid fields are: {}",
                    condition.field,
                    T::field_names().join(", ")
                ),
                condition.value_start,
                condition.value_end,
                query,
            )
        })?;
        // validate the value type using the position information from the condition
        validator(&condition.value).map_err(|err| {
            // the type is wrong for this field so build a descriptive error
            ShqlParseError::new(
                format!("Type mismatch for field '{}': {}", condition.field, err),
                condition.value_start,
                condition.value_end,
                query,
            )
        })?;
    }

    Ok(())
}
