//! Cursor aware completion for partially typed SHQL queries
//!
//! [`ParsedSelect::new`](super::ParsedSelect::new) is all or nothing — it either binds a whole
//! query or fails, which makes it useless for a query box where the text is almost never valid
//! yet. This module answers a different question: given a query and a cursor, what is the user
//! in the middle of typing, and what could it become?
//!
//! That happens in two steps:
//!
//! 1. [`analyze`] walks the text before the cursor with a deliberately forgiving scanner and
//!    reports the grammar position the cursor sits in. It knows nothing about any schema, so it
//!    is always available.
//! 2. [`suggest`] turns that position into a ranked list of [`Suggestion`]s by asking a client's
//!    `QuerySupport` impl for the tables and fields it knows about. It is only available with
//!    the `shql-complete` feature since it fuzzy matches with `nucleo-matcher`.

use super::{is_ident_continue, is_ident_start};

#[cfg(feature = "shql-complete")]
use nucleo_matcher::pattern::{CaseMatching, Normalization, Pattern};
#[cfg(feature = "shql-complete")]
use nucleo_matcher::{Config, Matcher};
#[cfg(feature = "shql-complete")]
use serde_json::Value;

#[cfg(feature = "shql-complete")]
use super::{FieldRole, TypeValidator};
#[cfg(feature = "shql-complete")]
use crate::shared::traits::QuerySupport;

#[cfg(test)]
mod tests;

/// The kind of thing the grammar expects at the cursor
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expecting {
    /// The `SELECT` keyword that starts every query
    Select,
    /// The `*` projection
    Star,
    /// The `FROM` keyword
    From,
    /// The name of a table
    Table,
    /// The `WHERE` keyword
    Where,
    /// The name of a field to constrain
    Field,
    /// The `=` or `IN` that separates a field from the values it may take
    Equals,
    /// A literal value for a field
    Value {
        /// The field this value is being written for
        field: String,
    },
    /// The `(` that opens the value list of an `IN`
    OpenList {
        /// The field this list is being written for
        field: String,
    },
    /// A literal value inside the value list of an `IN`
    ValueList {
        /// The field this list is being written for
        field: String,
    },
    /// Another value in an `IN` list, or the `)` that closes it
    ListContinuation {
        /// The field this list is being written for
        field: String,
    },
    /// The count for a `LIMIT` clause
    LimitCount,
    /// Another condition, a limit, or the end of the query
    Continuation,
    /// Nothing but the end of the query
    End,
    /// Nothing at all, either because the query is already complete or because it is malformed
    Nothing,
}

/// Where the cursor sits in a partially typed query
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionContext {
    /// What the grammar expects at the cursor
    pub expecting: Expecting,
    /// The table named after `FROM`, if one has been typed yet
    pub table: Option<String>,
    /// The byte offset the word under the cursor starts at
    pub word_start: usize,
    /// The byte offset the word under the cursor ends at, which is always the cursor
    pub word_end: usize,
    /// The word under the cursor, which is empty when the cursor follows whitespace
    pub word: String,
}

/// A single token in the text before the word under the cursor
///
/// This is far coarser than the real grammar on purpose — it only needs to be good enough to
/// track which clause we are in, and it must never fail on text that is still being typed.
#[derive(Debug, PartialEq, Eq)]
enum Token {
    /// An identifier or a keyword
    Ident(String),
    /// The `*` projection
    Star,
    /// An `=` sign
    Equals,
    /// The `(` opening an `IN` list
    OpenParen,
    /// The `)` closing an `IN` list
    CloseParen,
    /// A `,` separating the values of an `IN` list
    Comma,
    /// A `;` terminator
    Semicolon,
    /// A quoted string literal
    String,
    /// A numeric literal
    Number,
}

/// Whether two strings are the same ignoring case
///
/// # Arguments
///
/// * `token` - The token to compare
/// * `keyword` - The keyword to compare it against
fn is_keyword(token: &str, keyword: &str) -> bool {
    token.eq_ignore_ascii_case(keyword)
}

/// Find the word the cursor is sitting in
///
/// This walks backwards over identifier characters, which is the same set the parser's
/// `identifier` rule accepts, so the word we highlight can never disagree with the word the
/// parser would read. Numeric literals are picked up too since digits continue an identifier.
///
/// # Arguments
///
/// * `query` - The query being typed
/// * `cursor` - The byte offset of the cursor in the query
fn word_start(query: &str, cursor: usize) -> usize {
    // walk backwards from the cursor for as long as we see identifier characters
    query[..cursor]
        .char_indices()
        .rev()
        .take_while(|(_, c)| is_ident_continue(*c))
        .map(|(index, _)| index)
        .last()
        .unwrap_or(cursor)
}

/// Whether the cursor is inside an unterminated string literal
///
/// String literals have no escape syntax, so an odd number of quotes before the cursor means we
/// are inside one and have no business suggesting identifiers.
///
/// # Arguments
///
/// * `head` - The text before the cursor
fn in_string_literal(head: &str) -> bool {
    head.chars().filter(|c| *c == '\'').count() % 2 == 1
}

/// Break the text before the word under the cursor into coarse tokens
///
/// Anything that isn't recognized is skipped rather than raising an error, since this runs
/// against text that is still being typed.
///
/// # Arguments
///
/// * `head` - The text before the word under the cursor
fn tokenize(head: &str) -> Vec<Token> {
    // build the tokens we find in this text
    let mut tokens = Vec::new();
    // step through our text a character at a time
    let mut chars = head.char_indices().peekable();
    while let Some((index, current)) = chars.next() {
        match current {
            // skip over any whitespace between tokens
            _ if current.is_whitespace() => continue,
            // the projection star
            '*' => tokens.push(Token::Star),
            // the equals sign separating a field from its value
            '=' => tokens.push(Token::Equals),
            // the parens wrapping the values of an IN list
            '(' => tokens.push(Token::OpenParen),
            ')' => tokens.push(Token::CloseParen),
            // the separator between the values of an IN list
            ',' => tokens.push(Token::Comma),
            // the optional query terminator
            ';' => tokens.push(Token::Semicolon),
            // a string literal, which runs until the next quote or the end of the text
            '\'' => {
                // consume everything up to and including the closing quote
                for (_, next) in chars.by_ref() {
                    if next == '\'' {
                        break;
                    }
                }
                tokens.push(Token::String);
            }
            // an identifier or keyword
            _ if is_ident_start(current) => {
                // find the end of this identifier
                let mut end = index + current.len_utf8();
                while let Some((next_index, next)) = chars.peek() {
                    // stop as soon as we see something that can't continue an identifier
                    if !is_ident_continue(*next) {
                        break;
                    }
                    end = next_index + next.len_utf8();
                    chars.next();
                }
                tokens.push(Token::Ident(head[index..end].to_string()));
            }
            // a numeric literal, which can't start an identifier so it must be a number
            _ => {
                // consume the rest of this number
                while let Some((_, next)) = chars.peek() {
                    if !next.is_ascii_digit() && *next != '.' {
                        break;
                    }
                    chars.next();
                }
                tokens.push(Token::Number);
            }
        }
    }
    tokens
}

/// Work out where the cursor sits in a partially typed query
///
/// # Arguments
///
/// * `query` - The query being typed
/// * `cursor` - The byte offset of the cursor in the query, which must be on a character boundary
///
/// # Examples
///
/// ```
/// use shoal_core::shared::queries::parser::{analyze, Expecting};
///
/// let query = "SELECT * FROM Movie WHERE ti";
/// let context = analyze(query, query.len());
///
/// assert_eq!(context.expecting, Expecting::Field);
/// assert_eq!(context.table.as_deref(), Some("Movie"));
/// assert_eq!(context.word, "ti");
/// ```
pub fn analyze(query: &str, cursor: usize) -> CompletionContext {
    // clamp the cursor so a stale cursor can never panic on a slice
    let cursor = cursor.min(query.len());
    // find the word we are in the middle of typing
    let start = word_start(query, cursor);
    // build the context we will return, which we only ever change the expectation of
    let mut context = CompletionContext {
        expecting: Expecting::Nothing,
        table: None,
        word_start: start,
        word_end: cursor,
        word: query[start..cursor].to_string(),
    };
    // a cursor inside a string literal is typing data, not anything we can complete
    if in_string_literal(&query[..start]) {
        return context;
    }
    // walk the tokens before this word to find out what the grammar expects here
    let mut expecting = Expecting::Select;
    // the field the condition we are in the middle of is for
    let mut pending_field = String::new();
    for token in tokenize(&query[..start]) {
        // take what we expected before this token so we can match on it by value
        let current = std::mem::replace(&mut expecting, Expecting::Nothing);
        expecting = match (current, &token) {
            // the SELECT that starts every query
            (Expecting::Select, Token::Ident(word)) if is_keyword(word, "SELECT") => {
                Expecting::Star
            }
            // the projection, which can only ever be a star
            (Expecting::Star, Token::Star) => Expecting::From,
            // the FROM naming which table to read
            (Expecting::From, Token::Ident(word)) if is_keyword(word, "FROM") => Expecting::Table,
            // the table itself, which we hang on to so we can complete its fields later
            (Expecting::Table, Token::Ident(name)) => {
                context.table = Some(name.clone());
                Expecting::Where
            }
            // the mandatory WHERE clause
            (Expecting::Where, Token::Ident(word)) if is_keyword(word, "WHERE") => Expecting::Field,
            // a field name to constrain, which we hang on to so we can suggest its values
            (Expecting::Field, Token::Ident(field)) => {
                pending_field = field.clone();
                Expecting::Equals
            }
            // the equals sign separating that field from its value
            (Expecting::Equals, Token::Equals) => Expecting::Value {
                field: pending_field.clone(),
            },
            // the IN keyword, which gives that field a whole list of values instead
            (Expecting::Equals, Token::Ident(word)) if is_keyword(word, "IN") => {
                Expecting::OpenList {
                    field: pending_field.clone(),
                }
            }
            // the paren opening that list
            (Expecting::OpenList { field }, Token::OpenParen) => Expecting::ValueList { field },
            // a literal value, completing this condition
            (Expecting::Value { .. }, Token::String | Token::Number) => Expecting::Continuation,
            // a bare true/false/null literal, which also completes this condition
            (Expecting::Value { .. }, Token::Ident(word))
                if is_keyword(word, "true") || is_keyword(word, "false") || is_keyword(word, "null") =>
            {
                Expecting::Continuation
            }
            // a literal value inside a list, which can be followed by another
            (Expecting::ValueList { field }, Token::String | Token::Number) => {
                Expecting::ListContinuation { field }
            }
            // a bare true/false/null literal inside a list
            (Expecting::ValueList { field }, Token::Ident(word))
                if is_keyword(word, "true") || is_keyword(word, "false") || is_keyword(word, "null") =>
            {
                Expecting::ListContinuation { field }
            }
            // a comma joining another value onto this list
            (Expecting::ListContinuation { field }, Token::Comma) => Expecting::ValueList { field },
            // the paren closing this list, which completes the condition
            (Expecting::ListContinuation { .. }, Token::CloseParen) => Expecting::Continuation,
            // another condition joined onto this one
            (Expecting::Continuation, Token::Ident(word)) if is_keyword(word, "AND") => {
                Expecting::Field
            }
            // another value for a field, written as an OR rather than as an IN list
            (Expecting::Continuation, Token::Ident(word)) if is_keyword(word, "OR") => {
                Expecting::Field
            }
            // a limit on how many rows to return
            (Expecting::Continuation, Token::Ident(word)) if is_keyword(word, "LIMIT") => {
                Expecting::LimitCount
            }
            // the count for that limit, after which only the terminator is left
            (Expecting::LimitCount, Token::Number) => Expecting::End,
            // the terminator, which ends the query
            (Expecting::Continuation | Expecting::End, Token::Semicolon) => Expecting::Nothing,
            // anything else means this query is malformed, so we have nothing to offer
            _ => Expecting::Nothing,
        };
        // there is no recovering from a malformed query, so stop walking it
        if expecting == Expecting::Nothing {
            break;
        }
    }
    context.expecting = expecting;
    context
}

/// The kind of thing a suggestion is
#[cfg(feature = "shql-complete")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuggestionKind {
    /// A SHQL keyword or piece of punctuation
    Keyword,
    /// The name of a table
    Table,
    /// The name of a field
    Field,
    /// A literal value
    Value,
}

/// A single thing the user could type at the cursor
#[cfg(feature = "shql-complete")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suggestion {
    /// The text to splice into the query
    pub text: String,
    /// The kind of thing this suggestion is
    pub kind: SuggestionKind,
    /// A short annotation describing this suggestion, which may be empty
    pub detail: String,
}

#[cfg(feature = "shql-complete")]
impl Suggestion {
    /// Create a new suggestion
    ///
    /// # Arguments
    ///
    /// * `text` - The text to splice into the query
    /// * `kind` - The kind of thing this suggestion is
    /// * `detail` - A short annotation describing this suggestion
    fn new<T: Into<String>, D: Into<String>>(text: T, kind: SuggestionKind, detail: D) -> Self {
        Suggestion {
            text: text.into(),
            kind,
            detail: detail.into(),
        }
    }

    /// Get the text to splice into a query when this suggestion is accepted
    ///
    /// Most suggestions are followed by another token, so they get a trailing space. The ones
    /// that either open a literal or end the query do not.
    pub fn insert_text(&self) -> String {
        // an opening quote or paren and a terminator are never followed by anything we would
        // add a space for
        if self.text == "'" || self.text == ";" || self.text == "(" {
            self.text.clone()
        } else {
            format!("{} ", self.text)
        }
    }
}

/// A ranked list of suggestions and the span of the query they replace
#[cfg(feature = "shql-complete")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completions {
    /// The suggestions themselves, best match first
    pub items: Vec<Suggestion>,
    /// The byte offset the accepted text should be spliced in at
    pub word_start: usize,
    /// The byte offset the accepted text should be spliced in up to
    pub word_end: usize,
}

#[cfg(feature = "shql-complete")]
impl AsRef<str> for Suggestion {
    fn as_ref(&self) -> &str {
        &self.text
    }
}

/// Strip module paths out of a type name
///
/// [`std::any::type_name`] returns fully qualified names, so a `String` field comes back as
/// `alloc::string::String`. Only the last segment of each path is worth showing a user.
///
/// # Arguments
///
/// * `name` - The fully qualified type name to shorten
#[cfg(feature = "shql-complete")]
fn short_type_name(name: &str) -> String {
    // build the shortened name
    let mut short = String::with_capacity(name.len());
    // the path segment we are currently reading
    let mut segment = String::new();
    // step through the name a character at a time
    let mut chars = name.chars().peekable();
    while let Some(current) = chars.next() {
        if is_ident_continue(current) {
            // this character is part of the segment we are reading
            segment.push(current);
        } else if current == ':' && chars.peek() == Some(&':') {
            // this segment was a module path, so drop it along with its separator
            segment.clear();
            chars.next();
        } else {
            // this segment was a real type name, so keep it and whatever ended it
            short.push_str(&segment);
            segment.clear();
            short.push(current);
        }
    }
    short.push_str(&segment);
    short
}

/// Probe a field's validator to find the name of its type
///
/// [`make_validator`](super::make_validator) hands back the field's type name whenever a value
/// deserializes, so feeding it one literal of each shape tells us what the field is without
/// needing the type itself.
///
/// # Arguments
///
/// * `validator` - The validator for the field to probe
#[cfg(feature = "shql-complete")]
fn probe_type_name(validator: TypeValidator) -> Option<String> {
    // try one literal of each shape until one of them deserializes
    [
        Value::Bool(true),
        Value::Number(0.into()),
        Value::String(String::new()),
        Value::Null,
    ]
    .iter()
    .find_map(|value| validator(value).ok())
    .map(|name| short_type_name(&name))
}

/// Build the literals a field will accept
///
/// # Arguments
///
/// * `validator` - The validator for the field to build literals for
#[cfg(feature = "shql-complete")]
fn value_suggestions(validator: TypeValidator) -> Vec<Suggestion> {
    // build the literals this field accepts
    let mut suggestions = Vec::new();
    // booleans are the only values we can offer in full
    if let Ok(name) = validator(&Value::Bool(true)) {
        let detail = short_type_name(&name);
        suggestions.push(Suggestion::new("true", SuggestionKind::Value, detail.clone()));
        suggestions.push(Suggestion::new("false", SuggestionKind::Value, detail));
    }
    // a nullable field can be matched against null
    if let Ok(name) = validator(&Value::Null) {
        suggestions.push(Suggestion::new(
            "null",
            SuggestionKind::Value,
            short_type_name(&name),
        ));
    }
    // a string field at least gets its opening quote
    if let Ok(name) = validator(&Value::String(String::new())) {
        suggestions.push(Suggestion::new(
            "'",
            SuggestionKind::Value,
            short_type_name(&name),
        ));
    }
    suggestions
}

/// Build a keyword suggestion in the case the user is typing
///
/// Keywords are case insensitive in SHQL, so somebody typing `sel` should be offered `select`
/// rather than having their query shift under them.
///
/// # Arguments
///
/// * `keyword` - The canonical uppercase keyword
/// * `word` - The word the user is in the middle of typing
#[cfg(feature = "shql-complete")]
fn keyword_suggestion(keyword: &str, word: &str) -> Suggestion {
    // match the case the user has typed so far, defaulting to the canonical uppercase
    let text = if !word.is_empty() && !word.chars().any(char::is_uppercase) {
        keyword.to_lowercase()
    } else {
        keyword.to_string()
    };
    Suggestion::new(text, SuggestionKind::Keyword, "keyword")
}

/// Build the annotation shown next to a field
///
/// # Arguments
///
/// * `role` - The role this field plays in a query
/// * `type_name` - The name of this field's type, if we could probe it
#[cfg(feature = "shql-complete")]
fn field_detail(role: FieldRole, type_name: Option<String>) -> String {
    // name the role this field plays
    let role = match role {
        FieldRole::Partition => "partition",
        FieldRole::Sort => "sort",
        FieldRole::Filter => "filter",
    };
    // pair it with the field's type when we know it
    match type_name {
        Some(type_name) => format!("{} {}", role, type_name),
        None => role.to_string(),
    }
}

/// Build every candidate that could follow the cursor, before any filtering
///
/// # Arguments
///
/// * `context` - Where the cursor sits in the query
#[cfg(feature = "shql-complete")]
fn candidates<S: QuerySupport>(context: &CompletionContext) -> Vec<Suggestion> {
    match &context.expecting {
        // the keywords and punctuation that hold a query together
        Expecting::Select => vec![keyword_suggestion("SELECT", &context.word)],
        Expecting::Star => vec![Suggestion::new("*", SuggestionKind::Keyword, "all columns")],
        Expecting::From => vec![keyword_suggestion("FROM", &context.word)],
        Expecting::Where => vec![keyword_suggestion("WHERE", &context.word)],
        Expecting::Equals => vec![
            Suggestion::new("=", SuggestionKind::Keyword, "equals"),
            keyword_suggestion("IN", &context.word),
        ],
        Expecting::OpenList { .. } => vec![Suggestion::new(
            "(",
            SuggestionKind::Keyword,
            "start of value list",
        )],
        Expecting::ListContinuation { .. } => vec![
            Suggestion::new(",", SuggestionKind::Keyword, "another value"),
            Suggestion::new(")", SuggestionKind::Keyword, "end of value list"),
        ],
        Expecting::Continuation => vec![
            keyword_suggestion("AND", &context.word),
            keyword_suggestion("OR", &context.word),
            keyword_suggestion("LIMIT", &context.word),
            Suggestion::new(";", SuggestionKind::Keyword, "end of query"),
        ],
        Expecting::End => vec![Suggestion::new(";", SuggestionKind::Keyword, "end of query")],
        // every table in this database
        Expecting::Table => S::table_names()
            .iter()
            .map(|name| Suggestion::new(*name, SuggestionKind::Table, "table"))
            .collect(),
        // the fields of the table this query is reading
        Expecting::Field => {
            // we can't name any fields until we know which table we are reading
            let Some(table) = &context.table else {
                return Vec::new();
            };
            let Some(fields) = S::table_fields(table) else {
                return Vec::new();
            };
            // only fields with a role can be used in a where clause
            let mut fields: Vec<(FieldRole, Suggestion)> = fields
                .into_iter()
                .filter_map(|field| {
                    // fields with no role are rejected by the parser, so never offer them
                    let role = field.role?;
                    // probe this field's validator so we can show its type
                    let type_name =
                        S::table_field_validator(table, field.name).and_then(probe_type_name);
                    let detail = field_detail(role, type_name);
                    Some((role, Suggestion::new(field.name, SuggestionKind::Field, detail)))
                })
                .collect();
            // a query has to constrain a partition key, so offer those first
            fields.sort_by_key(|(role, _)| match role {
                FieldRole::Partition => 0,
                FieldRole::Sort => 1,
                FieldRole::Filter => 2,
            });
            fields.into_iter().map(|(_, field)| field).collect()
        }
        // the literals the field being constrained will accept, on its own or in a list
        Expecting::Value { field } | Expecting::ValueList { field } => context
            .table
            .as_ref()
            .and_then(|table| S::table_field_validator(table, field))
            .map(value_suggestions)
            .unwrap_or_default(),
        // there is nothing useful to offer for a limit count or a malformed query
        Expecting::LimitCount | Expecting::Nothing => Vec::new(),
    }
}

/// Suggest what could be typed at the cursor of a partially typed query
///
/// Candidates are fuzzy matched against the word under the cursor and returned best match
/// first, so `mvk` will find `MovieByKeyword`. Ties keep the order the candidates were built
/// in, which puts partition keys ahead of sort keys and filters.
///
/// # Arguments
///
/// * `query` - The query being typed
/// * `cursor` - The byte offset of the cursor in the query
#[cfg(feature = "shql-complete")]
pub fn suggest<S: QuerySupport>(query: &str, cursor: usize) -> Completions {
    // work out where in the grammar this cursor is
    let context = analyze(query, cursor);
    // build everything that could go here
    let items = candidates::<S>(&context);
    // narrow those down to what matches the word being typed
    let items = if context.word.is_empty() {
        items
    } else {
        // fuzzy match the way helix does, in smart case so a lowercase word ignores case
        let mut matcher = Matcher::new(Config::DEFAULT);
        let pattern = Pattern::parse(&context.word, CaseMatching::Smart, Normalization::Smart);
        pattern
            .match_list(items, &mut matcher)
            .into_iter()
            .map(|(item, _)| item)
            .collect()
    };
    Completions {
        items,
        word_start: context.word_start,
        word_end: context.word_end,
    }
}
