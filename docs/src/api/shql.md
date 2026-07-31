# SHQL

SHQL — Shoal Query Language — is a small SQL-like parser for read queries, built with
[winnow] in `shoal-core/src/shared/queries/parser.rs`. It exists mainly to give
[shoalctl](../operations/shoalctl.md) a way to accept typed queries from a human.

It is not a general query language, and the gap between what it looks like and what it does is
wide enough to be worth stating up front.

For the user-facing version of this page — what to type into the query bar and what the errors
mean — see [shoalctl](../operations/shoalctl.md#writing-queries).

## Grammar

```
query   := ws "SELECT" ws1 "*" ws1 "FROM" ws1 identifier where [ws limit] [ws ";"] ws eof
where   := ws1 "WHERE" ws1 condition { ws "AND" ws1 condition }
condition := identifier ws "=" ws value
limit   := "LIMIT" ws1 digits
value   := string | float | integer | boolean | null
string  := "'" { any character except "'" } "'"
float   := ["-"|"+"] digits "." digits
integer := ["-"|"+"] digits
boolean := "true" | "false"        (case-insensitive)
null    := "null"                  (case-insensitive)
identifier := xid_start { xid_continue }
```

Keywords are case-insensitive (`winnow::ascii::Caseless`, `parser.rs:368-372`). The trailing
semicolon is optional (`parser.rs:582`). Identifiers are **not** case-insensitive — the name
after `FROM` is matched against the table's Rust struct name.

Identifiers follow the same rules Rust does, which is to say
[UAX #31](https://www.unicode.org/reports/tr31/): an `XID_Start` character or an underscore,
followed by `XID_Continue` characters (`is_ident_start`/`is_ident_continue`, `parser.rs`). Table
and field names in a query *are* Rust identifiers, so anything you can name a struct or a field
can be typed in a query.

```sql
SELECT * FROM Movie WHERE id = 12345;
SELECT * FROM Movie WHERE id = 12345 AND title = 'Inception'
select * from Movie where id = 12345 limit 10
```

## What it does not support

- **Only `SELECT *`.** No projection — the literal `*` is required (`parser.rs:370`).
- **Only `=`.** No `<`, `>`, `!=`, `LIKE`, `IN`, or `BETWEEN` (`parser.rs:409`).
- **Only `AND`.** No `OR` and no parentheses (`parser.rs:475-484`).
- **No `ORDER BY`, `GROUP BY`, `JOIN`, or aggregates.**
- **No `INSERT`, `UPDATE`, or `DELETE`.** Writes must be built as typed queries.
- **No escape syntax in string literals.** `string_literal` is
  `delimited("'", take_till(0.., '\''), "'")` (`parser.rs:207-211`), so a value containing a
  single quote cannot be expressed at all — not by doubling it, not by backslash.
- **No exponent form for floats, and digits required on both sides of the point.**
  `float_number` is `(opt(sign), digit1, ".", digit1)` (`parser.rs:261`), so `1e9`, `.5`, and
  `5.` all fail. `1e9` is especially confusing: it parses as the integer `1` and then fails as
  trailing input.
- **The `WHERE` clause is mandatory.** `ParsedSelect::new` checks for the keyword before
  parsing and reports its absence directly:

  ```rust
  if !starts_with_keyword(parsable.trim_start(), "WHERE") {
      return Err(ShqlParseError::at_position(
          "A WHERE clause is required, and it must constrain a partition key",
          query.len() - parsable.len(),
          query,
      ));
  }
  ```

  `parser.rs:548-555`

  `SELECT * FROM Movie;` does not parse. Since the only access path is by partition key, a
  full scan is not expressible anyway.

## The two-stage pipeline

```
  "SELECT * FROM Movie WHERE id = 5 AND title = 'x' LIMIT 10"
                  │
                  │ ParsedSelect::new       — syntax only
                  ▼
  ParsedSelect { table_name: "Movie", conditions: [...], limit: Some(10) }
                  │
                  │ generated parse arms    — per table, from structs/client.rs
                  ▼
        ┌─────────┴──────────┐
        │ validate + type check each condition against the schema
        │ pull out the partition keys, then the sort keys
        │ Movie::shql_build_filters   — filter conditions → MovieFilter
        ▼
  MovieGet { partition_key, sort_keys, filters, limit }  ──▶  DbQueryKinds::Movie(...)
```

### Stage 1: parse

`ParsedSelect::new` (`parser.rs:537-598`) is table-agnostic. It produces field names and
`serde_json::Value` literals, tracking each value's byte offsets for error reporting:

```rust
pub struct WhereClause {
    pub field: String,
    pub value: Value,
    pub value_start: usize,
    pub value_end: usize,
}
```

`parser.rs:101-111`

`serde_json::Value` is the intermediate type because validation is done by round-tripping
through serde, below.

The parser is scannerless — there is no separate lexer, and whitespace is handled explicitly per
rule. `ParsedSelect::new` threads `&mut &str` by hand rather than composing one top-level
combinator, so it can compute byte offsets as `original.len() - input.len()`.

### Stage 2: bind to a table

The `QuerySupport::parse` impl is generated per database
(`shoal-derive/src/structs/client.rs`), matching `table_name` against each table's struct name
and then doing three things.

**Type check.** Each condition's field must exist and its value must deserialize into the
field's declared type:

```rust
let validator = <#inner_type as TableSchemaSupport>::get_field_validator(&condition.field)
    .ok_or_else(|| ShqlParseError::new(
        format!("No validator for field '{}'", condition.field),
        condition.value_start, condition.value_end, query,
    ))?;
validator(&condition.value).map_err(|err| {
    ShqlParseError::new(
        format!("Type mismatch for field '{}': {}", condition.field, err),
        condition.value_start, condition.value_end, query,
    )
})?;
```

`shoal-derive/src/structs/client.rs:139-153` (unsorted), `:210-224` (sorted)

Validators are generated closures over `serde_json::from_value::<T>`:

```rust
pub fn make_validator<T: serde::de::DeserializeOwned + 'static>() -> TypeValidator {
    |value: &Value| {
        serde_json::from_value::<T>(value.clone())
            .map(|_| any::type_name::<T>().to_string())
            .map_err(|e| format!("Cannot deserialize to {}: {}", any::type_name::<T>(), e))
    }
}
```

`parser.rs:154-160`

**This is why SHQL requires `serde`.** A table field whose type is not `DeserializeOwned`
cannot be type-checked, and the generated `TableSchemaSupport` impl will not compile. rkyv
alone is not enough for a table you intend to query with SHQL.

Note the validator deserializes and throws the result away — it proves the value *could* be
that type, then the real conversion happens separately in the generated parse arm.

**Pull out the keys.** Conditions are bucketed by the `FieldRole` the schema assigns each field
(`shoal-derive/src/traits/table_schema.rs`). The two table kinds differ:

- **Unsorted** takes the *first* partition condition via `.find(...)` and errors with
  `Missing partition key in WHERE clause` if there is none
  (`structs/client.rs:156-166`). Extra partition conditions are ignored, so
  `WHERE id = 1 AND id = 2` becomes `id = 1`.
- **Sorted** collects *every* partition condition into a `Vec` in written order, and every sort
  condition into `sort_keys` (`structs/client.rs:227-270`). `WHERE movie = 'a' AND movie = 'b'`
  queries both partitions.

A field with no role — declared on the struct but marked neither partition, sort, nor filter —
has no `FieldRole` and produces `Unknown field`. The error lists `T::field_names()`, which
includes *all* fields, so it can report a field as unknown while listing it as valid. Mildly
confusing.

**Build the filters.** Conditions naming a filterable field are converted into the table's
generated `*Filter` struct by `shql_build_filters`, an inherent function emitted alongside the
filter struct itself (`shoal-derive/src/structs/filter.rs:103-119`):

```rust
get_query.filters = <#inner_type>::shql_build_filters(&parsed.conditions, query)?;
```

`shoal-derive/src/structs/client.rs:181` (unsorted), `:280` (sorted)

It returns `None` when no condition named a filterable field, so a key-only query leaves
`filters` unset. It lives on the row type rather than on `TableSchemaSupport` because the
filter struct's type comes from `ShoalTableSupport::Filters`, which that trait cannot name.

## Error reporting

```rust
ShqlParseError::new(message, value_start, value_end, query)
```

Positions are carried from parse through binding so errors can point at the offending literal
rather than at the whole query — the reason `WhereClause` tracks offsets at all. Rendering lives
in `shoal-core/src/client/errors.rs`, and it slices the span with `get` rather than indexing, so
a span that lands mid-character falls back to showing the whole input instead of panicking.

## What actually happens to `LIMIT`

It parses. It type-checks. It is stored on `ParsedSelect.limit`, copied into the generated
`*Get` struct, serialized, sent over the wire, and delivered to the table.

Then it is ignored. `PersistentSortedTable::get` inlines its own scan loop with no limit check
(`shoal-core/src/server/tables/persistent/sorted.rs:436-448`), even though
`SortedPartition::get` — which it does not call — implements it correctly
(`.../tables/partitions.rs:278-284`). See
[Known Issues](../appendix/known-issues.md#7-limit-is-ignored-by-persistent-sorted-tables).

For unsorted tables the point is moot: a partition holds one row.

Similarly, sort-key conditions are collected onto `SortedGet::sort_keys` and then have no
effect on a get, because the server ignores them
([Known Issues](../appendix/known-issues.md#8-sort-keys-are-accepted-and-ignored)). A SHQL
query narrowing by sort key will return the whole partition.

Filter conditions, by contrast, now do reach the server — they are applied through
`ShoalTableSupport::is_filtered_archived` like filters on any typed query.

## Design notes

**Two stages so the parser stays table-agnostic.** Syntax is parsed once, without generics;
binding and type checking are generated per table. The parser knows nothing about schemas and
the schema code knows nothing about syntax.

**`serde_json::Value` as the bridge.** Literals need a dynamic representation between the two
stages, and going through serde means type validation is free for any `DeserializeOwned`
type — no per-type parsing code. The cost is a `serde` dependency on every queryable table and
a `Value` clone per validation.

**Positions tracked from the start.** Carrying byte offsets through both stages means a type
error can underline the literal, which is the difference between a usable REPL and a
frustrating one.

**Nothing is consumed silently.** `ParsedSelect::new` rejects leftover input rather than
stopping where it happens to run out of grammar, and the `AND` loop matches through `opt` so a
failed match restores the input. Without the second part the first is useless: winnow's tuple
parser does not backtrack on failure, so a dangling `AND` was being consumed by the failed match
and then never noticed.

## Completion

A third, optional stage answers a different question: given a query and a cursor, what could be
typed next? `shoal-core/src/shared/queries/parser/complete.rs` holds it.

`analyze(query, cursor)` walks the text behind the cursor with a forgiving scanner — the real
parser is no help here since it fails outright on the half-written text a query box is full of —
and reports an `Expecting` describing where in the grammar the cursor sits, the word being typed,
and the table named after `FROM` if one has been. It knows nothing about any schema and is always
compiled in.

`suggest::<Client>(query, cursor)` turns that into a ranked `Vec<Suggestion>` by asking the
client's `QuerySupport` impl for the tables and fields it knows about, fuzzy matching them
against the word under the cursor with [`nucleo-matcher`][nucleo] — the same matcher helix uses.
It is behind the `shql-complete` feature so a server build doesn't pay for it.

Value suggestions and the type names shown beside each field come free from the validators stage
2 already builds: `make_validator` hands back the field's type name whenever a value
deserializes, so feeding a validator one literal of each shape reveals both what the field
accepts and what type it is. No extra code is generated for it.

`shoalctl` renders the result; see [its docs](../operations/shoalctl.md#autocompletion).

## Testing

Stage 1 is covered by `shoal-core/src/shared/queries/parser/tests.rs` — the grammar, each
literal form, keyword case-insensitivity, byte-offset tracking, and every error path including
the overflow and trailing-input cases. The module docs on `parser.rs` carry doctests, so the
documented grammar is compiler-verified.

Stage 2 is covered by `shoal/tests/shql.rs`, which defines a real sorted and unsorted table via
the derive macros and asserts on the bound query. It needs no server, so it runs in
milliseconds. The same file covers `suggest` against that schema.

Completion's grammar-position scanner has its own tests in
`shoal-core/src/shared/queries/parser/complete/tests.rs`, and the menu that renders it is covered
by `shoalctl/tests/completion.rs`.

## Limitations

- Equality only, `AND` only, `SELECT *` only, reads only.
- A `WHERE` clause is mandatory and must constrain a partition key.
- String literals have no escape syntax, so they cannot contain a single quote — which makes
  rows whose partition key holds an apostrophe unreachable
  ([Known Issues #27](../appendix/known-issues.md#27-shql-cannot-express-a-string-containing-a-single-quote)).
- Floats need digits on both sides of the point and have no exponent form, and `1e9` fails with
  a misleading trailing-input error
  ([#29](../appendix/known-issues.md#29-shql-rejects-exponent-notation-with-a-misleading-error)).
- `LIMIT` is parsed and discarded by the server
  ([#7](../appendix/known-issues.md#7-limit-is-ignored-by-persistent-sorted-tables)).
- Sort-key conditions are parsed and discarded by the server
  ([#8](../appendix/known-issues.md#8-sort-keys-are-accepted-and-ignored)).
- Repeating a field is handled three different ways depending on where it lands — dropped for
  unsorted partition keys and for filters, honoured as extra partitions for sorted ones
  ([#26](../appendix/known-issues.md#26-shql-silently-drops-duplicate-conditions-on-one-field)).
- "Unknown field" can name a field that appears in the list of valid fields
  ([#28](../appendix/known-issues.md#28-shql-unknown-field-names-the-field-it-is-listing-as-valid)).
- Requires `serde::DeserializeOwned` on every field type.

[winnow]: https://docs.rs/winnow/
[nucleo]: https://docs.rs/nucleo-matcher/
