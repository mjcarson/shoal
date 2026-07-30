# SHQL

SHQL — Shoal Query Language — is a small SQL-like parser for read queries, built with
[winnow] in `shoal-core/src/shared/queries/parser.rs`. It exists mainly to give
[shoalctl](../operations/shoalctl.md) a way to accept typed queries from a human.

It is not a general query language, and the gap between what it looks like and what it does is
wide enough to be worth stating up front.

## Grammar

```
query   := ws "SELECT" ws1 "*" ws1 "FROM" ws1 identifier where [ws limit] [ws ";"] ws
where   := ws1 "WHERE" ws1 condition { ws "AND" ws1 condition }
condition := identifier ws "=" ws value
limit   := "LIMIT" ws1 digits
value   := string | number | boolean | null
string  := "'" ... "'"
number  := ["-"|"+"] digits ["." digits]
boolean := "true" | "false"        (case-insensitive)
null    := "null"                  (case-insensitive)
identifier := (alpha | "_") { alphanumeric | "_" }
```

Keywords are case-insensitive (`winnow::ascii::Caseless`, `parser.rs:237-241`). The trailing
semicolon is optional (`parser.rs:384`).

```sql
SELECT * FROM Movie WHERE id = 12345;
SELECT * FROM Movie WHERE id = 12345 AND title = 'Inception'
select * from Movie where id = 12345 limit 10
```

## What it does not support

- **Only `SELECT *`.** No projection — the literal `*` is required (`parser.rs:239`).
- **Only `=`.** No `<`, `>`, `!=`, `LIKE`, `IN`, or `BETWEEN`
  (`parser.rs:273`).
- **Only `AND`.** No `OR` and no parentheses (`parser.rs:312`).
- **No `ORDER BY`, `GROUP BY`, `JOIN`, or aggregates.**
- **No `INSERT`, `UPDATE`, or `DELETE`.** Writes must be built as typed queries.
- **The `WHERE` clause is mandatory.** `ParsedSelect::new` calls `where_conditions` directly
  rather than wrapping it in `opt`, so it is a hard error:

  ```rust
  let conditions = where_conditions(&mut parsable, query).map_err(|e| {
      ShqlParseError::at_position(format!("Error parsing WHERE clause: {}", e), 0, query)
  })?;
  ```

  `parser.rs:373-375`

  `SELECT * FROM Movie;` does not parse. Since the only access path is by partition key, a
  full scan is not expressible anyway — but the error message says "Error parsing WHERE
  clause", not "a WHERE clause is required".

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
        │ type_check_conditions       — do the literals fit the field types?
        │ Conditions::new             — partition / sort / filter
        ▼
  MovieGet { partition_key, filters, limit }  ──▶  DbQueryKinds::Movie(...)
```

### Stage 1: parse

`ParsedSelect::new` (`parser.rs:362-393`) is table-agnostic. It produces field names and
`serde_json::Value` literals, tracking each value's byte offsets for error reporting:

```rust
pub struct WhereClause {
    pub field: String,
    pub value: Value,
    pub value_start: usize,
    pub value_end: usize,
}
```

`parser.rs:16-26`

`serde_json::Value` is the intermediate type because validation is done by round-tripping
through serde, below.

### Stage 2: bind to a table

The `QuerySupport::parse` impl is generated per database
(`shoal-derive/src/structs/client.rs`), matching `table_name` against each table's struct name
and then:

**Type check.** Each condition's value must deserialize into the field's declared type:

```rust
let validator = T::get_field_validator(&condition.field).ok_or_else(|| {
    ShqlParseError::new(
        format!("Unknown field '{}'. Valid fields are: {}", condition.field, T::field_names().join(", ")),
        condition.value_start, condition.value_end, query,
    )
})?;
validator(&condition.value).map_err(|err| {
    ShqlParseError::new(format!("Type mismatch for field '{}': {}", condition.field, err), ...)
})?;
```

`parser.rs:476-508`

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

`parser.rs:55-61`

**This is why SHQL requires `serde`.** A table field whose type is not `DeserializeOwned`
cannot be type-checked, and the generated `TableSchemaSupport` impl will not compile. rkyv
alone is not enough for a table you intend to query with SHQL.

Note the validator deserializes and throws the result away — it proves the value *could* be
that type, then the real conversion happens separately in the generated parse arm.

**Categorise.** Conditions are bucketed by field role:

```rust
match role {
    FieldRole::Partition => { conditions.partition_keys.insert(clause.field, clause.value); }
    FieldRole::Sort      => { conditions.sort_keys.insert(clause.field, clause.value); }
    FieldRole::Filter    => { conditions.filters.insert(clause.field, clause.value); }
}
```

`parser.rs:444-457`

A field with no role — declared on the struct but marked neither partition, sort, nor filter —
has no `FieldRole` and produces `Unknown field`. The error lists `T::field_names()`, which
includes *all* fields, so it can report a field as unknown while listing it as valid. Mildly
confusing.

Since the buckets are `HashMap`s keyed by field name, repeating a field silently keeps the
last value: `WHERE id = 1 AND id = 2` becomes `id = 2`.

## Error reporting

```rust
ShqlParseError::new(message, value_start, value_end, query)
```

Positions are carried from parse through binding so errors can point at the offending literal
rather than at the whole query — the reason `WhereClause` tracks offsets at all. See
`shoal-core/src/client/errors.rs` for the rendering.

## Using it

```rust
let queries = client.query().parse("SELECT * FROM Movie WHERE id = 550")?;
let mut results = client.send(queries).await?;
```

`Queries::parse` (`shoal-core/src/shared/queries.rs:79-85`) parses and appends. It is marked
`#[must_use]` and returns a `Result`, so it does not chain with the other builder methods as
smoothly as it looks.

## What actually happens to `LIMIT`

It parses. It type-checks. It is stored on `ParsedSelect.limit`, copied into the generated
`*Get` struct, serialized, sent over the wire, and delivered to the table.

Then it is ignored. `PersistentSortedTable::get` inlines its own scan loop with no limit check
(`shoal-core/src/server/tables/persistent/sorted.rs:436-448`), even though
`SortedPartition::get` — which it does not call — implements it correctly
(`.../tables/partitions.rs:278-284`). See
[Known Issues](../appendix/known-issues.md#7-limit-is-ignored-by-persistent-sorted-tables).

For unsorted tables the point is moot: a partition holds one row.

Similarly, sort-key conditions are categorised into `Conditions::sort_keys` and then have no
effect on a get, because the server ignores `SortedGet::sort_keys`
([Known Issues](../appendix/known-issues.md#8-sort-keys-are-accepted-and-ignored)). A SHQL
query narrowing by sort key will return the whole partition.

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

## Limitations

- Equality only, `AND` only, `SELECT *` only, reads only.
- A `WHERE` clause is mandatory, with a misleading error when it is missing.
- `LIMIT` is parsed and discarded by the server.
- Sort-key conditions are parsed and discarded by the server.
- Duplicate conditions on one field silently keep the last.
- "Unknown field" can name a field that appears in the list of valid fields.
- Requires `serde::DeserializeOwned` on every field type.
- No test module in `parser.rs` — the grammar is not covered by unit tests.

[winnow]: https://docs.rs/winnow/
