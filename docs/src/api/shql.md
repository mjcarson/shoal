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
query      := ws "SELECT" ws1 "*" ws1 "FROM" ws1 identifier where [ws limit] [ws ";"] ws eof
where      := ws1 "WHERE" ws1 condition { ws "AND" ws1 condition }
condition  := comparison { ws "OR" ws1 comparison }
comparison := identifier ws ( "=" ws value
                            | "IN" ws "(" ws value { ws "," ws value } ws ")"
                            | range_op ws value )
range_op   := ">=" | "<=" | ">" | "<"
limit      := "LIMIT" ws1 digits
value      := string | float | integer | boolean | null
string     := "'" { any character except "'" } "'"
float      := ["-"|"+"] digits "." digits
integer    := ["-"|"+"] digits
boolean    := "true" | "false"        (case-insensitive)
null       := "null"                  (case-insensitive)
identifier := xid_start { xid_continue }
```

Keywords are case-insensitive (`winnow::ascii::Caseless`). The trailing
semicolon is optional (`ParsedSelect::new`). Identifiers are **not** case-insensitive — the name
after `FROM` is matched against the table's Rust struct name.

Identifiers follow the same rules Rust does, which is to say
[UAX #31](https://www.unicode.org/reports/tr31/): an `XID_Start` character or an underscore,
followed by `XID_Continue` characters (`is_ident_start`/`is_ident_continue`, `parser.rs`). Table
and field names in a query *are* Rust identifiers, so anything you can name a struct or a field
can be typed in a query.

```sql
SELECT * FROM Movie WHERE id = 12345;
SELECT * FROM Movie WHERE id = 12345 AND title = 'Inception'
SELECT * FROM Movie WHERE id IN (12345, 12346)
SELECT * FROM Movie WHERE id = 12345 OR id = 12346
select * from Movie where id = 12345 limit 10
SELECT * FROM MovieByKeyword WHERE keyword = 'alien' AND title > 'Gravity' LIMIT 20
```

## Range operators

A **sort key** — and only a sort key — can be bounded rather than matched, with `<`, `<=`, `>`,
or `>=`:

```sql
SELECT * FROM MovieByKeyword WHERE keyword = 'alien' AND title > 'Gravity' LIMIT 20
SELECT * FROM MovieByKeyword WHERE keyword = 'alien' AND title >= 'G' AND title < 'H'
```

This is the one place `AND` may name a field twice. The two conditions bound opposite ends and
are folded into a single clause while the `WHERE` clause is read, so everything downstream still
sees one clause per field. Naming the same end twice is refused:

```
'title' is given two lower bounds by AND. A range has one lower bound, so write the tighter of
the two
```

as is mixing the two ways of constraining a field (`title = 'x' AND title < 'z'`), and joining a
range with `OR`, which would be a union of ranges with no access path to read it.

**Ranges bind on a sort key alone.** A range on a partition key or a filter parses and is then
refused during binding, naming the field:

```
'keyword' is a partition key and cannot be given a range. A partition is located by its exact
key, so name the ones to read with = or IN
```

A partition is located by hashing its key, so there is no ordering to bound. A filter is a
membership test evaluated per row, so a bound on one has nothing to mean.

**An exclusive lower bound is a cursor.** Take the sort key of the last row of a page, feed it
back as `>`, and ask for the same limit again — that is the next page, and it costs a seek plus
its own rows rather than every row before it. That is what range predicates are for; see
[F1](../features/sort-key-ranges.md).

## `AND`, `OR`, and `IN`

One rule decides which connective is legal where: **`OR` and `IN` choose between values for a
single field, and `AND` joins conditions on different fields.**

```sql
-- allowed
SELECT * FROM MovieByKeyword WHERE keyword IN ('giant worm', 'alien')
SELECT * FROM MovieByKeyword WHERE keyword = 'giant worm' OR keyword = 'alien'
SELECT * FROM Movie WHERE id = 550 AND title IN ('Alien', 'Aliens')

-- rejected
SELECT * FROM MovieByKeyword WHERE keyword = 'giant worm' AND keyword = 'alien'
SELECT * FROM Movie WHERE id = 550 OR title = 'Alien'
```

`IN` is the primary spelling; `OR` between two conditions on the same field folds into exactly
the same clause, so `id = 1 OR id = 2` and `id IN (1, 2)` are indistinguishable after parsing.
Duplicated values are dropped, so `IN (1, 2, 1)` names two partitions rather than three.

**Why `AND` cannot repeat a field with values.** Two conditions naming values on one field ask
for the rows satisfying both. For a partition key that means the rows present in *every* one of
those partitions, and a get answers by reading each named partition and returning their union —
so the query would have quietly returned the rows in *any* of them. The parser refuses it and
names the `IN` list that was meant:

```
'keyword' is constrained twice by AND. Several values for one field are a union in shoal, not
an intersection, so write it as keyword IN ('giant worm', 'alien')
```

A real intersection is unbuilt work; see
[TODOs](../appendix/todos.md#intersection-across-partitions-a-real-and-on-one-field). The one
exception to the rule is two *range* conditions on a sort key, which bound opposite ends of one
range rather than asking for two things at once — see [Range operators](#range-operators) above.

**Why `OR` cannot cross fields.** `keyword = 'a' OR title = 'Alien'` asks for every row whose
title is `Alien` in *any* partition, and the only access path in shoal is by partition key —
there is no scan to answer that side with. Rather than accept some cross-field `OR`s and reject
others, all of them are rejected. What a full boolean `OR` would take is written up in
[TODOs](../appendix/todos.md#full-boolean-or).

**Several filter values.** `IN` on a filterable field means the same thing it does on a
partition key: a row matches if its value is any of the listed ones. Filters on *different*
fields must all match, so `title IN ('Alien', 'Aliens') AND watched = true` is a disjunction
inside a conjunction.

## What it does not support

- **Only `SELECT *`.** No projection — the literal `*` is required.
- **`=`, `IN`, and the range operators `<`, `<=`, `>`, `>=`.** No `!=`, `LIKE`, or `BETWEEN`.
  `BETWEEN` is sugar over `>= AND <=` and its inner `AND` collides with the one that joins
  clauses, so it was deliberately left out ([TODOs](../appendix/todos.md)).
- **A range binds on a sort key only,** and a range over a *prefix* of a composite sort key is
  not expressible at all. See [Range operators](#range-operators).
- **No parentheses,** other than the ones wrapping an `IN` list. There is no grouping and so no
  precedence to reason about: a connective's meaning is decided entirely by whether its two
  sides name the same field.
- **`OR` only joins conditions on the same field,** cannot join a range at all, and **`AND`
  cannot constrain one field twice** unless the two conditions bound opposite ends of one range.
  See [`AND`, `OR`, and `IN`](#and-or-and-in) above.
- **Composite partition keys are not reachable.** A table with several `#[shoal(partition)]`
  fields has a tuple `PartitionKey`, and no SHQL literal can produce one.
- **No `ORDER BY`, `GROUP BY`, `JOIN`, or aggregates.**
- **No `INSERT`, `UPDATE`, or `DELETE`.** Writes must be built as typed queries.
- **No escape syntax in string literals.** `string_literal` is
  `delimited("'", take_till(0.., '\''), "'")` (`string_literal`, `parser.rs`), so a value containing a
  single quote cannot be expressed at all — not by doubling it, not by backslash.
- **No exponent form for floats, and digits required on both sides of the point.**
  `float_number` is `(opt(sign), digit1, ".", digit1)` (`float_number`, `parser.rs`), so `1e9`, `.5`, and
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

  `ParsedSelect::new`, `parser.rs`

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
        │ pull out the partition keys, then the row selection
        │ Movie::shql_build_filters   — filter conditions → MovieFilter
        ▼
  MovieGet { partition_keys, sort_select, filters, limit } ──▶  DbQueryKinds::Movie(...)
```

### Stage 1: parse

`ParsedSelect::new` (`parser.rs`) is table-agnostic. It produces field names and
`serde_json::Value` literals, tracking each value's byte offsets for error reporting:

```rust
pub struct WhereValue {
    pub value: Value,
    pub start: usize,
    pub end: usize,
}

pub struct WhereClause {
    pub field: String,
    pub field_start: usize,
    pub field_end: usize,
    pub constraint: WhereConstraint,
}

pub enum WhereConstraint {
    /// `=`, `IN`, or an `OR` of the same field
    Values(Vec<WhereValue>),
    /// `<`, `<=`, `>`, or `>=`
    Range(WhereRange),
}

pub struct WhereRange {
    pub lower: Option<WhereBound>,
    pub upper: Option<WhereBound>,
}

pub struct WhereBound {
    pub value: WhereValue,
    pub inclusive: bool,
}
```

A `Values` clause holds every value its field may take, so `IN` and `OR` produce one shape and a
plain `=` is just the case where there is one of them. Folding happens during parsing:
comparisons joined by `OR` are merged into the clause for the field they name, and two *range*
comparisons on one field are merged into a clause bounded at both ends. Everything else naming a
field twice is refused. Downstream code can therefore assume **one clause per field**, which is
what makes the binding stage a lookup rather than a search.

The two arms are different questions answered by different access paths, which is why the
constraint is an enum rather than an operator tag beside the values: every consumer has to say
which of them it can take, so a range reaching a partition key or a filter is a place that
refuses rather than a case that falls through. `WhereClause::values()` walks the literals of
either arm, since type checking and error rendering care about the literals and not about what
they mean.

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
        condition.field_start, condition.field_end, query,
    ))?;
for found in &condition.values {
    validator(&found.value).map_err(|err| {
        ShqlParseError::new(
            format!("Type mismatch for field '{}': {}", condition.field, err),
            found.start, found.end, query,
        )
    })?;
}
```

The check runs per value, so one bad literal in an `IN` list is underlined on its own rather
than taking the whole list with it.

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

`make_validator`, `parser.rs`

**This is why SHQL requires `serde`.** A table field whose type is not `DeserializeOwned`
cannot be type-checked, and the generated `TableSchemaSupport` impl will not compile. rkyv
alone is not enough for a table you intend to query with SHQL.

Note the validator deserializes and throws the result away — it proves the value *could* be
that type, then the real conversion happens separately in the generated parse arm.

**Pull out the keys.** Conditions are bucketed by the `FieldRole` the schema assigns each field
(`shoal-derive/src/traits/table_schema.rs`). The two table kinds differ:

Both kinds now work the same way: there is at most one partition condition, and every value it
names becomes a partition to read, in written order. A query with none errors with
`Missing partition key in WHERE clause`, and one that bounds its partition key rather than naming
it is refused, since a partition is located by hashing its exact key.

Sorted tables additionally read the sort condition into a `SortSelect`, which is the whole of how
a query narrows the rows it wants out of every partition it reads:

| Sort condition | `SortSelect` |
| --- | --- |
| none written | `All` — every row of each partition |
| `= 'x'`, `IN ('x', 'y')` | `Keys([..])` — those rows and no others |
| `> 'x'`, `>= 'a' AND < 'm'` | `Range(..)` — the rows between the bounds |

`All` is the only arm that means every row. It is not the same as `Keys([])`, which is a set with
nothing in it and selects nothing — a distinction that did not exist when a get carried a bare
`sort_keys: Vec<Sort>` and an empty one meant "unnarrowed". See
[F1](../features/sort-key-ranges.md#invariants-to-uphold).

Unsorted gets used to carry a single scalar partition key, so `WHERE id = 1 AND id = 2` bound
`id = 1` and dropped the second value without a word. `UnsortedGet` now carries a `Vec<u64>`
like `SortedGet` does, and both split across shards the same way.

A field with no role — declared on the struct but marked neither partition, sort, nor filter —
has no `FieldRole` and produces `Unknown field`. The error lists `T::field_names()`, which
includes *all* fields, so it can report a field as unknown while listing it as valid. Mildly
confusing.

**Build the filters.** Conditions naming a filterable field are converted into the table's
generated `*Filter` struct by `shql_build_filters`, an inherent function emitted alongside the
filter struct itself (`shoal-derive/src/structs/filter.rs`):

```rust
get_query.filters = <#inner_type>::shql_build_filters(&parsed.conditions, query)?;
```

`shoal-derive/src/structs/client.rs`, in both the unsorted and the sorted parse arm

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
`*Get` struct, serialized, sent over the wire, and delivered to the table — which honours it.

The limit is counted against the rows a get has accumulated, so it spans every partition the get
names and survives a get that has to park on a disk read and resume
([Query Execution](../tables/query-execution.md#what-a-get-actually-filters-on)).

Across shards it is still global: each shard applies the limit to its own share, and the shard
that split the query merges the shares, puts their rows back into the order the query named its
partitions in, and trims their union before replying. This was not always
true — see [`limit` was ignored by persistent sorted tables](../appendix/resolved/sorted-limit.md).

Because the order is now defined, so is *which* rows a limit keeps: the first ones, in the order
below. A get whose first partition has to be read from disk waits for it rather than answering
out of a partition that was quicker — see
[Row order](../appendix/resolved/partition-order.md).

## Row order

The rows a get answers with are a function of the query alone:

1. Partitions come back in the order the query named them, which for `IN` is the order the
   values were written.
2. Within a partition, a sorted table's rows come back in sort-key order. An unsorted partition
   holds one row.

```sql
SELECT * FROM MovieByKeyword WHERE keyword IN ('giant worm', 'alien') LIMIT 3
```

reads the `giant worm` partition first and answers with its first three titles; reversing the
list answers out of `alien` instead. Running either query again returns the same rows in the
same order, which is what makes paging over several partitions possible without pulling the
whole result back and sorting it client side.

This is not a global `ORDER BY`. Titles are not interleaved across the two keywords — every
`giant worm` row comes before every `alien` one. Sorting the union by sort key would mean every
named partition had to be read even under a small limit, which is a trade this has not made.

`LIMIT 0` is honoured literally: nothing is scanned, nothing is read, and the response is an empty
get. Note that an empty get is not distinguishable from a get that found nothing, so a `LIMIT 0`
surfaces through `send_one` as a failed query.

For unsorted tables the point is nearly moot: a partition holds one row, so `LIMIT 0` is the only
limit an unsorted get can reach.

Sort-key conditions are collected onto `SortedGet::sort_select`, which either names the rows to
return or bounds them. `WHERE keyword = 'alien' AND title = 'Aliens'` answers with that row alone,
`title IN ('Alien', 'Aliens')` with those two, and `title > 'Gravity'` with everything after that
title. Rows come back in sort-key order whichever of the three it is — however the `IN` list was
written, and however the bounds were ordered — and a query that names no sort key still returns the
whole partition. Selecting rows used to be parsed and thrown away
([item 8](../appendix/resolved/sort-keys.md)); bounding them was added by
[F1](../features/sort-key-ranges.md).

Because the order within a partition is defined, an exclusive lower bound is a cursor: the sort key
of the last row of a page names where the next page starts. Across partitions that pages each of
them in turn rather than globally, since rows are grouped by partition rather than interleaved.

A table with several `#[shoal(sort)]` fields cannot be narrowed this way at all — neither named
nor bounded — for the same reason a composite partition key cannot be named: no SHQL literal is a
tuple ([item 42](../appendix/known-issues.md#42-shql-cannot-express-a-composite-sort-key)). The
typed API can range over a tuple `Sort`, since a tuple is `Ord`; what neither can do is bound a
*prefix* of one.

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
stopping where it happens to run out of grammar, and the connective loop matches through `opt` so
a failed match restores the input. Without the second part the first is useless: winnow's tuple
parser does not backtrack on failure, so a dangling `AND` was being consumed by the failed match
and then never noticed.

**A spelling that cannot lie.** Where a query could be written in a way that reads as one thing
and behaves as another, the parser rejects it and says what to write instead. That is why
`keyword = 'a' AND keyword = 'b'` is an error rather than a synonym for `IN`: it would have kept
working, and kept meaning the opposite of what it says.

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

The operator slot is the one place the menu consults a field's role: `=` and `IN` are offered for
every field, and `<`, `<=`, `>`, `>=` only for a sort key. Binding refuses a range anywhere else,
so offering one there would walk a user straight into an error the menu could see coming.

`shoalctl` renders the result; see [its docs](../operations/shoalctl.md#autocompletion).

## Testing

Stage 1 is covered by `shoal-core/src/shared/queries/parser/tests.rs` — the grammar, each
literal form, keyword case-insensitivity, byte-offset tracking, `IN` lists and their error
cases, `OR` folding, each range operator and the folding and refusals around it, and every other
error path including the overflow and trailing-input cases. The module docs on `parser.rs` carry
doctests, so the documented grammar is compiler-verified.

The row order a query promises is covered end to end against a running server in
`shoal/tests/persistent_sorted_table.rs` and `shoal/tests/persistent_unsorted_table.rs`, since
it only holds once the shard collecting a split query's shares puts them back in order. The
stability tests run the same query many times over, because the order they replaced depended on
which shard happened to answer first and so could pass a single run by luck.

Stage 2 is covered by `shoal/tests/shql.rs`, which defines a real sorted and unsorted table via
the derive macros and asserts on the bound query. It needs no server, so it runs in
milliseconds. The same file covers `suggest` against that schema.

Completion's grammar-position scanner has its own tests in
`shoal-core/src/shared/queries/parser/complete/tests.rs`, and the menu that renders it is covered
by `shoalctl/tests/completion.rs`.

## Limitations

- Equality, `IN`, and the range operators only; `SELECT *` only; reads only.
- A `WHERE` clause is mandatory and must constrain a partition key.
- `OR` only joins conditions on the same field and cannot join a range, and no field may be
  constrained twice by `AND` unless the two bound opposite ends of one range. There is no
  intersection across partitions and no full boolean `OR`
  ([TODOs](../appendix/todos.md#intersection-across-partitions-a-real-and-on-one-field)).
- A range binds on a sort key alone, applies to the whole `Sort` value rather than a prefix of a
  composite one, and does not reduce the I/O of a cold partition
  ([F1](../features/sort-key-ranges.md#limitations)).
- Composite partition keys cannot be expressed, since no literal can produce a tuple.
- String literals have no escape syntax, so they cannot contain a single quote — which makes
  rows whose partition key holds an apostrophe unreachable
  ([Known Issues #27](../appendix/known-issues.md#27-shql-cannot-express-a-string-containing-a-single-quote)).
- Floats need digits on both sides of the point and have no exponent form, and `1e9` fails with
  a misleading trailing-input error
  ([#29](../appendix/known-issues.md#29-shql-rejects-exponent-notation-with-a-misleading-error)).
- There is no `ORDER BY`. Rows come back in the order the query named its partitions, and in
  sort-key order within each of them, which is what `LIMIT` takes the first of. Sorting across
  partitions is up to the caller.
- A sort-key condition can bound rows as well as name them, but only on the whole `Sort` value
  and only with `<`, `<=`, `>`, `>=` — there is no `BETWEEN` and no bound over a *prefix* of a
  composite sort key ([F1](../features/sort-key-ranges.md#limitations)).
- Composite sort keys cannot be expressed, for the same reason composite partition keys cannot
  ([#42](../appendix/known-issues.md#42-shql-cannot-express-a-composite-sort-key)).
- "Unknown field" can name a field that appears in the list of valid fields
  ([#28](../appendix/known-issues.md#28-shql-unknown-field-names-the-field-it-is-listing-as-valid)).
- Requires `serde::DeserializeOwned` on every field type.

[winnow]: https://docs.rs/winnow/
[nucleo]: https://docs.rs/nucleo-matcher/
