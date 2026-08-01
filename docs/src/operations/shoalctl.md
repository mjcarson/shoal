# shoalctl

`shoalctl` is a terminal UI for querying a Shoal database, built on [ratatui]. Because Shoal's
schema is a compile-time construct, **shoalctl cannot be a standalone binary** — it is a
library you compile against your schema.

## Compiling it for your schema

```rust
#[db]
pub struct Tmdb {
    pub movies: PersistentUnsortedTable<Movie, FileSystem>,
    pub movies_by_keyword: PersistentSortedTable<MoviesByKeyword, FileSystem>,
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    let shoal = Arc::new(Shoal::<TmdbClient>::new("127.0.0.1:12000").await?);
    shoalctl::run(shoal).await
}
```

`shoalctl/examples/tmdbctl.rs` is the worked example; run it with:

```bash
cargo run --example tmdbctl --release
```

The schema definition must be *duplicated* from wherever your server defines it — `tmdbctl.rs`
restates the entire `Movie` and `MoviesByKeyword` definitions that `shoal/examples/tmdb.rs`
already contains. There is no shared crate, so the two copies can drift, and if they do the
rkyv layouts diverge and responses fail validation
([Wire Protocol](../architecture/wire-protocol.md#limitations)). Factoring a schema into its
own crate is the obvious fix and is not done anywhere in the repo.

## Entry point

```rust
pub async fn run<S>(shoal: Arc<Shoal<S>>) -> color_eyre::Result<()>
```

`shoalctl/src/lib.rs:118`

`run` installs `color_eyre`, enables mouse capture, initialises the terminal, runs the app, and
restores the terminal on the way out (`lib.rs:129-167`). The restore happens before the error
is propagated, so a failure does not leave the terminal in raw mode.

The `where` clause is 20 lines of rkyv bounds (`lib.rs:119-140`), repeated verbatim on
`run_app` (`lib.rs:89-109`) — a good illustration of the ergonomic cost of the
generic-over-schema approach.

## Architecture

```
   crossterm EventStream            background query tasks
            │                                │
            │ AppEvent::Terminal             │ AppEvent::QueryResult
            └────────────┬───────────────────┘
                         ▼
                  kanal channel
                         │
                         ▼
                    App::start          ── the event loop
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
         handle_event          render(frame)
                                     │
                    ┌────────────────┼────────────────┐
                    ▼                ▼                ▼
                tab bar          query bar    ratatui-hypertile panes
                              └── completion    └── TabState per pane
                                     menu            └── content
```

The tab bar, query bar, and status bar are fixed chrome laid out by `App::render`
(`app.rs:527`); hypertile only tiles the content panes. The query bar is *not* one of those
panes — it is sized to its query, one row of text until the query is long enough to wrap, and
hypertile splits proportionally rather than to content. It sits directly under the tab bar so
the completion menu has room to drop down under the cursor.

```rust
pub enum AppEvent<S: QuerySupport> {
    Terminal(Event),
    QueryResult { tab_id: Uuid, table_name: S::TableNames, result: QueryResult<S> },
}
```

`shoalctl/src/lib.rs:64-74`

Terminal input and query results arrive on one channel, so the UI never blocks on a query.
Queries run as background tokio tasks and post results back tagged with the tab's `Uuid`
(`app.rs:444`).

Panes are managed by `ratatui-hypertile`, giving a tiling layout with multiple independent
query tabs.

## Keys

From `App::handle_normal_mode_key` (`shoalctl/src/app.rs:336-374`):

| Key | Action |
| --- | --- |
| `q` | Quit |
| `i` | Insert mode (edit the query bar) |
| `Esc` | Back to normal mode |
| `h` `j` `k` `l` / arrows | Scroll results |
| `n` / `+` / `=` | Add a pane |
| `-` | Remove a pane |
| `[` / `]` | Resize the focused content pane by 5% |
| `Space` | Enter shortcut mode |

Shortcut mode (`app.rs:402-432`):

| Key | Action |
| --- | --- |
| `q` | Focus the query bar |
| `w` | Clear the query |
| `p` | Focus a pane |

Query box, when the completion menu is open (`TabState::handle_query_input`,
`shoalctl/src/components/tab.rs`):

| Key | Action |
| --- | --- |
| `Tab` / `Down` / `Ctrl-n` | Next suggestion |
| `Shift-Tab` / `Up` / `Ctrl-p` | Previous suggestion |
| `Enter` | Accept the selected suggestion |
| `Esc` / `Ctrl-c` | Close the menu |
| `Ctrl-Space` | Open the menu even with nothing typed |

`Enter` only submits the query when the menu is closed, so how a query gets run depends on where
it ends. `SELECT * FROM Movie WHERE id = 550` ends on a value that could be anything, which puts
nothing on offer, so the first `Enter` submits. Type a trailing space and the menu comes back
with `AND`/`OR`/`LIMIT`/`;` — there, `Esc` then `Enter` runs it, or accept `;` and press `Enter`,
since a terminated query has nothing left to suggest.

Mouse clicks are captured and routed to panes (`app.rs:472`).

## Autocompletion

The query box completes table names, field names, keywords, and boolean and null literals. All
of it comes from the client's own schema — `QuerySupport::table_names`, `table_fields`, and
`table_field_validator`, all generated by `#[db]` — so nothing is asked of the server and the
suggestions cannot drift from what the query parser will accept.

The menu is modelled on helix's: the best match is preselected, moving through it wraps, and it
closes the moment nothing matches. It is drawn on the row below the cursor, flipping above only
when there is no room. Matching is fuzzy and smart-cased, so `mov` finds `Movie` and `mvk` finds
`MovieByKeyword` while the canonical name is what gets typed in. The rest of the selected
suggestion is also shown dimmed after the cursor.

Unlike helix there is no trigger length: the menu opens wherever the grammar knows what could
come next, including with nothing typed at all. A brand new query box already shows `SELECT`.
That works because every position that allows something *unbounded* — the count after `LIMIT`,
the value of a numeric field, the body of a string — offers nothing, so an empty list is the
only gate the menu needs.

What is offered depends on where the cursor sits in the grammar:

| Position | Suggestions |
| --- | --- |
| Start of a query | `SELECT`, then `*`, then `FROM` |
| After `FROM` | Every table in the schema |
| After a table | `WHERE`, then its fields |
| After `WHERE`, `AND`, or `OR` | The table's fields, partition keys first, annotated with role and type |
| After a field | `=`, `IN` |
| After `=` | `true`/`false` for a bool, `null` for a nullable field, an opening quote for a string |
| After `IN` | `(` |
| Inside an `IN` list | The same values as after `=`, then `,` or `)` |
| After a condition | `AND`, `OR`, `LIMIT`, `;` |
| After `LIMIT` or inside a value | Nothing — a number or a string can be anything |

Only fields that can appear in a `WHERE` clause are offered. A field marked `#[shoal(update)]`
and nothing else has no role, so the parser rejects it — and the menu never suggests it.

## Writing queries

Queries are typed as [SHQL](../api/shql.md), so the UI inherits its limits: `SELECT *` only,
equality only, a mandatory `WHERE`, and no `ORDER BY` — so a `LIMIT` returns whichever rows the
scan reaches first.

Assume a schema with these two tables:

```rust
#[derive(ShoalUnsortedTable)]
#[shoal_table(db = "Tmdb")]
pub struct Movie {
    #[shoal(partition)]
    pub id: u64,
    #[shoal(filter)]
    pub title: String,
    #[shoal(filter)]
    pub watched: bool,
}

#[db]
pub struct Tmdb {
    pub movies: PersistentUnsortedTable<Movie, FileSystem>,
}
```

Then these all work:

```sql
SELECT * FROM Movie WHERE id = 550
SELECT * FROM Movie WHERE id = 550 AND title = 'Fight Club'
SELECT * FROM Movie WHERE id = 550 AND watched = true LIMIT 10;
SELECT * FROM Movie WHERE id IN (550, 551)
SELECT * FROM Movie WHERE id = 550 OR id = 551
select * from Movie where id = 550
```

### Four rules that catch people out

**The name after `FROM` is the Rust struct name.** It is `Movie`, not the `movies` field on the
schema struct and not a snake_cased table name. Keywords are case-insensitive but this
identifier is not, so `from movie` fails while `from Movie` works.

**`WHERE` is mandatory and must constrain a partition key.** Every read path starts by locating
a partition, so there is no way to express a full scan. `SELECT * FROM Movie` and
`SELECT * FROM Movie WHERE title = 'Fight Club'` are both errors — the second parses fine and
then fails to bind because `title` is a filter, not the partition key.

**Strings use single quotes and have no escapes.** `'Fight Club'` is a string; `"Fight Club"` is
not. There is no escape syntax, so a value containing a single quote cannot be written at all.

**`OR` and `IN` choose values, `AND` joins fields.** To read two partitions at once write
`id IN (550, 551)`, or `id = 550 OR id = 551` — they mean the same thing. `AND` is for conditions
on *different* fields, and naming one field twice with it is an error, because two values for one
field are a union in shoal rather than an intersection and the query would have meant the
opposite of what it says. Rows come back partition by partition in the order you listed them, so
`LIMIT 2` on `IN ('a', 'b')` gives you the first two rows of `'a'`.

### What the errors mean

| Message | Cause |
| --- | --- |
| `Expected SELECT * FROM <table>` | The query does not begin `SELECT * FROM <identifier>`, usually a named column instead of `*` |
| `A WHERE clause is required, and it must constrain a partition key` | No `WHERE` clause at all |
| `Expected '=' or IN after field 'x', SHQL only supports equality` | A `<`, `>`, `!=`, or `LIKE` was used |
| `'x' is constrained twice by AND` | One field was given two values with `AND`; the message names the `IN` list to write instead |
| `'x' cannot be OR'd with 'y'` | `OR` was used between two different fields, which shoal cannot answer without a full scan |
| `IN needs at least one value for field 'x'` | An empty `IN ()` list |
| `Trailing comma in the IN list for field 'x'` | A comma with no value after it |
| `Expected ',' or ')' in the IN list for field 'x'` | The list was never closed |
| `Expected a value for field 'x'` | The literal is malformed — an unterminated string, or a number too large for its type |
| `Unexpected trailing input` | Something follows the query, including a dangling `AND` or `OR` |
| `Unknown table 'x'` | No table in the schema has that struct name |
| `Unknown field 'x'` | The table has no field by that name, or the field is marked neither partition, sort, nor filter |
| `Type mismatch for field 'x'` | The literal cannot deserialize into the field's declared type |
| `Missing partition key in WHERE clause` | The query parsed but constrains no partition key |

Parse errors are shown in the UI and the query is left in the bar for editing.

## Rendering results

Responses are formatted through generated code rather than by shoalctl itself:

```rust
fn format_response(archived: &<Self::ResponseKinds as Archive>::Archived)
    -> Option<(Vec<&'static str>, Vec<Vec<String>>)>;
```

`shoal-core/src/shared/traits.rs:193-195`

The generated impl matches the response variant, pulls `TableRowFormat::headers()` and
`row_values()` for the archived row type, and returns strings
(`shoal-derive/src/structs/client.rs:85-108`). Note it formats **archived** rows — shoalctl
renders query results without ever deserializing them.

`format_ascii_table` (`app.rs:33`) then lays those out with `unicode-width` for correct column
alignment on wide characters.

## Known rough edges

The parse-error branch in `Tab::submit_query` used to carry an unconditional `panic!` with an
unreachable `return` after it, so any typo crashed the TUI and — because the terminal restore in
`run` is bypassed by a panic — could leave the terminal in raw mode. It now records the error
for rendering and returns:

```rust
Err(e) => {
    // show the parse error in the UI and leave the query for the user to fix
    self.error = Some(format!("Parse error: {}", e));
    return;
}
```

`shoalctl/src/components/tab.rs:141-145`

`TabState::next` and `TabState::prev` are dead code (`components/tab.rs:430`, `:440`).

`Tab::submit_query` still `tokio::spawn`s the query and then immediately `.await`s the join
handle (`tab.rs:153-159`), so the spawn buys nothing and the UI blocks for the round trip.

`app.rs:436` carries `// TODO: Handle insert mode for editing rows` — insert mode edits the
query bar only; result rows are read-only.

There is one doctest, in the crate-level docs (`lib.rs:8`), and it is compile-only
(`no_run`). It is the only test in the crate, and it passes.

## Design notes

**A library, not a binary.** The schema is a type, so a generic binary is impossible without
runtime reflection. Making shoalctl a library and asking users to write a ten-line `main` is
the honest consequence.

**One channel for input and results.** Merging terminal events and query completions into a
single stream keeps the event loop a simple `select`-free `recv` and guarantees the UI stays
responsive while a query is in flight.

**Formatting lives in generated code.** `TableRowFormat` is derived per table, so shoalctl
needs no knowledge of any schema and no reflection — and it renders straight from archived
rows.

## Limitations

- The schema must be duplicated into the shoalctl binary, with no shared-crate pattern and no
  drift detection.
- Read-only: SHQL parses no writes, so shoalctl cannot insert, update, or delete.
- Every query must name a partition key, so there is no way to browse a table.
- Dead `next`/`prev` methods.
- The query round trip blocks the UI despite being spawned.
- No connection retry or reconnect if the server goes away.
- Effectively untested — the crate itself has one compile-only doctest, though the SHQL parsing
  it depends on is now covered in `shoal-core` and `shoal/tests/shql.rs`.

[ratatui]: https://ratatui.rs/
