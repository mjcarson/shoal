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
                                     ▼
                          ratatui-hypertile panes
                            └── TabState per pane
                                 ├── query_bar
                                 └── content
```

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
| `[` / `]` | Resize the query pane by 5% |
| `Space` | Enter shortcut mode |

Shortcut mode (`app.rs:402-432`):

| Key | Action |
| --- | --- |
| `q` | Focus the query bar |
| `w` | Clear the query |
| `p` | Focus a pane |

Mouse clicks are captured and routed to panes (`app.rs:472`).

Queries are typed as [SHQL](../api/shql.md), so the UI inherits its limits: `SELECT *` only,
equality only, a mandatory `WHERE`, and a `LIMIT` the server ignores.

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

`cargo check` reports two warnings in `shoalctl`, both real:

```rust
panic!("{:#?}", self.error);
return;
```

`shoalctl/src/components/tab.rs:144-145`

An unconditional `panic!` on a code path meant to *display* an error, with an unreachable
`return` after it. Rather than showing the error in the UI, shoalctl crashes — and because the
terminal restore in `run` is bypassed by a panic, it can leave the terminal in raw mode.

`TabState::next` and `TabState::prev` are dead code (`components/tab.rs:430`, `:440`).

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
- `panic!` instead of rendering errors, which can leave the terminal unusable.
- Dead `next`/`prev` methods.
- No connection retry or reconnect if the server goes away.
- Effectively untested.

[ratatui]: https://ratatui.rs/
