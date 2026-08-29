# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# You can't build just shoal on its own you can either build an example or the tests
# To build the examples you can use
cargo build --example tmdb
cargo build --release --example tmdb_dataset

# the benchmark workloads are the other thing that instantiates a schema
cargo build --release --bin shoal-workload

# Build with release optimizations (recommended for benchmarking)
cargo build --release

# Run tests
cargo test

# Run a specific test
cargo test <test_name>

# the client compiles a schema with no engine in its graph. this is D5's regression test: it
# fails to resolve `glommio` if a server path creeps back into the client half of #[shoal::db]
cargo check -p shoal-client-check --no-default-features

# the explorer's native window is behind a non-default feature, so `--workspace --all-targets`
# does not compile it. this is the same idiom as the line above, for the same reason
cargo check -p shoal-top --features native

# and the browser half, which is the target that actually gets used. RUSTFLAGS is set explicitly
# because the shell exports `-C target-cpu=native`, which reaches wasm32 and beats every config file
cd shoal-top && RUSTFLAGS="-C target-cpu=generic" \
    cargo check --target wasm32-unknown-unknown --lib --features web

# the client half of the trace context is behind a feature, so `cargo test -p shoal` compiles
# `trace_propagation.rs` away. a `--workspace` run does build it, because `shoal-bench` enables
# `shoal/otel` and cargo unifies features - this is the form that runs it on its own (F35)
cargo test -p shoal --features otel --test trace_propagation

# and the same property on a program somebody runs
cargo build -p shoalctl

# Run with hotpath profiling enabled (attribution only, never a baseline number)
cargo build --release --bin shoal-workload --features hotpath

# Run the TMDB example. Needs no config, no dataset and no flags - it starts its own
# server against a temp dir under target/ and reads the rows back four ways.
cargo run --example tmdb

# The same two tables against the real dataset, which is the one example that needs
# something off disk: TMDB_movie_dataset_v11.csv (538MB, from kaggle, not in this repo)
# at ~/datasets/ or --dataset, and a shoal.yml in the cwd or the defaults' /opt/shoal.
# --limit loads a slice. The rows/sec it prints is not a measurement - see shoal-bench.
cargo run --release --example tmdb_dataset -- --limit 10000
```

## Benchmarking

Everything goes through `shoal-bench`, the workspace crate that runs the benchmarks, stores the
results with their provenance, compares them, and generates the book's results page.

```bash
# capture a full run: micro + macro + hotpath + stages, into docs/perf/runs/
cargo run -p shoal-bench --release -- run --label <label>

# a subset while iterating, filtered the way `cargo test` filters tests
cargo run -p shoal-bench --release -- list get_key
cargo run -p shoal-bench --release -- run --label <label> --layer micro get_key

# the named groups, what each answers, and what a capture of it would cost
cargo run -p shoal-bench --release -- list --groups

# one question rather than the whole suite. --group combines with or, and intersects
# with --layer and the filters. it selects only: nothing runs concurrently, ever
cargo run -p shoal-bench --release -- run --label <label> --group conf/storage

# a whole capture at a fraction of the data, for checking a workload runs at all
cargo run -p shoal-bench --release -- run --label <label> --scale smoke --runs 2

# what workloads exist, and what each one isolates
./target/release/shoal-workload list

# compare against the frozen baseline and the trailing one, which is the default pair
cargo run -p shoal-bench --release -- compare <label>

# what has been captured, and whether it still describes the current code
cargo run -p shoal-bench --release -- status

# regenerate the results pages, or check that the committed ones are current
cargo run -p shoal-bench --release -- render
cargo run -p shoal-bench --release -- render --check

# the interactive explorer (F29). unlike `render`, this draws any number of captures on one chart,
# against either a swept fact or the capture timeline. a native window needs a display, so on a
# machine reached over ssh the second form is the one that works
cargo run -p shoal-bench --release -- explore
cargo run -p shoal-bench --release -- explore --serve      # http://127.0.0.1:8321
cargo run -p shoal-bench --release -- explore --index-only  # target/explore/index.json
```

`--serve` needs `wasm-bindgen` at **exactly** the version the lockfile resolved; it refuses to build
with the `cargo install` line to run rather than producing a bundle that fails in the browser. The
index is written to `target/explore/` and must never be committed - `gather` computes `Page::dirty`
from every uncommitted path outside `docs/src/performance/`, so a committed index would break
`render --check` permanently.

`docs/perf/baselines/B1-performance.json` is frozen and never overwritten — `promote` refuses it
with no override. `shoal.yml` is the benchmark config and is committed — changing it invalidates
the baseline. The benchmarks assume the CPU governor is `performance`. `run` refuses a dirty tree
unless given `--allow-dirty`, because a measurement of bytes that exist in no commit cannot be
located in history afterwards.

### Commit first, then capture

**A capture taken on a dirty tree is marked `uncommitted` in its provenance and stays that way
forever.** There is no fixing it up afterwards: the label records the commit it ran against, and if
that commit does not contain the bytes that were measured, the number can never be located in
history. So the order is always:

1. **Commit the change.** All of it — a capture against a half-committed tree is the same problem.
2. **Capture**, on a clean tree, with the `performance` governor.
3. **`render`**, and commit the regenerated pages under `docs/src/performance/` as a follow-up.

`--allow-dirty` exists for throwaway captures while iterating on a workload, never for one whose
numbers are going to be committed or quoted. A capture taken that way should be deleted from
`docs/perf/runs/` rather than left in the corpus, because everything in that directory appears on
the freshness table as though it were a real measurement.

### When a capture is needed

Not on every change. Take one when:

- **A workload was added, removed or changed.** Its own source fingerprint moved, so every existing
  capture is correctly reported as no longer describing it.
- **`shoal.yml`, the seed, or anything in `shoal-bench/src/workloads/` changed.** Same reason, and
  this includes adding a field to `ScaleFacts` or a row type to the shared schema — both change the
  fingerprint of *every* workload.
- **A change claims a performance effect.** `docs/src/appendix/optimizations.md` will not accept an
  entry as acted on until a benchmark exists that would show the difference, and names it.

Do **not** take one for a docs-only change, a renderer change, or a test-only change. For a renderer
change, `render` and `render --check` are the verification; the artifacts underneath are untouched.

A full capture was **seventy-five minutes**, measured by `F20-conf` on 2026-08-22 — the first one
anybody timed. This file said four to five hours until then, and that figure was never a
measurement. [F22](docs/src/features/row-size-benchmarks.md) then added 165 macro arms, so the
projection is now **about two hours** — and that figure is a projection again, scaled from the
measured one by arm count, until somebody times a capture of the current set. While iterating, `--scale smoke --runs 2` runs the whole set at a hundredth of the
data and proves every workload still runs — which is what you want before spending the hour on the
real one. Budget twenty minutes for a macro-only smoke pass; the criterion layer is what makes a
full smoke run take longer than you expect.

The macro layer is three hundred and seventy four **workloads** living in `shoal-bench/src/workloads/`,
each generating its own rows from `--seed` — there is no dataset to fetch
([F8](docs/src/features/purpose-built-workloads.md)). They come in three kinds and the differences
matter:

- **Isolating workloads** drive one path each, so a difference between two captures can be
  attributed to something. Eight of them are storage-free controls over ephemeral tables, each
  paired with a persistent workload it is read against
  ([F9](docs/src/features/ephemeral-tables.md)) — a pair differs in the storage engine and in
  nothing else, and each pair has a test asserting that, so a constant changed in one half has to
  change in the other.
- **The grid** is two hundred and twenty-nine workloads driving a *mixture* of reads and writes,
  swept across row width, read share, key distribution and load depth
  ([F17](docs/src/features/workload-grid.md), extended by
  [F22](docs/src/features/row-size-benchmarks.md)). It answers what a caller's workload costs. The
  width axis is sixteen widths against all four tables at three mixtures, plus a rung at each width
  with one query outstanding.
  **A regression is never attributed to a grid arm** — the grid says a mixture got slower, the
  isolating pairs say which half.
- **The configuration sweep** is fifty-eight workloads under `macro/conf/`, each one the grid's
  reference cell `macro/grid/unsorted/r50/1024` with **exactly one field** of the server
  configuration moved ([F20](docs/src/features/configuration-sweeps.md)). It answers what a setting
  in `shoal.yml` is worth. Every sweep contains the value the committed `shoal.yml` resolves to, and
  a test fails if one stops bracketing it — so retuning that file is a test failure rather than a
  sweep that quietly stops covering the configuration everything else is measured under.

They are compiled into a second binary of that crate, `shoal-workload`, which the runner builds and
spawns; the runner half still builds with `--no-default-features` and no engine at all, which is
what lets a capture be judged while `shoal-core` will not compile. **A workload's identifier is the
key every comparison joins on, so renaming one orphans every capture taken before the rename.** Add
and deprecate instead — and **append**, never interleave: a workload's position in
`workload_ids::IDS` decides the TCP port a capture gives it. Adding a workload means one file, one
line in `workloads::all()`, one line in `workload_ids::IDS`, and a family in
`shoal-bench/src/render/family.rs` — a test fails if you forget any of the last three.

A full capture is about two hours and its macro layer is most of it, so **use a group**
rather than a prefix ([F21](docs/src/features/benchmark-groups.md)) when a question is narrower than
the whole set. `list --groups` prints the twelve declared sets, what each answers, and what a
capture of it would cost — projected onto `FULL_MACRO_CAPTURE_SECS`, which is hand-maintained and
has not been re-measured since the macro layer grew by 165 arms, so every projection it prints is
currently low; `--group grid` is the grid alone, `--group
isolating` is everything that drives one path, `--group conf/storage` is the writer knobs. A group
**selects and never schedules** — a capture still runs one `shoal-workload` process at a time, and
must, because two servers at once share a page cache, a device queue and a set of cores. `--scale
smoke` runs anything at a hundredth of the data.

**Everything under `docs/src/performance/` except `benchmarking.md` and `baseline.md` is
generated** — never edit those pages by hand, run `shoal-bench render`, which writes all eleven or
none. Each page opens with four mandatory blocks (what it measures, how to read it, what would make
it wrong, what it cannot tell you) that come from its family
([F18](docs/src/features/results-pages.md)). See `docs/src/performance/benchmarking.md` for the
runbook, `docs/src/performance/baseline.md` for the frozen numbers and hardware, and
`docs/src/features/bench-runner.md` for how the tool is put together.

## Fixing a Bug

**Every bug fix updates the docs in `docs/src/`, in the same change as the code.** The reasoning
behind a fix is worth more than the fix, and a page that still describes the broken behaviour is
worse than no page. This is not optional and does not need to be asked for.

1. **Find the item.** Open `docs/src/appendix/known-issues.md` and locate the defect. Keep its
   number — numbers are shared between the known and resolved pages and are **never reused**, so a
   number appears on exactly one of them. A defect that is not filed yet gets the next free number.
2. **Reproduce before fixing.** Write the test first, run it against the unfixed tree, and keep the
   actual failure output. That output goes into the **Evidence** section, which always states
   whether the defect was established by reading the source or by reproducing it. A test written
   after the fix and assumed to cover it is not evidence.
3. **Fix it**, then move the item:
   - delete it from `known-issues.md` — or, if only part of it was fixed, leave the open remainder
     there and say so on both pages, as items 9, 20, and 24 do;
   - add a row to `docs/src/appendix/resolved-issues.md`;
   - write `docs/src/appendix/resolved/<slug>.md`.
4. **The resolved page always uses the same sections**, in this order: **Symptom, Cause, Evidence,
   The fix, Alternatives rejected, Invariants to uphold, Still open, Tests, Related.** Two of these
   carry most of the value. *Alternatives rejected* — a fix is only understandable next to what it
   is not. *Invariants to uphold* — this is the section someone reads before changing that code
   again, so state what the fix depends on, not what it does. The *Tests* table names what breaks
   if the fix is reverted, one row per test.
5. **Register the page** in `docs/src/SUMMARY.md`, in item-number order.
6. **Sweep the rest of the docs for anything the fix just made false.** `grep -rn "<feature>"
   docs/src` and read every hit. The pages that go stale most often are
   `tables/query-execution.md` (its Limitations list), `tables/table-types.md`,
   `tables/partitions.md`, `api/shql.md`, `api/derive-macros.md`, `appendix/glossary.md`,
   `appendix/todos.md`, `appendix/optimizations.md`, and `appendix/test-coverage.md`. Check the
   "Still open" sections of other resolved pages too — a follow-up one of them called for may be
   what you just did.
7. **File what you found on the way.** New defects go to `known-issues.md` at the next free number;
   performance findings go to `optimizations.md` as a new `O` number; work that is a missing
   feature rather than a defect goes to `todos.md`. An optimization you deliberately did not take
   is recorded, not dropped.
8. **Re-run and re-count.** `cargo check --workspace --all-targets` and `cargo test --workspace`,
   then update the baseline counts in the headers of `known-issues.md` and `test-coverage.md`, and
   the per-binary counts in `test-coverage.md`.

Code style for the fix itself follows the rest of the repo: a docstring on every function, struct,
enum, and method, and an inline comment for each step inside them.

## Adding a Feature

**Every feature updates the docs in `docs/src/`, in the same change as the code.** A bug fix earns
a page when it had a wrong mental model behind it; a feature earns one when it introduces a model
that was not there before, because the next person to touch that code will reason from the model
rather than from the diff. This is not optional and does not need to be asked for.

1. **Find what it was filed as.** Most features are already described in
   `docs/src/appendix/todos.md`, often with a design sketch. Read it first — it is usually right
   about the hard part, and where it is wrong that is worth saying on the page you write.
2. **Take the next free `F` number.** They are shared across
   `docs/src/features/delivered-features.md` and never reused, the same rule issue and `O` numbers
   follow.
3. **Build it**, then write `docs/src/features/<slug>.md`. **The page always uses the same
   sections**, in this order: **Context, What it does, Design choices, Alternatives rejected,
   Limitations, Invariants to uphold, Performance, Tests, Related.** Three of these carry most of
   the value. *Alternatives rejected* — a design is only understandable next to what it is not.
   *Limitations* — the gap between what a feature looks like and what it does, written down rather
   than discovered. *Invariants to uphold* — what the feature depends on, not what it does. The
   *Tests* table names what breaks if the feature is reverted, one row per test.
4. **Add a row to `delivered-features.md`** and register the page in `docs/src/SUMMARY.md`, in
   `F` number order.
5. **Sweep the rest of the docs for anything the feature just made false.** `grep -rn "<feature>"
   docs/src` and read every hit. A feature makes *more* pages stale than a fix does, because it
   changes what the system can do rather than what it does correctly. The pages that go stale most
   often are `api/shql.md`, `api/derive-macros.md`, `tables/query-execution.md`,
   `tables/table-types.md`, `tables/partitions.md`, `appendix/glossary.md`, `introduction.md`, and
   `operations/shoalctl.md`. Check the "Still open" sections of the `resolved/` pages and the
   entries in `optimizations.md` too — a feature often closes one.
6. **Say so where the old claim was.** A rule the feature changed is struck through and kept with
   what replaced it, not deleted: `resolved/sort-keys.md` and `optimizations.md` both do this. A
   reader who learned the old rule needs to find out it moved.
7. **File what you found on the way**, the same way a fix does: `known-issues.md`,
   `optimizations.md`, `todos.md`. A piece of the feature you deliberately did not build is
   recorded in `todos.md` with the reason, not dropped.
8. **Re-run and re-count.** `cargo check --workspace --all-targets` and `cargo test --workspace`,
   then update the baseline counts in the headers of `known-issues.md` and `test-coverage.md`, and
   the per-binary counts in `test-coverage.md`.

Code style is the same as for a fix.

## Architecture Overview

Shoal is a high-performance, distributed database with persistence, built on three crates:

### Crate Structure

Six implementation crates plus a facade. The split is
[F15](docs/src/features/client-server-split.md) and the rule it enforces is simple: **nothing
outside `shoal-proto`, `shoal-client`, `shoal-core` and `shoal-derive` names any of them — callers
go through `shoal`.**

- **shoal-proto** - The wire format and everything both peers agree about: `shared/protocol/`
  (framing, handshake, auth frames), `shared/queries/` (including the SHQL parser),
  `shared/responses.rs`, `shared/traits.rs`, `shared/auth/` (SCRAM), `shared/tls.rs` (rustls
  config, no I/O), `client/errors.rs`, `stamps.rs`. **Links no async runtime.** Do not add tokio,
  glommio or kanal here
- **shoal-client** - The tokio client: `Shoal<S>`, the three streaming modes, the `bb8` pool.
  Links no storage engine
- **shoal-core** - The database engine:
  - `server/` - Shard management, query handling, TCP listener
  - `server/database.rs` - `ShoalDatabase`, the trait a schema implements to be served
  - `server/routing.rs` - `ShardRouting`, which splits a query across the shards owning its keys
  - `server/tables/storage/` - Filesystem storage with DMA (glommio-based)
  - `pub use shoal_proto::shared` - so the whole of `server/` names the protocol unchanged.
    Depends on `shoal-proto`, and **never** on `shoal-client`
- **shoal-derive** - Procedural macros. Emits `::shoal::` paths only and depends on no shoal crate
- **shoal** - The facade. `default-features = false` drops `shoal-core` and leaves a client
- **shoal-client-check** - A schema that compiles against the client alone. Not a library: it
  exists to fail if a server path creeps back into the client half of `#[shoal::db]`
- **shoal-top** - The benchmark explorer ([F29](docs/src/features/benchmark-explorer.md)):
  `index.rs` holds the portable index types and depends on `serde` alone, and everything behind the
  `ui` feature draws them with `egui`/`egui_plot`. **`shoal-bench` enters it with
  `default-features = false`**, which is what keeps egui out of `cargo tree -p shoal-bench
  --no-default-features`. It must never depend on `shoal-bench`: that crate pulls `walkdir`, which
  does not build for `wasm32-unknown-unknown`, and the explorer's primary target is a browser

### Key Abstractions

**Table Types:**
- `PersistentSortedTable<T, Storage, TableNames>` - Partitioned + sorted with disk persistence
- `PersistentUnsortedTable<T, Storage, TableNames>` - Partitioned only with disk persistence
- `EphemeralSortedTable<T>` / `EphemeralUnsortedTable<T>` - In-memory only. **Aliases**, not
  implementations: the same two tables with `NoStorage` as the engine, so they cannot drift from
  the persistent pair ([F9](docs/src/features/ephemeral-tables.md)). A schema writes one generic
  and the macro fills the rest in. Nothing they hold is ever evicted, and nothing survives a
  restart.

**Macros:**
- `#[derive(ShoalSortedTable)]` - Generates query structs (Get, Insert, Update, Delete) and trait impl for sorted tables
- `#[derive(ShoalUnsortedTable)]` - Same for unsorted tables
- `#[shoal_db]` - Attribute macro that rewrites table field types (adds `<Self>` to storage and `TableNames`; for an ephemeral table there is no storage generic to add it to, so `Self` is pushed as one) and generates `TableNames` enum, `*Client` struct, `QueryKinds`/`ResponseKinds` enums. It classifies a field by looking for `Sorted`/`Unsorted`/`Persistent`/`Ephemeral` in the type name, so a table type named anything else panics during expansion

**Field Attributes:**
- `#[shoal(partition)]` - Partition key (required)
- `#[shoal(sort)]` - Sort key (sorted tables only)
- `#[shoal(filter)]` - Filterable field
- `#[shoal(update)]` - Updatable field
- `#[shoal_table(db = "DbName")]` - Links table to database schema

### Server Model

- One shard per CPU core (core 0 reserved for coordination)
- Uses `glommio` LocalExecutor for thread-per-core async I/O
- `kanal` channels for lock-free inter-shard communication
- Consistent hash ring for partition routing

### Client Model

- `Shoal<S>` wraps TCP connection pool (min 10, max 50 connections)
- Three streaming modes: `send()`, `stream()`, `stream_unordered()`
- UUID-based query tracking for response routing

### Wire Protocol

- Client→Server: `[8-byte length][rkyv-serialized Queries]`
- Server→Client: `[16-byte UUID][8-byte length][rkyv-serialized ResponseKinds]`

## Configuration (shoal.yml)

```yaml
resources:
  cores: 12                    # Number of cores for shards
  exclude_cores: [12, 13, 14, 15]  # Physical cores to exclude, both SMT threads of each
  memory: "4Gi"                # Memory limit; exceeding it evicts 40% of current usage
storage:
  default:
    filesystem:
      latency_sensitive:
        path: "/opt/shoal"
      throughput_sensitive:
        path: "/opt/shoal"
tracing:
  level: Info                  # Trace/Debug/Info/Warn/Error/Off
```

## Usage Pattern

```rust
// 1. Define table with derive macros
#[derive(ShoalUnsortedTable)]
#[shoal_table(db = "MyDb")]
pub struct MyTable {
    #[shoal(partition)]
    pub id: u64,
    #[shoal(filter)]
    pub name: String,
}

// 2. Define database schema
#[shoal_db]
pub struct MyDb {
    pub my_table: PersistentUnsortedTable<MyTable, FileSystem>,
}

// 3. Start server
let conf = Conf::from_file("shoal.yml")?;
let pool = ShoalPool::<MyDb>::start(conf)?;

// 4. Create client and send queries
let client = Shoal::<MyDbClient>::new("127.0.0.1:12000").await?;
let mut results = client.send(queries).await?;
```

## Key Dependencies

- `rkyv` - Zero-copy serialization (all data types must derive rkyv traits)
- `glommio` - Thread-per-core async runtime with DMA filesystem support
- `gxhash` - Fast hashing for partition keys
- `kanal` - Lock-free channels
- `bb8` - Connection pooling
