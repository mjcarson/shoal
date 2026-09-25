# F54. The TMDB dataset as a deployable database, and a loader for it

## Context

`shoal/examples/tmdb_dataset.rs` was a tour. It started a `ShoalPool` in its own process against
`./shoal.yml` or the built-in defaults, loaded the csv into it over loopback, read a few rows
back, and stopped the pool. Almost everything around the load was configuration: which config
file, `--storage` to move the roots off `/opt/shoal`, `--local-tracing` to drop the lab's
collector, a memory default patched in by hand
([Resolved #126](../appendix/resolved/storage-directory-unusable.md)). None of that applies to a
deployed cluster, where `cluster bootstrap` renders every node's `shoal.yml`. And nothing let the
dataset be deployed at all: `cluster bootstrap` needs a node program built from the schema and a
`shoalctl` program built from the same schema, and the example was neither.

The user asked for the example to become a folder holding a deployable database and a loader,
compiled and deployed through the inventory wizard ([F53](inventory-wizard.md)), with the loader
able to fill a deployed cluster easily.

## What it does

`examples/tmdb_dataset/` is a workspace crate, `tmdb-dataset`, with a library and two binaries:

| Target | What it is |
| --- | --- |
| `tmdb_dataset` (lib) | The schema: `Movie`, `MovieByKeyword` and `#[shoal::db] Tmdb`, plus `load`, the pipeline |
| `tmdb-dataset-node` | `shoal::server::node::main::<Tmdb>()` under mimalloc. The program an inventory's `server:` names |
| `tmdb-dataset-loader` | `load`, plus every `shoalctl` command for this schema: `cluster new`, `bootstrap`, `status`, `tui` and the rest |

The whole flow:

```bash
# the node for the oldest cpu among the hosts, never native, in its own target dir
CARGO_TARGET_DIR=target/deploy RUSTFLAGS="-C target-cpu=znver1" \
    cargo build --release -p tmdb-dataset
target/deploy/release/tmdb-dataset-loader cluster new -o tmdb.yml
#   server: target/deploy/release/tmdb-dataset-node
target/deploy/release/tmdb-dataset-loader cluster bootstrap -i tmdb.yml
target/deploy/release/tmdb-dataset-loader load -i tmdb.yml --dataset ~/datasets/TMDB_movie_dataset_v11.csv
target/deploy/release/tmdb-dataset-loader tui -i tmdb.yml
```

**`load -i <inventory>`** reads the deployment's local state (`~/.shoal/clusters/<name>/`). It
connects to every recorded member as the cluster's admin and leaves out, with a warning, any
member that does not answer within ten seconds. Its `--workers` (default eight) are spread round
robin over the members, so every member coordinates a share of the writes and routes each to the
node that owns it. The csv is read on a blocking thread. Each movie becomes one `Movie` insert and
one `MovieByKeyword` insert per keyword, sent in batches of `--batch` on an unordered stream per
worker, behind a gate of `--in-flight` outstanding queries (default 1,024, ~~4,096~~). A csv row
that will not parse is skipped and counted. ~~A write that fails stops the load with the error.~~
A query that fails with a code saying to try again (`OutcomeUnknown`, `Shedding`, `NotLeader`,
`Unavailable`, `QuorumUnavailable`, `ConnectionLost`, `Timeout`) is sent again after a backoff
that doubles from 20ms to 500ms, up to `--retries` times (default eight). Any other failure, or a
query that outlasts its retries, stops the load with the error. The count of retries is printed
at the end.

Afterwards `--verify` movies (default 10,000), sampled at a fixed stride over the file and
deduplicated, are read back. A movie that was written and is not found fails the run.

`--addr <host:port>` loads a single node without credentials instead, for a node started by hand.

## Design choices

- **A workspace crate, not files under `shoal/examples/`.** The loader connects through
  `shoalctl::deploy`, and `shoalctl` depends on `shoal`, so the loader cannot be an example of the
  `shoal` crate. This is the same arrangement as `shoal-bench`'s `shoal-node` and
  `shoal-benchctl`: one package, one schema, both binaries.
- **The schema is the library, not an `include!`.** The TMDB pair F51 built shares
  `shoalctl/examples/tmdb/tables.rs` by `include!` because its two halves live in two crates.
  Here both binaries live in one crate and use one type, so their fingerprints agree by
  construction.
- **The loader carries the shoalctl commands.** `shoalctl::cli::Command` is now public and
  `shoalctl::cli::run` takes one. The loader's own `Command` enum flattens it beside `Load`. One
  program therefore runs the wizard, bootstraps the cluster, loads it and queries it, and it
  cannot be built against a different schema from the loader's.
- **The loader needs the state, not the node binary.** `Deployment::attach` opens an inventory
  through the new `Inventory::read`, which judges the shape and does not look for the `server`
  program. A loader, or `tui -i`, therefore runs on a machine that never built the node.
  `Deployment::open` still checks for the program, since bootstrap and add copy it.
- **Stopping is safe because loading is idempotent.** An insert replaces the row with the same
  partition key and sort key (`SortedPartition::insert`; `table-types.md` for the unsorted table),
  and every row's key is derived from the movie. A stopped load is finished by running it again.
  That is why the loader stops on a failure it cannot retry instead of counting failures and
  carrying on, and why it keeps no resume point.
- **Retrying is safe for the same reason, and a retry is a new query.** The first run against the
  lab stopped six seconds in on `OutcomeUnknown`: eight workers × 4,096 in flight is a queue, and
  a write that waited in it past `replication.write_timeout` could not commit
  ([item 129](../appendix/known-issues.md#129-an-overloaded-group-answers-outcomeunknown-rather-than-shedding),
  and [Resolved #128](../appendix/resolved/hop-deadline-margin.md) for why it read as a timed out
  RPC). ~~The loader stops on the first failed write.~~ Each worker keeps the row behind every query it
  has sent, by the query's index in its stream (`Pipeline::outstanding`), and a retriable failure
  puts that row back behind a backoff. The resend is not the client's own `exec_with` retry,
  which repeats a bundle under the same identity. A stream's bundles cannot be repeated one query
  at a time, and a replacing insert does not need the identity to be safe.
- **A worker reads its stream to the end before it drops it.** The first run with retries
  wrote the whole dataset and then hung in verify. Each worker had broken out once every query
  was answered, closed the stream and dropped it without reading the end the close sends. That
  left the stream's slot in the client's channel map pointing at a closed channel, and a late
  frame for it ends the read loop of the connection it lands on
  ([item 130](../appendix/known-issues.md#130-a-frame-for-a-stream-dropped-without-its-end-ends-that-connections-read-loop)),
  stranding the verify stream's answers on that connection
  ([item 131](../appendix/known-issues.md#131-a-connection-that-dies-fails-only-the-streams-it-was-the-last-connection-for)).
  After `close`, the worker now calls `next()` until it returns `None`, which releases the slot.
- **A smaller gate.** 1,024 in flight per worker is still more than four times `--batch`, which
  `validate` requires. The old 4,096 bought nothing past what the cluster commits: it only made
  the queue longer than `write_timeout`.
- **A keyword row sorts by title and then id.** The example sorted `MovieByKeyword` by `title`
  alone, so two films of one title under one keyword were one row, and whichever loaded last won.
  The sort key is now `order`, which is the title, a `0x1f` separator and the id padded to twenty
  digits (`MovieByKeyword::order`). A sort key is one field: the derive accepts two
  `#[shoal(sort)]` fields, but a tuple is not `RkyvSupport`. So the pair is one string, the same
  way the bench schema pads its `Event` sort key (`SORT_KEY_WIDTH`). `title` and `id` stay as
  plain fields for reading.

## Alternatives rejected

- **Keeping the tour and adding a `--cluster` mode.** Every configuration step the tour needed
  (a config file, storage, tracing, a memory default) is wrong or meaningless against a deployed
  cluster, and a program with both modes has two sets of flags that each half ignores.
- **Two crates, a node and a loader, sharing tables by `include!`.** That is what F51 did for
  `tmdb_node` and `tmdbctl`, and it only works because nobody edits one without the other. One
  crate makes the shared schema a type instead of a convention.
- **A loader-only binary plus a separate `tmdb-datasetctl` for the cluster commands.** That
  would be a third program that has to be built from the same schema, for no gain.
- **Reading the admin password and member addresses directly from `~/.shoal/clusters`.** That
  duplicates `State`'s layout, `SHOAL_DEPLOY_HOME` and `SHOAL_ADMIN_PASSWORD`. `Deployment::attach`
  and `Deployment::connect` are the deployment's own code.
- **Counting failed writes and reporting them at the end.** A failed write is a movie missing from
  the database, and a count does not say which one. Stopping and rerunning (safe, see above) puts
  it back.
- **Retrying through `Shoal::exec_with`.** That retries a whole bundle under one identity and
  waits for the bundle to finish, so it would give up the unordered stream's pipelining that the
  loader's throughput comes from.
- **Retrying without a limit.** A cluster that cannot take a write at all (a group with no
  quorum) would hold the load forever. Eight doublings up to the cap is about three seconds of
  waiting per query, and after that the load stops with the error, which reruns can finish.
- **Linking the engine only into the node.** One package has one feature set. Gating the engine
  behind a feature the node requires would save compile time on the loader and nothing at run
  time, at the cost of a feature a reader has to know to pass. `shoal-benchctl` makes the same
  trade.

## Limitations

- **The schema is not `tmdbctl`'s.** `MovieByKeyword` has `order` and `id`, and `Movie` reads the
  csv leniently, so `tmdbctl` refuses a `tmdb-dataset-node` at the hello and vice versa. Use
  `tmdb-dataset-loader tui` against this cluster.
- **No resume point.** A stopped load is rerun from the top of the file. On the full dataset that
  is minutes of rewriting rows that are already there.
- **The loader links the engine.** It never starts a shard, but it is a larger binary than a
  client needs.
- **Verification samples movies, not keyword rows.** A keyword row that failed to land would have
  failed its write and stopped the load, but nothing reads keyword rows back.
- **Retries hide overload rather than fix it.** A load that retries a lot is a load pushed past
  what the cluster commits. The server answers that with `OutcomeUnknown` at the write deadline
  rather than a cheap `Shedding` at admission
  ([item 129](../appendix/known-issues.md#129-an-overloaded-group-answers-outcomeunknown-rather-than-shedding)).
  Lower `--in-flight` or `--workers` if the retry count is large.
- **A retried query is not deduplicated.** It is safe only because every write this loader makes
  replaces its row. A write that did not (an update that adds, or an insert with a generated key)
  could not be retried this way.
- **A member that did not answer at connect time is not retried.** The rows still reach it,
  because the members that did answer route to it, but it coordinates none of the load.

## Invariants to uphold

- **Both binaries use `tmdb_dataset::Tmdb`.** A node and a loader built from one commit agree on
  the fingerprint. A second schema struct in either binary brings back the drift `include!` was
  guarding against.
- **Every row's key is a function of the movie.** Idempotency, and so the stop-and-rerun
  recovery and the retry of an unknown outcome, depends on it. A key that included a load timestamp or a counter would duplicate rows
  on every rerun.
- **`MovieByKeyword::order` pads the id to `ID_WIDTH` digits and joins with a byte below every
  printable character.** Changing either reorders every keyword partition that is already on disk,
  so it is a migration, not an edit.
- **`shoalctl::cli::run` is the one place a command is dispatched.** `shoalctl::cli::main` parses
  and calls it, and the loader parses and calls it. A command added to shoalctl reaches the loader
  with no change to the loader.
- **`color_eyre::install` runs once per process.** The terminal UI installs it itself, so the
  loader installs it only on its `load` path.

## Performance

Not a benchmark, the same as the example it replaces: the rows-per-second it prints has no
warmup, no repetition and no baseline behind it. For scale only, a local run of one node
(two cores, `--addr`) against the first 20,000 movies wrote 170,051 rows in 1.3 s and read 2,000
back in 11 ms. Measuring Shoal is `shoal-bench`'s job.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_load_names_one_target_and_a_pipeline_that_flows` | `examples/tmdb_dataset/src/load.rs` | A load with no target, or with both `-i` and `--addr`, is accepted; a stalling `--in-flight` or a missing dataset is found only after connecting |
| `a_csv_row_becomes_a_movie_and_its_keyword_rows` | `examples/tmdb_dataset/src/load.rs` | A dataset row stops parsing into `Movie`, a bad id is not skipped, a movie stops fanning out one keyword row per keyword, or `order` stops sorting by title and then numeric id |
| `a_transient_failure_is_retried_after_a_capped_backoff` | `examples/tmdb_dataset/src/load.rs` | A retriable code stops the load, a definite refusal is retried, the backoff stops doubling or passes its cap, or the defaults go back to no retry or a 4,096 gate |

It was also run end to end on the development host. A `tmdb-dataset-node serve` was started
against a scratch `shoal.yml` under `target/`, and `load --addr` wrote the first 20,000 movies and
read 2,000 back. The same load run a second time succeeded with the same counts, and a missing
`--dataset` was refused before connecting. `cluster status -i tmdb_cluster.yaml` accepted the
edited inventory and stopped at "has not been deployed". ~~The loader has not been run against a
deployed cluster from this change.~~

Against the three-node lab cluster (`tmdb_cluster.yaml`: europa, titan, hyperion, six cores each,
a factor of three), the loader at the old defaults stopped six seconds in on `OutcomeUnknown`
([Resolved #128](../appendix/resolved/hop-deadline-margin.md),
[item 129](../appendix/known-issues.md#129-an-overloaded-group-answers-outcomeunknown-rather-than-shedding)).
With the retry, the 1,024 gate and the drain, the full file (1,188,548 movies) wrote 2,193,788
rows in 53.2s and read 10,073 back in 60ms, with no retries. The run before the drain was added
wrote everything and hung in verify (item 130).

## Related

- [F51](cluster-deployment.md), `node::main` and `shoalctl cluster`.
- [F53](inventory-wizard.md), the wizard the inventory is built with.
- [Resolved #127](../appendix/resolved/wizard-loopback-address.md), found deploying this: the
  wizard saved a node whose name resolved only to loopback, and a source file as the server.
- [Resolved #126](../appendix/resolved/storage-directory-unusable.md), whose `--storage` and
  `--local-tracing` flags went with the example.
