# F66. `shoaladm bench`: benchmark any schema against a dataset folder

## Context

`shoal-bench` measures Shoal with one schema it owns, `Bench`, four tables of synthetic rows, on
a server it starts itself ([F8](purpose-built-workloads.md), [F17](workload-grid.md)). That is
the right instrument for asking whether a change to the engine moved anything. It is the wrong
one for an operator who wants to know what *their* workload costs on *their* cluster:

- the rows are not theirs;
- the cluster is not the one they deployed;
- and every workload in it is written against the `Bench` row types.

The last point is structural. `#[shoal::db]` mints its own set of types per schema, so harness
code written for one schema cannot be pointed at another. Before this, each database that wanted
a benchmark wrote one by hand. The TMDB dataset's loader carries a `bench`
([F54](tmdb-dataset-deployment.md)) that names `MovieGet` and `MovieByKeyword` and can be
nothing else.

This feature makes the benchmark generic. A schema opts its tables in, an operator points
`shoaladm bench run` at a folder with one file per table, and the same code that deploys their
cluster ([F51](cluster-deployment.md), [F63](shoaladm.md)) brings up a copy of it, loads the
rows, runs a matrix of read and insert ~~mixes~~ workloads ([F67](bench-run-wizard.md)) at several bundle sizes, and keeps a capture that
can be compared with the next one. It is meant to become how Shoal is benchmarked;
`shoal-bench` stays until it is retired ([todos](../appendix/todos.md#retiring-shoal-bench)).

## What it does

### A table opts in, and writes no benchmark code

```rust
#[derive(Debug, Archive, Serialize, Deserialize, serde::Deserialize, Clone, ShoalUnsortedTable, DeepSizeOf)]
#[rkyv(derive(Debug))]
#[shoal_table(db = "Catalog", dataset)]
pub struct Item {
    #[shoal(partition)]
    pub id: u64,
    pub name: String,
}
```

`dataset` in `#[shoal_table]`, plus `serde::Deserialize` on the row, is all a schema adds. The
table derive then emits two impls, both generic over the database's query kinds, so it never
names the database's types:

- **`DatasetRow`**: parse a row, insert it, take its read key (the partition key, plus the sort
  key for a sorted table) and build one get of any number of those keys.
- **`DatasetTable`**: hand a visitor the row type.

`#[shoal::db]` emits `DatasetSupport` on every client: the tables with whether each opted in,
and a dispatch from a table's name to its row type. A table that did not opt in still gets a
`DatasetTable`, one that refuses by name, so a schema without serde on its rows compiles as it
did. The attribute never reaches the schema fingerprint, so a benchmark built with it talks to a
node built without it.

### A dataset folder

A folder holds one `<Table>.csv`, `<Table>.json` or `<Table>.jsonl` per table, named by the
table's exact name as `QuerySupport::table_names` reports it: the row struct's name, `Movie`,
not `movies`.

- **Formats.** A csv has a header row naming the fields. A json file is one array of rows, read
  an element at a time and never held whole. A jsonl file is one row per line.
- **Ignored files.** Dotfiles, `*.md`, `README*` and a `bench.yml` kept with the data.
- **Refusals.** The folder is judged whole before any host is touched, and every problem is
  refused together, by name: a file naming no table (with the table it probably meant when it
  differs only in case), a table that did not opt in, a table with two files, and an empty
  folder.

Each file is scanned once, typed, through the row type its table handed back. The scan:

- counts and digests the file;
- skips a row that does not parse in the same place every time, up to `--max-parse-errors`
  (one percent);
- keeps the read key of every row.

The file then splits in file order:

- **The preload**, by default the first half (`--preload 50%` or a row count). It is inserted
  before anything is measured, and its distinct keys are the only keys a read asks for, so
  every read is of a row that exists.
- **The insert pool**, the rest. A reader thread streams it from the file again for each arm
  that inserts, so a dataset larger than memory is never held.

A row whose key an earlier row had is counted. It is inserted as an overwrite, or skipped with
`--dedupe`.

### Arms

An **arm** is one workload at one bundle size, under one set of inventory overrides and one
event, run `--runs` times (three by default). It is named
`{workload}/b{bundle}[/{override}]/{event}`, for example `rw50/b16/none`. That name is what two
captures are compared on, so it is never renamed - [F67](bench-run-wizard.md)'s rename of the
axis left every id as it was.

**Workloads** (`--workloads`) are `insert100`, `read100`, `rw50`, `read90`, or
`read:N,insert:M`. ~~**Mixes** (`--mixes`)~~ was the name until [F67](bench-run-wizard.md): the
flag still parses, and a spec's `mixes` or a capture's `mix` still reads. The defaults are the
four named workloads at bundles 1, 16 and 64. A run that names no workload opens a wizard on a
terminal that explains each one and chooses the whole run ([F67](bench-run-wizard.md)); with no
terminal it runs the defaults and says so.

**Each worker** (eight by default, spread over the members) does the following:

- opens an unordered stream;
- fills a bundle with the queries its picker chooses;
- sends the bundle once it holds `bundle` queries;
- keeps `in_flight` queries outstanding (four bundles by default).

Every choice - read or insert, which table, which keys - is a function of the seed, the arm, the
run, the worker and the operation's index, so two runs of an arm send the same reads in the same
order. Reads follow `--distribution`: uniform, YCSB's scrambled zipfian, or latest, the same
generators `shoal-bench` uses, copied with their tests. `--read-keys N` makes each read one get
of N keys, which is a different path on the server from N gets of one key bundled together.

An arm that runs out of inserts before its time ends there and says when (`--on-exhaust end`),
or starts its pool over, with every later insert recorded as an overwrite (`wrap`).

**Steady arms run first**, then event arms. A run starts each group one arm later than the run
before it, so drift over a long run is spread across the arms rather than always falling on the
last.

### The bench's own cluster

By default the bench never drives the inventory's cluster. It writes a copy,
`inventory.bench.yml`, into the capture's directory:

- named `<name>-bench`, so its unit, its local state and its authority are its own;
- under `<remote_dir>-bench`, with every port moved by `--port-offset` (100);
- with **every storage root the inventory names, at the deployment, a group or a node, moved
  to `<root>-bench`**, or all of them to one `--bench-storage` directory.

A copy that overlaps the inventory's roots, directory, ports or name in either direction is
refused.

**Before bootstrapping**, the bench checks every host:

- A port it needs that is already bound is refused.
- Another running `shoal-*` unit is refused, unless it is named by `--stop-unit` (stopped for
  the run and started again after it) or `--allow-neighbours` is given.
- `--governor performance` sets every node's governor and puts back the one each had.

**Wiping.** Before anything is wiped, every root is read: it is wiped only if it is empty,
missing, or its marker names the bench's own recorded cluster or node.

**Resets.** After an arm that inserted, ran an event, or before one under another override, the
cluster is wiped, bootstrapped again with its authority and admin password kept, and preloaded
again. `--reads cold` restarts every node after the preload, so reads start from storage.

**Putting the hosts back.** Every change to a host is recorded as it is made, and undone on
every way out: success, failure, Ctrl-C, an abort from the screen, and from `Drop` if a panic
skipped the rest. The cluster is torn down first, then drop-ins and governors are restored,
then stopped units are started and checked active. `--keep-cluster` leaves the cluster up.

**`--attach`** drives the inventory's own cluster as it is:

- No wipe, no bootstrap, no governor, and no event that takes a node down.
- Inserts need `--yes-write`; a repair or backup needs `--yes-events`.
- ~~A read needs `--preloaded` (checked by reading a sample of the preload back) or an insert arm
  before it.~~ The run loads the preload itself before its first arm, which is a write, so a run
  that is not `--preloaded` (checked by reading a sample of the preload back) needs `--yes-write`,
  whatever order its workloads are in ([item 201](../appendix/resolved/attached-read-order.md)).

**`--addr host:port`** drives one node started by hand, which is what the tests do.

### Events

An event runs beside an arm's workers on the arm's own clock, leaving marks:

- **kill** sends SIGKILL to a node's unit behind a runtime drop-in with `Restart=no`. Without the
  drop-in, the unit's own `Restart=on-failure` would end the outage after five seconds. At
  `--restart-at` the drop-in is removed and the unit started.
- **stop** is a clean stop and start.
- **remove** is a kill followed by a `remove` of the node.
- **rebalance** and **decommission** move onto `--spare`, a node of the inventory left out of the
  copy's bootstrap set and joined before the arm.
- **repair** (verify) and **backup** act on `--event-table`.

Each admin operation is the cluster tab's own line, followed through the record it writes. The
arm keeps running until it is done, up to `--event-timeout` past its time.

**Windows.** Afterwards the arm's seconds are cut into before, during and after:

- for a fault, at the first second the client saw a failure and the first of two clean seconds;
- for a background operation, at its requested and done marks.

A rebalance or decommission records `p99_ratio_permille`, the during p99 over the before p99.
After a restart, the cluster's largest replication lag is sampled each second until two samples
in a row are zero.

### The screen

On a terminal, the run draws the `shoaladm stats` view ([F64](stats-tui.md),
[F65](query-figures-home-tab.md)) with the benchmark in it:

- **A strip on the home tab**: the arm and its phase, client ops/s, client p50 and p99 per query
  and per bundle, errors and warnings, and the newest mark.
- **A bench tab** (0 or b): every arm with what it measured, the current arm's seconds charted
  with its marks as rules, and the log.

**The nodes' own figures** are read for every cluster the run drives, through any deployed
member or, by `--addr`, the node itself ([item 200](../appendix/resolved/bench-addr-reads-no-figures.md)).
A member that answers with no query figures runs a build from before
[F65](query-figures-home-tab.md): the run says so once, by name, and the capture records it
([item 199](../appendix/resolved/bench-unfigured-members.md)).

**Two latencies, labelled apart.** The driver's is timed from each query's send. A node's own
p99 is timed from when its bundle's frame arrived, so a large bundle's queries have already
waited in it before that clock starts. The two are never drawn on one axis.

**Stopping.** `q` asks before it stops a running benchmark, and the view stays up while the
cluster is torn down and the hosts are put back. `--basic`, or any pipe, prints a line a second
instead, and Ctrl-C stops the run the same way.

### `--profile`

`--profile` deploys a node built for profiling, from the project even over a `server:`
inventory:

- **A wrapper crate of its own** (`target/shoal-build/<package>-profile`), built into target
  directories of their own, so a plain build never changes or rebuilds.
- **The allocator** is jemalloc with heap profiling, exported as `_rjem_malloc_conf`: a sample
  every 512 KiB, a dump every 2 GiB and one at exit, into the node's directory.
- **Frame pointers and line tables** are built in.
- **Installed as `<package>-<Db>-node-<cpu>-profile`**, so a later plain deploy never picks it
  up.

The dumps are brought back into the capture's `prof/<node>/` before each reset and before the
teardown, and the programs are copied beside them, so `target/lab/r15/prof/heap.py` reads them as
it read round 15's. A capture's flavor is part of what compare checks.

### Captures

A capture is a directory, by default `target/shoaladm-bench/<label>/` in the project, holding:

- `bench.json`;
- the spec as it ran (`spec.yml`) and the copied inventory;
- `log.txt`;
- with `--profile`, `prof/`.

The label defaults to the time and the project's commit. A tree with uncommitted changes, in the
project or in a shoal checked out beside it, is refused unless `--allow-dirty`, and recorded as
dirty when allowed.

`bench.json` holds:

- **Provenance**: commits, rustc and flags, the driver's machine, every node's cpu, cores,
  memory, governor (as found and as run) and kernel, each node's program digest, whether the
  driver shares a host with a node, the inventory's digest and shape, the dataset's digest, the
  spec's digest, and the schema's fingerprint.
- **Every run of every arm**:
  - the measured window and the warmup;
  - a window a second;
  - each feed's facts;
  - the read back of every acknowledged insert, where a miss is a lost write;
  - the event's marks, windows and catch up;
  - the leader's own figures every two seconds;
  - the driver's busiest second.

**`shoaladm bench compare <baseline> <candidate>` refuses** two captures that differ in the
dataset, the spec, the inventory's shape, any node's machine or governor, the driver's machine,
the flavor, the mode or the schema. It lists every difference. `--allow <fact>` waives one by
name, and the waiver is printed with the result.

**What compare reports.** A metric differs only when the two captures' intervals across their
runs do not overlap, the rule `shoal-bench` used, and the effect is the gap between the nearest
ends. A run that wrapped its inserts is never compared with one that did not.

`list` shows every capture with whether the project has moved past its commit. `show` prints one.

## Design choices

**The per-table code is generated, and everything else is generic.** The client must be
compiled against the exact schema (the handshake compares fingerprints), so the driver has to
be code over `S: QuerySupport`. It runs inside the schema's admin program, which `shoaladm`
already builds and hands every connecting command to ([F63](shoaladm.md)). What generic code
cannot do - name a row type - is the visitor's job. `DatasetSupport::visit_table` calls back
with the type, and `shoal-loadgen`'s `TableSource` erases it again, so a worker holds one
trait object per table and builds the database's query kinds without knowing any row.

**The impls belong to the macro that knows their inputs.** The table derive knows the key
fields and the attribute. The db macro knows the table names. Each emits its half, and the
row's impls are generic over `K: From<Row> + From<RowGet>`, so the row derive needs nothing
from the database.

**Reads ask only for preloaded keys.** Every read is then a read of a row that is there, so
"read 90" means reads that found something and a miss is a failure, the property `shoal-bench`'s
`expects_rows` guarded. Inserts come from rows the cluster does not hold yet, so an insert arm
measures inserts and not overwrites unless the pool wraps, which is recorded.

**An own cluster by default.** An arm that inserts changes what the next one reads, and an arm
run twice on the same cluster overwrites its own rows. A wipe between arms is the only way two
runs of an arm measure the same thing, and the inventory's cluster is never one to wipe.

**The run on a thread of its own.** A deployment is not `Sync`, and bootstrapping blocks on
ssh. Neither may stall the screen, so the run has its own OS thread and runtime, and talks to
the screen over a bounded tokio channel it never blocks on. A screen that falls behind loses a
second's numbers, never the run's time.

**Per query and per bundle.** With bundling, "latency" means two things: when a query's answer
came, and when the bundle's last did. Both are recorded, in hdrhistograms a second, so any
window is the sum of its seconds.

## Alternatives rejected

**SHQL as the generic path.** It is SELECT only, parsed on the client, and cannot name a
composite key. It would have given generic reads and no inserts.

**`FromStr` on `TableNames`, and a match on it in the driver.** That maps a name to a variant,
not to a type, and a variant cannot be inserted.

**Emitting the impls from the db macro.** It does not see a row's key fields or whether the row
opted in.

**trybuild for the serde requirement.** It is not in the lockfile. A `compile_fail` doctest on
`shoal::dataset` holds it, beside its passing twin.

**Reusing the inventory with `--wipe`.** The tmdb inventory names its storage at the group
level. A copy that only renamed the cluster would have kept `/optane/shoal-tmdb`, and its first
bootstrap would have wiped the inventory's data. Every root is moved, and the move is checked.

**`systemctl kill` alone for a fault.** The unit restarts itself five seconds later, which makes
every outage five seconds long.

**The run on the screen's runtime.** A bootstrap would freeze the screen for minutes, and the
deployment cannot be held across a spawn.

## Limitations

- **Closed loop only.** A worker sends more as answers come back, so the load is set by the
  depth, not offered at a rate. Coordinated omission applies: a stall delays the queries behind
  it rather than piling them up. An open-loop generator is in the [todos](../appendix/todos.md).
- **Reads and inserts only.** No update or delete workloads, and no partition scan of a sorted
  table. A sorted read names exact keys.
- **A sorted get of several keys** names each partition once and every sort key. It also returns
  a row whose sort key matches in another of the named partitions, so it can return more rows
  than keys.
- **A table with a partition key of two or more fields does not compile**, with or without
  `dataset`. That is an older defect of the partition key derive, filed as
  [known issue 198](../appendix/known-issues.md#198-a-composite-partition-key-does-not-compile).
- **Windows are cut at second resolution**, where `shoal-bench` cut at each operation's time.
- **Catch up is the cluster's largest lag**, not the returning node's alone, sampled from when
  the node reports up again.
- **An arm counts every failure where it happened.** It retries nothing unless `--retries` says
  so; a preload and a read back always retry the codes the client itself retries on, up to eight
  times, since they have to load or check every row.
- **A reset is a whole bootstrap**: minutes per arm on the lab. The defaults (four workloads, three
  bundles, three runs) are thirty-six arms, most of them after a reset and a preload, so
  `--dry-run` prints the plan and its time before anything is touched. A reset restored from a
  backup is in the todos.
- **One parsing thread per table.** Workers that wait on it are counted (`feed_wait_ms`) and
  warned about.
- **The driver may share a host with a node**, as it does on europa. The capture records that,
  with the driver's cpu each second, and compare refuses a capture whose driver differs.
- **`--profile` is heap profiles and frame pointers.** `perf record`, the stage breakdown
  ([F6](stage-breakdown.md)), hotpath and OTel export are in the todos.
- **No results page or explorer index yet.** `show` and `compare` print text, and `shoal-bench`'s
  render and [explorer](benchmark-explorer.md) read only its own corpus.

## Invariants to uphold

- **`dataset` never reaches the fingerprint or the schema id.** `opting_in_to_datasets_moves_no_fingerprint`
  holds it. If it moved, `--attach` would be refused by every cluster deployed before the schema
  opted in.
- **Generated code stays client-half only.** `DatasetSupport` is emitted in both halves and names
  only `::shoal::shared` paths. `cargo check -p shoal-client-check --no-default-features` is the
  check.
- **Nothing in `shoal-loadgen` or `shoaladm/src/bench` names a schema.** The two example crates'
  tests are the proof: neither has a line of benchmark code.
- **Every root, port and the directory of the bench's copy are disjoint from the inventory's.**
  `check_disjoint` is called on every copy. The wipe guard reads every root before any wipe and
  refuses one whose marker names another cluster.
- **Every change to a host goes through `Restore` before it is made**, so a run that dies
  halfway leaves nothing behind it did not record.
- **Reads ask only for preloaded keys, and the preload is loaded before any arm reads.** A read
  miss is otherwise a property of the dataset rather than of the cluster.
- **An arm's id is never renamed.** It is the key compare joins on.
- **The screen's new `select!` arms wait on tokio receivers**, which are cancel safe. A kanal
  receive raced there would lose a value ([Resolved #152](../appendix/resolved/kanal-receive-races.md)).

## Performance

The driver adds nothing to a node's query path; what it costs is its own process, recorded each
second as `driver_cpu_pct`. The runs below proved the feature on the lab on 2026-10-01, with the
driver on europa (Ryzen 9 7945HX, 32 cpus, `powersave`) and nodes on europa, titan and hyperion
(titan and hyperion: Ryzen Embedded V1756B, Zen1, 8 threads, `schedutil` unless set). **They are
not an A/B and measure nothing about a change**; they say what the tool shows, on what. The
driver was a debug build of `shoaladm`.

**Read only, against the user's `tmdb` cluster** (`--attach --preloaded --preload 10000`,
`read100`, eight workers, two runs of 10 s). Every read found its row, and the preloaded-sample
check passed first:

| Arm | Reads/s (two runs) | p50 | p99 | Driver cpu |
| --- | --- | --- | --- | --- |
| `read100/b1/none` | 164,310 – 165,100 | 0.16 ms | 0.42 ms | ~380% |
| `read100/b16/none` | 273,572 – 273,608 | 2.15 ms | 4.47 ms | ~590% |

At sixteen a bundle the driver used six of europa's cores, which is what it was measuring as much
as the cluster: the reason the capture records the driver's cpu each second.

**On the bench's own cluster beside `tmdb`** (`f66-bench`: 2 cores and 2 GiB a node,
`--allow-neighbours`, preload 20,000 rows, two runs): eight arm runs, each after a guarded wipe,
a bootstrap and a preload of 4–5 s, every acknowledged insert read back with none lost, and the
cluster, its roots, its unit and its state gone afterwards with `tmdb`'s marker unchanged.
`rw50` at sixteen a bundle ran 24,455 reads/s and 24,610 inserts/s, insert p99 107.9 ms against
read p99 7.4 ms; a bundle of mixed kinds is as slow as its slowest, so its per bundle p99 was
76.4 ms.

**Events** (`f66e-bench` at a factor of two, `read90` at sixteen a bundle, 30 s):

- `kill` of titan at 13.4 s, started again at 23.2 s: before 108k reads/s and 12k inserts/s;
  during 83k reads/s and 1.4k inserts/s with 108,515 failures, mostly `NotLeader` until a new
  leader was elected, inside [F62](failover-window.md)'s 7.5–10 s window; recovered (two clean
  seconds) at 27 s; 237,215 acknowledged inserts read back, none lost.
- `rebalance` onto hyperion, a spare joined before the arm: requested at 11.9 s, done at 103.2 s
  having moved 4 sets and 262 MB; the arm ran on to its done mark. Read p99 went 5.98 → 15.65 ms
  during it (2.6×) while insert p99 barely moved (112.5 → 122.1 ms); 964,065 inserts read back,
  none lost. This run found that the ratio of the worse kind is the one to report, and that the
  measured span must run to where the event left the clock: both fixed before the next run.

**`--profile` with `--stop-unit shoal-tmdb` and `--governor performance`**: `tmdb` stopped on all
three hosts for the run and active again after; every governor set and put back (europa
`powersave`, titan and hyperion `schedutil`); heap dumps of every node, each process's final dump
included, brought back with the two programs (`znver1`, `znver4`) beside them.

The first event run also found that a preload just after a bootstrap answers `OutcomeUnknown` for
a few seconds while groups settle; the preload and the read back retry since.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `shoal/tests/dataset_rows.rs` | Opted-in sorted and unsorted tables build the inserts and gets a caller would; a table that did not opt in, and a name no table has, are refused by name |
| `shoal/tests/fingerprint.rs::opting_in_to_datasets_moves_no_fingerprint` | The attribute stays out of the fingerprint and the schema id |
| `shoal::dataset` doctests | A row that opts in without `serde::Deserialize` does not compile; its twin with it does |
| `shoal-loadgen` unit tests (51) | Folder judging, the three readers (a json array streamed), the split, dedupe and wrap, the read back of acknowledged inserts, the spec and its matrix, the picker's determinism and shares, windows, fault and background cuts, the capture's round trip and format check, and compare's refusals and verdicts |
| `shoaladm` bench tests | The tmdb-shaped copy moves every root and port and is disjoint, an overlapping copy is refused, the wipe guard, unit and governor scripts, the flags laid over a spec, attached-run refusals, provenance and labels, the bench pane and its keys, the strip and tab drawn on a `TestBackend`, and the profile wrapper's manifest, source and install name |
| `examples/bench_dataset/tests/smoke.rs` | The driver over the committed dataset against an in-process node: preload, three workloads at two bundle sizes, no failure, no miss, nothing lost |
| `examples/bench_dataset/tests/bench_run.rs` | `a_run_against_one_node_writes_a_capture_that_compares`: `shoaladm bench run --addr` whole, two captures of twelve arm runs, then compare accepting them and refusing a third of another spec. `an_aborted_run_stops_and_keeps_what_it_measured`: an abort five seconds into a minute's arm stops it, records it as aborted, and keeps what it measured |
| `examples/bench_dataset/tests/profile_build.rs` (ignored) | A real profile build of the catalog's node, with the jemalloc settings in the program |
| `tmdb-dataset-loader` `every_command_has_one_name` | The loader's own commands and the admin commands it flattens have one name each; `bench` collided until the loader's became `drive` |

## Related

- [F8](purpose-built-workloads.md), [F17](workload-grid.md), [F21](benchmark-groups.md): what
  `shoal-bench` measures, and what this does not yet replace.
- [F51](cluster-deployment.md), [F63](shoaladm.md): the deployment the bench's own cluster is.
- [F64](stats-tui.md), [F65](query-figures-home-tab.md): the view the benchmark is drawn in, and
  the node figures beside it.
- [F54](tmdb-dataset-deployment.md): the TMDB dataset, whose `Movie` opts in; its loader's driver
  is `drive` now.
- [Distributed cluster testing](../cluster-testing/overview.md): the lab's own drivers, which know
  the TMDB rows by name.
