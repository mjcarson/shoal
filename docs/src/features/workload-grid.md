# F17. The workload grid

## Context

[F8](purpose-built-workloads.md) replaced one blended workload with a set that each isolate one
path, and it was right to. A single insert-and-get run over a foreign dataset meant a change to the
read path and a change to the write path moved the same figure, so neither could be attributed —
and the dataset was not in the repository, so no clean checkout could reproduce the number at all.

That argument is about **attribution**, and attribution is not the only thing a benchmark is for.
Nothing in this repository could answer the first question anybody evaluating a store asks: *how
will this handle my workload?* There was no read/write mixture anywhere — reads and writes were
separate workloads by construction. The row-width axis existed only inside the encryption sweep
(four widths) and the transport pair (two), and neither swept width against anything else. A reader
who wanted to know what a 50/50 workload over 8 KiB rows cost could not find out, because it had
never been measured.

The `todos.md` entry that anticipated this was the row-width lesson under *What F8 left undone*: it
specified a mode axis and missed the axis that decided whether the set could do its job. This is
that axis, plus the two the isolating workloads structurally could not have.

## What it does

Seventy-four workloads under `macro/grid/` and `macro/skew/`, in four sweeps.

| Sweep | Identifier | Arms | What varies |
|---|---|---|---|
| Row width | `macro/grid/<table>/r50/<width>` | 44 | eleven widths × four tables |
| Read share | `macro/grid/<table>/r<share>/1024` | 20 | five further shares × four tables |
| Key distribution | `macro/skew/<dist>/<table>` | 6 | three distributions × two tables |
| Load depth | `macro/grid/depth/<depth>` | 4 | four depths |

[F20](configuration-sweeps.md) added a fifth user of this driver. `Grid` gained a
`conf: ConfOverrides` field and a `Sweep::Conf` naming variant, and forty-eight arms under
`macro/conf/` are the reference cell `macro/grid/unsorted/r50/1024` with exactly one field of the
server configuration moved. They are minted by `conf_sweep.rs` rather than by `Grid::all`, so the
counts in this table are unchanged — but a change to the driver now moves both families, and the
reference cell is a control for both.

**Row widths**: 64 B, 128 B, 512 B, 1 KiB, 8 KiB, 512 KiB, 1 MiB, 4 MiB, plus three named
*distributions* of widths — `mixed_small` (four widths under a kilobyte), `mixed_mid` (1 KiB to
8 KiB) and `mixed_large` (512 KiB to 1 MiB).

**Read shares**: `r0`, `r30`, `r50`, `r70`, `r95`, `r100`, as percentages of queries that are reads.

**Tables**: all four Shoal has — persistent and ephemeral, sorted and unsorted.

Every arm reports **both** halves of its mixture separately: a `read` distribution and a `write`
distribution, never a pooled one, plus `reads` and `writes` counters that are counts of queries
rather than of rows. The second is what
[`ops_per_sec`](../performance/all-workloads.md) is built from, and it exists because `retrieved`
counts rows — one fan-out query answers with two hundred and fifty six of them and one keyed get
with one, so a rate built from rows is not comparable across workloads.

## Design choices

**The cross, not the cube.** Four tables × six shares × eleven widths is 264 arms and an overnight
capture. Each axis is swept fully against a fixed reference of the others instead, so the two large
sweeps share their four `r50/1024` cells and the total is 74. What this cannot see is an
*interaction* — a cost appearing only at a wide row under a write-heavy mixture would be missed by
both sweeps. That is recorded in [todos](../appendix/todos.md) rather than papered over.

**YCSB's specification, natively; not YCSB's harness.** `r50` is YCSB workload **A**, `r95` is **B**,
`r100` is **C**; the reference row is YCSB's 1 KiB record; `zipfian` is YCSB's
`ScrambledZipfianGenerator` and `latest` its `SkewedLatestGenerator`, with YCSB's `theta = 0.99`
and YCSB's FNV-1a scramble, constant for constant. A number here is therefore readable *next to* a
published YCSB figure. Two deviations are deliberate and are the reason this page says so rather
than leaving them to be discovered:

- **The record is one payload field, not ten hundred-byte fields.** `schema.rs` keeps the row narrow
  on purpose so that width is a knob rather than an accident, and a ten-field row would put field
  count into every width measurement.
- **A write is an insert into a key range no read touches**, not an update in place. Reads draw from
  the seeded range `[0, R)`; writes insert into `[R, …)`. This keeps every read a hit, keeps the hit
  rate from drifting as an arm runs, and keeps the payload on the write path — an `#[shoal(update)]`
  write carries only `label`, so under one the width axis would not reach the write path at all.

**Everything is a function of the query's index.** Which key a query asks for, how wide its row is,
and whether it is a read or a write are all derived from the index through
[`Seeded::at`](../performance/benchmarking.md), never drawn from a generator walked forward. A
mixture is driven by several slots pulling from one shared cursor, so anything drawn in sequence
would depend on which slot reached the cursor first — two runs of one arm would send different
queries, and the arm would stop being repeatable in exactly the way the grid exists to be.

**The depth does not move with the row width.** Every arm runs at 32 outstanding queries, at 64
bytes and at 4 MiB alike. Scaling it down as rows widen is the confound `encryption.rs` objects to
in the transport pair: the two axes would move together and neither could be plotted against the
other. Thirty-two is the deepest value that is safe at a 4 MiB row and deep enough to keep twelve
shards busy at 64 bytes.

**Payloads are built before the run, not inside it.** Generating a 4 MiB string costs milliseconds,
and a string generated inside the driver loop would land in the arm's wall clock and therefore in
its throughput. Eight distinct payloads per width are built up front and cloned per query. The clone
is still inside the wall clock and outside the samples, which is where the query building already
sat.

**The uniform skew arm and the depth-32 rung duplicate the reference cell on purpose.** Both are
the same measurement under a different identifier, and both have a test asserting their plans are
identical to the cell's. That turns the duplication into a control: a capture where the two disagree
has something moving that neither of them names.

## Alternatives rejected

**Running the real YCSB client against Shoal.** It would need a JDBC or Java binding, a JVM in the
toolchain, and a fetched dataset — reintroducing the exact property [F8](purpose-built-workloads.md)
removed, that the macro layer could be *read* by anyone and *reproduced* by one machine. The Java
client is also routinely the bottleneck against a fast store, so a low number would be ambiguous
between Shoal and the harness. Borrowing the specification costs none of that and gives up only the
claim that the same code produced both numbers — which is a claim worth having and not worth a JVM.

**The `db-benchmarks` harness.** Docker-orchestrated, over real datasets (`hn`, `taxi`), and aimed
at search and analytics engines where the interesting axis is query complexity. Against a
partition-keyed store it would measure mostly its own query planning, and it reintroduces the
fetched dataset.

**A workload merely shaped like YCSB, named after it.** This is the failure
[F8](purpose-built-workloads.md) records for `tmdb` and the retired baseline records for its own
numbers: *a file that parses and is not comparable costs a wrong answer, where one that fails to
parse costs only a comparison.* The generators here are YCSB's own algorithms with YCSB's own
constants, or the arm does not claim the name.

**Updating rows in place, as YCSB does.** Two problems. An `#[shoal(update)]` query carries only the
updatable field, so the width axis would stop at the read path; and an insert over an existing
partition key has semantics that vary by table kind, so the four tables would stop being comparable
on the one axis the page exists to compare them on. Disjoint ranges cost a growing table, bounded by
the byte budget, and buy a constant read hit rate.

**Sweeping both distributions across the whole grid.** Doubles it to ~130 arms. Uniform is the
honest default — it defeats every cache in the system — so the grid runs uniform and a six-arm sweep
quantifies the skew effect once, at the reference cell.

**A per-batch (saturating) variant of every cell, for a true throughput number.** Doubles the grid
again for a figure the depth ladder gives more usefully: a saturating run reports the maximum and
nothing about the shape of the approach to it, and the shape is what says whether a latency on the
rest of the site is a service time.

## Limitations

- **A grid arm's throughput is its throughput at a depth of 32**, not the most the server can do.
  Only the depth ladder says how far from saturated that is, and only for one cell.
- **[O31](../appendix/optimizations.md) applies and is unresolved.** Nothing in the artifact says
  which side of the knee an arm is on, and the disjointness rule detects *reliably* different rather
  than *meaningfully* different. A cell past its knee reports a p50 that is a queue length.
- **The widest arms have the fewest samples.** The byte budget means the 4 MiB arm runs 200 queries
  where the 64 B arm runs 20,000, so **read the p50 at the wide end** — a p99 over 200 samples is
  roughly its third-worst observation.
- **The wide arms seed a small key space** — 64 partitions at 4 MiB — so every read there is answered
  from an entirely resident table. Seeding more would measure eviction rather than the wire.
- **No interaction between axes is measured**, per the cross.
- **A mixture's `row_bytes` is a mean**, not a measurement. `row_profile` beside it says so, and
  every byte-rate figure derived from it is a mean rate.
- **Nothing here compares Shoal to another database.** The identifiers and `ScaleFacts` are shaped so
  a foreign system's numbers could be described in the same schema; nothing that would consume them
  is built.
- **The skew sweep measures a table that fits in memory**, so its gap is locality inside a resident
  table rather than a hit rate against disk. A table larger than memory would show a far larger gap
  and is unmeasured.

## Invariants to uphold

**Workload identifiers are join keys.** Renaming one orphans every capture taken before the rename.
Add and deprecate; never rename. The declared order in `workloads::all()` and `workload_ids::IDS`
also decides each workload's TCP port, so the grid is **appended** and nothing before it may move.

**Everything a query does must be a function of its index.** Key, width, and read-or-write. A draw
taken in sequence makes the arm depend on slot scheduling, and it will still pass every test —
it just stops being the same benchmark twice.

**The two ends of the mixture are exact, not probabilistic.** `r100` issues no writes and `r0` no
reads. One write in twenty thousand inside an `r100` arm puts a write's service time in the read
distribution, and at 4 MiB that single sample *is* the p99.

**Reads and writes are never pooled into one distribution.** A write costs several times a read, so
a pooled p99 is the write p99 wearing a different name whenever writes are more than a percent of
the traffic.

**Reads must only ask for keys inside the seeded range, and writes only outside it.** The moment
they overlap, the read hit rate drifts as the arm runs and the arm measures a mixture of hits and
misses that changes with its own progress.

**The new `ScaleFacts` fields stay optional and skipped when absent.** Four of them were added here.
If one stops being skipped, every one of the eighty-seven workloads that is not a mixture
re-serializes with new nulls and the whole committed corpus churns on a change that measured
nothing.

**A regression is never attributed to a grid arm.** It says a mixture got slower; the isolating
workloads say which half.

## Performance

The grid adds roughly two hours to a full capture, taking it from two-to-three hours to four-to-five.
It is the largest phase, and `shoal-bench run --layer macro macro/grid macro/skew` runs it alone
while `--scale smoke` runs it at a hundredth of the data in under a minute.

Its own numbers are on [Read/write mixtures](../performance/grid.md),
[Row size](../performance/row-size.md), [Table types](../performance/table-types.md) and
[Access patterns](../performance/access-patterns.md).

## Tests

| Test | What breaks if this is reverted |
|---|---|
| `grid::tests::every_arm_is_minted_exactly_once` | two arms claim one artifact key and one port, and the second silently wins |
| `grid::tests::an_id_names_the_axes_of_its_sweep` | an axis missing from an identifier is an axis two measurements are joined across |
| `grid::tests::the_sweeps_share_their_reference_cells` | the reference cell is measured twice, at twice the cost, under one name |
| `grid::tests::the_uniform_skew_arm_matches_the_reference_cell` | the skew sweep's control stops being a control |
| `grid::tests::the_ladder_passes_through_the_grids_own_depth` | the ladder can no longer place the grid on its curve |
| `grid::tests::two_cells_of_one_sweep_differ_in_one_axis` | a width sweep also sweeps the query count, warmup and key space |
| `grid::tests::every_arm_records_its_own_axes` | a reader has to parse identifiers back into facts |
| `grid::tests::a_read_share_is_the_share_that_is_issued` | an `r70` arm is a different measurement than its name says |
| `grid::tests::the_ends_of_the_mixture_are_absolute` | a write's service time lands in an `r100` arm's read p99 |
| `grid::tests::every_arm_is_bounded_at_both_ends` | an arm runs too few queries for a median or too many to finish |
| `grid::tests::a_seed_bundle_fits_in_a_frame_at_every_width` | the wide arms cannot run at all — a bundle over the frame bound is refused outright |
| `rows::tests::a_width_is_addressed_by_index` | the seed phase writes a 512 byte row and the read phase expects a 64 byte one |
| `rows::tests::the_declared_mean_is_the_observed_mean` | every byte budget and byte rate over a mixture is mis-sized |
| `keys::tests::every_draw_lands_in_the_key_space` | a Zipfian tail draw asks for a row that was never seeded, which reads as a miss |
| `keys::tests::zipfian_concentrates_where_uniform_does_not` | the skew sweep measures two uniform distributions and reports no effect |
| `keys::tests::a_chooser_is_reproducible_and_stream_scoped` | two runs of one arm ask for different keys |
| `committed_artifacts::a_workload_that_is_not_a_mixture_serializes_the_keys_it_always_did` | the whole committed corpus churns on the next fold |
| `committed_artifacts::a_capture_that_counted_no_queries_reports_no_query_rate` | every pre-F17 capture appears on a throughput chart at the origin |
| `workload_ids::the_declared_ids_are_the_registered_ones` | an arm is registered, never run, and never missed |

## Related

- [F8, purpose-built workloads](purpose-built-workloads.md) — the isolating set this sits beside,
  and the argument it does not repeal
- [F9, ephemeral tables](ephemeral-tables.md) — the control-pair shape, and the tables two of the
  grid's four columns drive
- [F18, the results pages](results-pages.md) — where these numbers are drawn and explained
- [O31](../appendix/optimizations.md) — the saturation trap the depth ladder makes visible and does
  not close
- [Benchmarking](../performance/benchmarking.md) — how to take a capture
