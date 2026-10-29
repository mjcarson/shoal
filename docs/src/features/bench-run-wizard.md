# F67. `shoaladm bench run` names workloads, chooses a run in a wizard, and proves every stat moves

## Context

[F66](dataset-benchmarks.md) shipped `shoaladm bench`, and the first time its user ran it two
things went wrong.

**The stats view's ops/s by kind chart never drew a line**, though the bench tab showed thirteen
thousand inserts a second. The tmdb cluster was running a node build from before
[F65](query-figures-home-tab.md), which has no per-kind figures. The bench recorded an empty
server series and never said so ([item 199](../appendix/resolved/bench-unfigured-members.md)).
Tracing that turned up a second gap: a run by `--addr` never read the node's figures at all
([item 200](../appendix/resolved/bench-addr-reads-no-figures.md)). No test anywhere checked that
a real node's figures reach a metric of the view, so neither gap could have been caught.

**`--mixes` was confusing.**
- The word named the axis without saying what was on it.
- The valid values were listed only in `--help`.
- Getting a run right meant knowing several other things too: which events need a spare, which
  are refused on an attached cluster, how bundle sizes multiply the arm count, and when a read
  needs `--preloaded`. The only way to find out was to be refused.

This feature renames the axis, lets a run be chosen in a form that explains every choice, and
holds every metric of the stats view to a real node.

## What it does

### Workloads, not mixes

An arm's share of reads and inserts is a **workload**. Everything that said mix now says
workload:
- the flag is `--workloads`, and ~~`--mixes`~~ still parses as a hidden alias;
- `shoal_loadgen::spec::Mix` is `Workload`;
- the spec's `mixes` is `workloads`, and an arm's `mix` is `workload`.

The values are unchanged: `read100`, `insert100`, `rw50`, `read90`, `read:N,insert:M`. `--help`
says what each one is.

What was saved before still reads:
- A spec or capture written before F67 reads unchanged, because `mixes` and `mix` are serde
  aliases.
- Arm ids keep their spelling (`rw50/b16/none`), so captures from before compare with captures
  from after.

A spec's digest is a hash of its JSON, so the same spec digests differently now. Comparing a
capture from before with one from after needs `--allow spec`.

### The wizard

`shoaladm bench run` that names no workload opens a full screen wizard **on a terminal**: stdin
and stdout are both terminals, and `--basic` was not given. "Names no workload" means no
`--workloads`, and no `workloads:` or `mixes:` key in its `--spec` file. Otherwise the four
defaults run, and the run says so on stderr and as its log's first line.

The wizard opens on the spec the flags left, so a flag that was given shows as set and can be
changed. It has six pages:

| Page | What it holds |
| --- | --- |
| Workloads | The four named workloads to tick, custom `read:N,insert:M` rows (`+` adds one, ctrl-d deletes it), with any kind the schema supplies beside them since [F69](driver-operation-kinds.md) and one it does not refused on its row, and, attached, `--yes-write` |
| Bundles | 1, 4, 16, 64 and 256 to tick, and any other sizes |
| Events | Every event to tick, and, attached, `--yes-events` |
| Timing | Measured and warmup seconds, runs, workers, in flight |
| Reads | Preload, `--preloaded` (attached), distribution, keys per get, read level, warm or cold, dedupe, what an arm does when its inserts run out, table weights |
| Review | The arm count and its factors, the least time the arms take, every refusal, the save path, and every arm in the order the run takes them |

Beside each page's rows is **what the focused row means**: what a workload sends and what it
needs, what an event does to the cluster and where it is allowed, and what a bundle size
measures. The sidebar carries every page's error count, the arm count and the least time.

**Every refusal the run would make** is shown on the page that fixes it. A refusal comes from
`BenchRunArgs::problems`, the same check the run makes, and is filed by `page_of`. Enter on the
review, or ctrl-r anywhere, starts the run once nothing is refused. Ctrl-s writes the run as a
spec file (`bench.yml` in the project by default, editable on the review), and
`--spec <file>` runs it again without the wizard. The file is written through a `.partial` and
moved into place, and the wizard asks before replacing a file that is already there.

### The nodes' own figures, and the members that have none

Every cluster a run drives now gets a reader of its figures, through `Cluster::poller`:
- a deployment is read through any member, as the admin;
- an `--addr` node is read through the address, the way the driver reaches it.

The screen and the per-arm sampler both use that reader.

- **A member that answers with no query figures** is on a build from before F65. The run warns
  about it once, by name. The capture records it as `RunResult.unfigured`, and `bench show` and
  `bench compare` print it.
- **Figures that never answer** are recorded as `RunResult.figures_unread`, with the reason. A
  standalone node keeps none.

### Every metric says whether it should move

Every metric of the stats view's catalog now carries `under_load: Expect`:
- `Moves`: a few seconds of `rw50` on a cluster of one node serving `bench_dataset`'s `Catalog`
  must move it off zero;
- `Quiet(reason)`: it may stay at zero, and why.

Of the fifty metrics, 33 must move. Seventeen are let off:
- errors, updates, deletes and misses, which the workload never causes;
- both stream rates, which only run between members;
- archived bytes and archived partitions, at the node and the cluster, which wait on a flush or a
  compaction;
- `chained`, `lru`, `volatile`, `compacting`, `apply_lag` and `pending`.

**A test holds the catalog to a real node.**
- `bench-dataset`'s `stats::every_metric_that_should_move_does` starts a cluster of one node in
  process and drives it with a bench run.
- Beside the run, it reads `Stats` every half second, the way the view reads it.
- Every metric that should move must read above zero at least once and have a line in the
  view's history: for a per-kind metric, that means `get` and `insert`. The failure names every
  metric that read zero.

Its engine-free twin, `every_metric_reaches_the_view`, feeds the view figures with every field
set. It requires every metric to read above zero, have a line per member or per kind in the
history, and draw on its tab without waiting.

## Design choices

- **The wizard edits the spec, not the command line.** It opens on `BenchRunArgs::spec()` and
  hands back a `BenchSpec`, so the run carries on exactly as it would have from flags. Only the
  two attached-cluster permissions are flags rather than spec fields. The wizard edits those as
  rows and hands back the flags with them.
- **Refusals come from the run, not from the wizard.** The wizard's own checks are limited to
  values that do not parse. Everything else is `BenchRunArgs::problems`, so the wizard cannot
  allow a run that the run refuses, or the other way round. `page_of` only decides where a
  refusal is shown.
- **The help text lives in one table.** `workload_help`, `event_help` and `BUNDLE_HELP` are what
  the panel shows, and a test requires every named workload and event to have its own words.
- **"Should move" is judged against one stated setup**, not as a property of the metric. A
  scrub, a flush or a second member would move more of them. What is held to the test is what one
  short run on one node must move, so the test does not fail intermittently.
- **The largest value over the run is judged, not one sample.** Rates are zero until the node's
  second stats tick, and gauges such as `pending` are momentary.

## Alternatives rejected

- **Keeping `--mixes` and documenting it better.** The name was the confusion. An alias keeps
  every old command line working, so the rename costs nothing but the spec digest.
- **A wizard for workloads alone.** The workload list was not the only thing a run was refused
  for. A spare, `--yes-events`, `--preloaded`, and how bundle sizes multiply the arm count were
  each learned by being refused, so the wizard covers the whole run.
- **Opening the wizard whenever stdout is a terminal**, as the stats view does. A run with stdin
  piped from a script, or with `--basic`, gets the defaults, because a form that nobody can
  answer would hang it.
- **Refusing a cluster on a build from before F65.** The driver's figures are whole, and only the
  nodes' series is missing. Warning about it and recording it keeps the measurement.
- **Comparing each member's build with the tool's.** That is the general check, but `NodeStats`
  carries no build identity. Filed in [todos](../appendix/todos.md#a-nodes-build-on-stats).
- **Asserting every metric moves.** Seventeen metrics wait on things one short run never does,
  and a test that demanded them would fail by design. They are let off by name, with their
  reasons.

## Limitations

- ~~**The defaults read before they insert**, so against an attached cluster (`--attach`, `--addr`)
  they are refused unless the cluster already holds the preload (`--preloaded`).~~ That rule was
  wrong: the run loads the preload before its first arm whatever the order, and an attached run
  now needs only `--yes-write` or `--preloaded` ([item 201](../appendix/resolved/attached-read-order.md)).
- **The wizard does not edit overrides, `--event-at`, `--restart-at`, `--victim`, `--spare` or
  `--event-table`.** It shows what they make the run refuse; they are set by flag or spec file.
- **The estimate is a floor**: warmup plus measured time for every arm run. It does not include
  the preload, the reset after a disturbing arm on the bench's own cluster, or the read back of
  acknowledged inserts.
- **`--addr` has no credentials**, so a node that requires authentication is never read
  ([todos](../appendix/todos.md#credentials-for-a-bench-run-by---addr)).
- **The per-metric test is one node.** Figures that only a cluster of several members moves, such
  as streams and moves between members, are proven by [F52](cluster-stats.md)'s two stats tests
  in `cluster_fixture`, not by this one.
- **A member is named as unfigured only while it has no query figures at all.** A later figure
  that an older build leaves out has no test of its own.

## Invariants to uphold

- **Every metric in `METRICS` has an `under_load`.** A metric added as `Moves` is held to a real
  node at once, and one added as `Quiet` needs a reason someone can check.
- **A refusal is never made by the wizard and not by the run, or the other way round.** The
  wizard's errors are parse failures and `BenchRunArgs::problems`, nothing else.
- **No terminal, no wizard.** `bench run` with stdin or stdout not a terminal, or with `--basic`,
  never opens a form.
- **Arm ids never change spelling.** The rename touched field names, never the names a capture is
  compared by.
- **An empty `server_series` always says why**: `unfigured`, `figures_unread`, or no sample
  taken.

## Performance

Nothing here is on a node's query path. A sampler polls one `Stats` read every two seconds per
arm. It now runs for `--addr` too, and wakes as soon as its arm ends, so `bench_run`'s whole run
takes the same 59 s it took before.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `examples/bench_dataset/tests/stats.rs::every_metric_that_should_move_does` | A metric marked `Moves` reads zero throughout a real run on a real node, or has no line in the view's history |
| `examples/bench_dataset/tests/stats.rs::a_run_by_addr_records_the_nodes_answers` | An `--addr` run's capture has no server series, or names a current build as unfigured |
| `examples/bench_dataset/tests/bench_run.rs::a_run_naming_no_workload_runs_the_defaults` | A run with no terminal and no workload opens nothing, runs the four defaults, and says so in its log |
| `shoaladm` `cluster::stats::view::tests::every_metric_reaches_the_view` | A metric's reader, its history line or its tab loses a figure the node sent |
| `shoaladm` `bench::wizard::form::tests` (8) | Prefill and build back, custom workloads and bundle sizes, attached writes refused until allowed, the arm count and least time, a saved spec read back by `--spec`, every workload and event explained and every refusal filed on its page, leaving asked about |
| `shoaladm/tests/bench_wizard.rs` (3) | The focused workload's explanation drawn, the review naming every arm and refusal, every page drawn at 200×50 and 80×24 |
| `shoaladm` `bench::args::tests::workloads_are_chosen_by_flag_or_spec` | A spec file naming `workloads` or `mixes` counts as chosen, and one naming neither does not |
| `shoal-loadgen` `spec::tests::a_spec_that_says_mixes_still_reads`, `results::tests::an_arm_that_says_mix_still_reads` | A spec or capture written before F67 no longer loads |
| `shoaladm` `bench::orchestrate::tests::a_sample_names_the_members_without_figures`, `a_warning_is_given_once` | Item 199: a member on a build from before F65 is folded in silently |
| `shoaladm` `bench::wizard::form::tests::an_attached_run_that_reads_first_starts_once_writes_are_allowed` | Item 201: the wizard's defaults against an attached cluster cannot start with writes allowed |

## Related

- [F66](dataset-benchmarks.md), the bench this changes.
- [F64](stats-tui.md) and [F65](query-figures-home-tab.md): the view and the figures every
  metric is now held to.
- [F53](inventory-wizard.md), the inventory wizard whose form, view and driver split this
  wizard follows.
- [Item 199](../appendix/resolved/bench-unfigured-members.md) and
  [item 200](../appendix/resolved/bench-addr-reads-no-figures.md), filed and fixed with it, and
  [item 201](../appendix/resolved/attached-read-order.md), which its wizard found.
