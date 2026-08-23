# F22. The benchmarks the row size page asked for

## Context

[Row size and what it costs](../tables/row-size.md) explains why an even read/write mixture gets
slower as rows widen. It names four mechanisms — the payload walked about six times per round trip,
the intent log giving up on batching past its 4096 byte staging buffer, a wide response blocking
narrow ones behind it in the relay, and plain arithmetic — and it could **isolate none of them**.
Every claim on it about *where* the curve bends was an argument from the source that the capture
happened to be consistent with.

That was not a gap in the code. It was a gap in the evidence, and it had five named shapes:

- the width axis went 8 KiB → 512 KiB with **nothing in between**, so a step at the staging buffer
  and a slope that starts near it were indistinguishable
- every grid arm ran at **32 outstanding queries** at every width, so at 4 MiB — 128 MiB in flight
  against a key space of sixty four partitions — the numbers were as much queue depth as service
  time
- the axis was swept at **`r50` only**, so a write path effect was being asked half a question
- `latency_buffer` was swept at the grid's reference cell of **1 KiB**, which is the one width where
  the setting cannot bite, and reported a confident 1.06× and a `yes` in the *Real?* column
- `wire_codec` swept bundle size and response cardinality over a fixed thirty byte row, so it
  **never varied payload width at all**, and the stage layer ran one workload, so nothing said which
  of nineteen stages grows with bytes

The page's own closing section listed the six benchmarks that would settle it, cheapest first, and
[TODOs](../appendix/todos.md#the-row-size-axis) filed the same six. This is all six.

## What it does

**165 macro arms, 50 micro benchmarks and a stage layer that profiles three row widths.** The macro
layer goes from 209 workloads to 374 and a full capture from about seventy five minutes to about two
hours.

| | What it answers | Cost |
| --- | --- | ---: |
| A width axis on `wire_codec` | The per-byte half of [O1](../appendix/optimizations.md) and [O2](../appendix/optimizations.md), with a confidence interval | 50 micro ids |
| `latency_buffer` above the buffer | [O34](../appendix/optimizations.md) outright: is the setting a step, and is the step at the buffer | 10 arms |
| The 64× hole filled | Where the knee is, as a measurement rather than an inference | 20 arms |
| A depth-1 arm at each width | How much of a wide arm's latency was queue rather than service | 15 arms |
| The width axis at `r0` and `r100` | Which half of the mixture the per-byte cost is on | 120 arms |
| The stage breakdown at three widths | **Which** of the nineteen stages grows with the bytes | 3 instrumented runs |

**On the micro layer**, `shoal/benches/wire.rs` gained a `WideRow` table and four
`wire_codec/width/*` groups sweeping five widths from 64 bytes to 64 KiB with the bundle size and
the response cardinality held fixed. The header decode is swept alongside as a **control**: eight
bytes is eight bytes at every width, so a rising line there would mean the axis was measuring
something other than the payload.

**On the macro layer**, three passes at the end of `Grid::all`: `INFILL_WIDTHS` at the reference
mixture on all four tables; the whole width axis at `r0` and `r100` on all four tables; and
`macro/grid/depth/1/<width>` on the persistent unsorted table. `conf_sweep` gained `WIDE_REPEATS`,
which reruns a knob's rungs at a width that is not the reference one — today `latency_buffer` at
8 KiB and 64 KiB.

**On the results pages**, `row-size.md` gained three sections — the axis at each end of the mixture,
the axis at one query outstanding, and the per-stage breakdown against width — and `micro.md` gained
a second scaling chart whose x axis is bytes rather than rows.

## Design choices

**Every new arm is appended, never spliced.** `INFILL_WIDTHS` is a second array rather than five
entries inserted into `WIDTHS` between `8 * 1024` and `512 * 1024`, because a workload's position in
`workload_ids::IDS` decides the TCP port a capture gives it and inserting there would move every
grid arm after it. The split is bookkeeping and says so in its own doc comment; `every_width()` is
what everything that sweeps the axis *as an axis* uses, and a test asserts the two arrays do not
overlap.

**The identifiers of existing arms did not move.** An identifier is the join key of every
comparison, so the 48 configuration arms keep the shape `macro/conf/<section>/<knob>/r<mix>/<value>`
and only a repeat away from the reference width carries a `w<width>` segment. Same on the micro
layer: widening `TitleByKeyword` would have kept all thirty nine `wire_codec` names and changed what
every one of them measured, which is worse than adding fifty, so `WideRow` is a second row type.

**Selection reads facts, never identifiers.** `Arm::read_pct`, `row_bytes`, `table_kind` and `depth`
come from `ScaleFacts`, which is why 140 of the 165 new arms needed **no renderer change at all** to
be selected correctly — the infill widths simply joined the existing charts, and the `r0`/`r100`
arms were already excluded from them by a filter written long before they existed. This is
[F18](results-pages.md)'s design paying off rather than anything new.

**The two ladders cross rather than duplicate.** `macro/grid/depth/1` is a rung of the depth ladder
at the reference width *and* the reference width's rung of the new ladder. It is minted once, and
`arms::width_depth` selects on the fact (`is_depth() && depth() == 1`) so it appears on both curves,
while `arms::depth_ladder` excludes the `macro/grid/depth/1/` prefix so the depth chart does not
acquire fifteen points at a depth of one. Two curves with no point in common cannot be read against
each other.

**A knob at two widths is two sweeps, not one longer sweep.** `arms::conf_sweeps` keys on the row
width as well as the knob and the read share. Merging them would put a 1 KiB arm and an 8 KiB arm in
one ladder and let the difference between two *widths* be read as the difference between two
*values of the setting*, which is the one thing the whole configuration sweep is built to prevent.

**The stage layer got its own list.** `PROFILED_WORKLOADS` drove both instrumented layers, so
pointing the stage layer at three widths would have tripled the hotpath phase too — three profiles
that mostly repeat each other, which is the cost `Workload::profiles` exists to avoid. There are two
lists now, `stage_profiles()` defaults to `profiles()` so nothing that opted in before had to
change, and a test asserts the runner's copies match what the workloads say. That test did not
exist; two doc comments claimed it did.

**A section that cannot answer says so.** The three new sections on `row-size.md` require an actual
curve — two or more widths per series — before they draw anything. Without that check the mixture
section would have drawn the existing `r0`/`r100` arms at 1 KiB as five single dots and read as
though the axis had been swept at six mixtures. Twenty committed captures predate these arms and
every one of them now renders a sentence saying which question it cannot answer.

## Alternatives rejected

**Adding a `payload` field to `TitleByKeyword`.** One line, no new table, and it silently redefines
all thirty nine existing `wire_codec` measurements — which are then compared against
`B1-performance.json`, a frozen baseline, under names that no longer mean what they meant. A
benchmark that changes what it measures while keeping its name is worse than no benchmark.

Adding a *table* is not automatically safe either, and that was the part worth checking rather than
assuming: `WireDbQueryKinds` and `WireDbResponseKinds` are enums whose archived size is the largest
of their variants, so a second table can move the layout of the first one's frames. It does not
here — a ten-insert bundle archives to 812 bytes and a 256 row response to 7,724, both unchanged —
and `check_original_archive_sizes` now asserts exactly that at the top of `bench_header`, because a
criterion target has no test harness to put it in and the check has to live somewhere that runs.

**Splicing the infill widths into `WIDTHS`.** Reads better and re-ports every grid arm after 8 KiB.
Ports are not a join key, so this was a choice about convention rather than correctness; the
convention is worth keeping, and the cost of keeping it is one extra array with a comment.

**A full cube.** Sixteen widths × six mixtures × four tables is 384 arms for the width and mixture
axes alone, plus the depth ladder crossed with both. The cross was chosen for the same reason
[F17](workload-grid.md) chose it, and what it still cannot see is recorded in
[TODOs](../appendix/todos.md) rather than papered over.

**A `row-size` benchmark group.** Considered and dropped. The width sweep now *is* most of the grid,
and the identifiers of a width cell and a mixture cell are the same shape — so a `row-size` group
would have selected about 95% of what `--group grid` selects and earned nothing.
[F21](benchmark-groups.md) is about naming coherent sets, not about having a name per feature. Two
arms were added to `quick` instead, so a smoke run touches the two new arm shapes.

**Walking scratch to find the stage reports.** The first draft of the collector, and wrong for a
reason worth writing down: `target/shoal-bench/scratch` is created and never cleared, so it holds
every run of every capture ever taken in the tree — over a thousand files here. A glob would have
folded a previous capture's stage reports into this one's artifact under the same workload keys, and
produced something indistinguishable from a correct capture. The collector is handed the paths the
plan named instead, and a named file that no run wrote is an error.

**One stage artifact per workload.** The natural fix for the collision it exposed, and it would make
the stage layer the only layer whose artifact is not a file — `--check`, the freshness table and the
provenance all assume one artifact per layer per capture. A scratch file per run plus a `Collect`
step is what every other layer already does. See
[Resolved #73](../appendix/resolved/stage-artifact-overwrite.md#alternatives-rejected).

**Bumping `STAGE_REPORT_VERSION`.** The *report's* shape did not change; the artifact's did. Bumping
the report version would have refused nine committed captures over a change that touches no field
any of them holds. There are two version constants now, and they move independently.

## Limitations

- ~~**Nothing here has been captured.**~~ **Captured as `f22-row-size`**, and five of the six
  questions came back with an answer — see
  [What it settled](../tables/row-size.md#what-it-settled--five-of-six-ran). Two of the five
  contradicted what the page they were built for expected: the `latency_buffer` gain is in records
  per buffer rather than at the buffer threshold, and the 45× tail the response relay was blamed for
  is load depth.
- **The sixth benchmark did not run at all, and reported that it had.** All three stage reports
  carry `joined: 0` and no ops; the client half of a stage record is written only by `drive_with`,
  and a grid arm's measured phase uses `drive_mixed_per_query`, which has none of that wiring
  ([item 76](../appendix/known-issues.md#76-the-stage-layer-joins-nothing-for-any-grid-arm-and-reports-it-as-a-layer-that-ran)).
  This is the failure this feature was least guarded against: `STAGED_WORKLOADS` has a test
  asserting it matches what the workloads say, and nothing asserts that a workload on that list can
  produce a joined record. **The pattern is worth taking from this**: every check written here was
  about the *selection* being right, and the thing that broke was the *collection*.
- **Every existing grid capture stops describing the current code**, correctly: `grid.rs` moved, so
  the source fingerprint of every grid workload moved with it. That is the freshness table working,
  not breakage, and it is why the todo page said to batch these three items rather than take them
  one at a time.
- **Still a cross, not a cube.** `r0` and `r100` are swept at a depth of 32; the depth-1 ladder is
  swept at `r50`. A cost that appears only at one query outstanding under a pure write mixture is
  invisible to both.
- **The depth-1 ladder is one table**, so the persistent/ephemeral subtraction that makes a width
  effect attributable to storage ([F9](ephemeral-tables.md)) does not exist for the depth axis.
- **The `wire_codec` width axis stops at 64 KiB**, two doublings below the grid's widest arm minus
  six. Every criterion sample builds a response, and 64 KiB across sixteen rows is already a
  megabyte per sample.
- **The stage breakdown is drawn at the `all` rank only.** Which stage grows with bytes is asked of
  the mean of every query; whether the *tail* is made of a different stage than the median is not.
- **48 configuration arms moved onto different ports**, because the grid grew in front of them in
  `workload_ids::IDS`. A port is derived from that list and is never a join key, so what this costs
  is the port and nothing else — but it is the one place this change did not hold the convention it
  otherwise kept.
- **The hotpath layer still shares one artifact between its workloads.**
  [Item 73](../appendix/known-issues.md) — latent, since its list holds one workload.

## Invariants to uphold

- **`INFILL_WIDTHS` and the three new mint passes stay at the end of `Grid::all`.** Their position is
  the whole reason they are separate from the sweeps they extend.
- **A repeat away from the reference width carries `w<width>` in its identifier; one at the
  reference width does not.** The asymmetry is what keeps the original 48 configuration ids intact.
- **`arms::conf_sweeps` keys on the row width.** Dropping it from the key silently merges two
  sweeps whose difference is the width into one ladder that reads as a difference between values.
- **`arms::width_depth` selects on the fact and `arms::depth_ladder` on the prefix.** Swapping
  either loses the crossing point or floods the depth chart.
- **No two identifiers may flatten to one file name.** `slug` replaces `/` with `-`, and names
  both a workload's scratch results and its storage directory with the result.
  `macro/grid/depth/1/512` is one character from colliding with `macro/grid/depth/128`; the set is
  injective today and was never constructed to be.
- **`STAGED_ARMS` names arms that `Grid::all` actually mints**, and a test walks it. It is a list of
  identifier strings, so a width renamed would leave the stage layer quietly profiling two widths
  and calling it three.
- **A new section on a generated page must decline when the capture cannot answer it.** Twenty
  committed captures predate these arms; a section that drew whatever it found would have invented
  a curve out of five unrelated points.
- **The original `wire_codec` archives keep their pinned sizes.** A third table on `WireDb` that
  moves either number invalidates every `wire_codec` measurement ever captured, under names that do
  not change. `check_original_archive_sizes` is what turns that into a loud failure at the top of a
  run rather than a silent one nobody notices.
- **`micro_scaling::ScalingAxis::column` renders the row axis exactly as it always did.** It is a
  table heading in a committed page, so making it prettier fails `render --check` on captures
  nothing touched.

## Performance

None claimed and none measured. This feature adds measurement and changes no code any query
executes — the only paths it touches are the benchmark harness, the renderer and a criterion bench
target. What it makes newly *adjudicable* is the point:

| Entry | What can now decide it |
| --- | --- |
| [O1](../appendix/optimizations.md), [O2](../appendix/optimizations.md) | `wire_codec/width/request/decode/{access,deserialize}/*` and `wire_codec/width/response/encode/*`, with a confidence interval |
| [O34](../appendix/optimizations.md) | `macro/conf/storage/latency_buffer/r50/w8192/*` against `.../r50/*` — the same five rungs either side of the buffer |
| [O11](../appendix/optimizations.md), [O29](../appendix/optimizations.md) | The `r0` width sweep against the `r100` one, and the per-stage breakdown at three widths |
| [O35](../appendix/optimizations.md) | The depth-1 ladder: a p99 that collapses at depth 1 and not at 32 is a queue in front of the relay |

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `workloads::grid::tests::every_arm_is_minted_exactly_once` | The grid is 229 arms and no identifier is minted twice |
| `workloads::grid::tests::the_width_arrays_do_not_overlap` | A width declared in both arrays, minted twice at the reference mixture |
| `workloads::grid::tests::the_width_axis_is_swept_at_every_declared_mixture` | A mixture or a table missing a width, which is a pair that cannot be subtracted |
| `workloads::grid::tests::the_two_ladders_cross_at_one_arm` | The depth and width ladders share `macro/grid/depth/1`, and the width ladder never mints it again |
| `workloads::grid::tests::the_staged_arms_are_the_three_declared_ones` | `STAGED_ARMS` names three arms that exist |
| `workloads::conf_sweep::tests::a_width_repeat_names_a_real_sweep_at_a_new_width` | A repeat names a declared knob, and never the width it already runs at |
| `workloads::conf_sweep::tests::a_repeat_runs_every_rung_its_sweep_does` | A repeat is the same ladder at another width, not a shorter one |
| `workload_ids::tests::the_declared_ids_are_the_registered_ones` | All 374 ids, in mint order |
| `workloads::tests::the_runners_copy_of_the_profiled_workloads_is_current` | Both instrumented lists match what the workloads say |
| `run::plan::tests::each_staged_workload_writes_to_its_own_artifact` | Three stage runs get three artifacts |
| `collect::stages::tests::the_reports_fold_into_one_artifact` | Three reports become one artifact keyed by workload |
| `collect::stages::tests::a_report_the_run_never_wrote_is_an_error` | The collector is given its inputs rather than walking scratch, which is never cleared |
| `run::plan::tests::no_two_workloads_share_a_slug` | No two of the 374 identifiers flatten to one file name, which would mean two workloads sharing a scratch file and a storage directory |
| `render::family::tests::every_workload_has_a_family` | `macro/grid/depth/1/*` resolves to `width-depth` and not to `depth` |
| `render::family::tests::every_family_says_all_four_things` | The new family's four mandatory blocks |
| `groups::tests::the_quick_group_names_only_real_benchmarks` | The two arms added to `quick` exist |
| `committed_artifacts::every_committed_page_is_current` | `render --check` — the three new sections and the second micro chart |
| `model::stages::tests::a_single_report_reads_as_a_one_workload_artifact` | The version 1 fallback — nine committed captures depend on it |
| `model::stages::tests::the_primary_report_is_the_write_path_one` | Which report the attribution page draws, deterministically |
| `pages::configuration::tests::a_knob_the_order_does_not_name_is_appended` | Every sweep in a capture reaches the recommendation table — [item 74](../appendix/resolved/conf-knob-dropped.md) |
| `wire::check_original_archive_sizes` | The second table on `WireDb` did not move the layout of the frames the original 39 ids measure. An assertion inside `bench_header`, since a criterion target has no test harness; it runs under `cargo test --bench wire` and under `cargo bench` |

## Related

- [Row size and what it costs](../tables/row-size.md) — the page that asked for all six
- [Row size](../performance/row-size.md) — where the answers will appear
- [F17](workload-grid.md) — the grid these arms extend, and the cross that left the gaps
- [F20](configuration-sweeps.md) — the configuration sweep, and why its `latency_buffer` row was
  measured at the one width whose answer is no
- [F6](stage-breakdown.md) — the nineteen stages
- [F21](benchmark-groups.md) — why this added no group
- [Resolved #73](../appendix/resolved/stage-artifact-overwrite.md),
  [#74](../appendix/resolved/conf-knob-dropped.md) — the two defects found on the way, both a doc
  comment describing a guarantee nothing enforced
- [Optimizations](../appendix/optimizations.md) — O1, O2, O11, O29, O34, O35
