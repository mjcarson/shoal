# F30. Only comparable axes share a chart

## Context

[F29](benchmark-explorer.md) made the axes a control. A reader picks a metric, a key axis, a set of
captures and a set of workloads, and gets a chart. What it did not do is ask whether the workloads
belong on one chart, and the answer is usually no: the corpus holds three hundred and seventy-four
of them, driving different paths, at two scales, timed two ways, over rows whose width is sometimes
a measurement and sometimes a mean.

The explorer folded all of them into one line per capture. Its own opening view was the clearest
case — twenty-four grid arms across four tables, drawn as a single zigzag where
[the grid page](../performance/grid.md) draws four lines. Two refusals that would have caught the
rest were written into the doc comments on `ScaleFactsLite::scale` and `Timing` and never
implemented. That is [item 82](../appendix/resolved/one-line-per-capture.md), resolved by this
feature.

The two charts this was built against are the two a reader actually opens the explorer to redraw:

| Chart | Page | What it is |
|---|---|---|
| `chart-grid-throughput` | [grid.md](../performance/grid.md) | queries a second against the read share, one line per table, at 1 KiB rows |
| `chart-row-size-ops` | [row-size.md](../performance/row-size.md) | queries a second against the row width, one line per table, at an even mixture, both axes logarithmic |

## What it does

**Two workloads may share a chart when their axis units match, and for no other reason.** The units
of a measurement are:

| | What it is | What refines it |
|---|---|---|
| **Value axis** | `Metric::unit()` — a query rate, a row rate, a byte rate, a duration, a percentage | the named **scale**, because a rate over two thousand rows is not a smaller version of one over two hundred thousand; and, **for a percentile only**, `Timing`, because a per-batch p99 charges a query for the ones queued ahead of it and a per-query p99 is a service time |
| **Key axis** | the quantity the axis reads — a share, a width, a load depth, a row count, a key count, or the capture | whether the measurement is on that axis at all (a workload that is not a mixture has no share), and whether a width is **measured or a mean** |

That last one is the book's own argument, made in `row_size.rs`: a declared distribution of widths
reports a mean, and *plotting a mean beside a measurement invites the two to be read alike*. So a
`mixed` profile and a fixed 1 KiB row are different quantities on the width axis, which is why the
row width page draws the mixtures in a table rather than on the chart.

The rule is deliberately weaker than *they measure the same thing*. An insert loop and a read/write
mixture are both queries a second, and a reader comparing them is asking a real question. What is
refused is a comparison that cannot be read at all.

### Matching units is not one line

A legitimate selection is still several curves. A sweep now draws one series per **(capture, curve)**,
where the curve is the facts a caller *set* — width, share, load depth, clients, row profile, key
distribution, table kind and the server configuration — minus whichever one the key axis is reading.
Each curve is named by what separates it from the others being drawn and by nothing else, so four
tables at one width read `f24-routing · persistent_sorted` rather than repeating the width four
times. One curve on its own is still called after the capture that took it.

**`rows` and `keys` are not in the curve key.** A workload sizes them from the row width against a
byte budget, clamped at both ends by `MIN_ROWS` and `MAX_ROWS`, so they follow the axis rather than
naming a curve. Holding them would give the width sweep fifty-two curves of one point each.

**What the key cannot tell apart is split downstream of it, since
[F32](chart-line-identity.md).** Two workloads that set none of those facts differently answer to one
key and are still two workloads — the two keyed gets are one struct with a `Residency` field that
reaches no `ScaleFacts` — so a curve that would draw two of its arms at *one position on the axis* is
split into one line per workload, each named by its identifier. That is
[item 84](../appendix/resolved/identical-facts-one-line.md). It is deliberately not in the key: the
identifier is a fact that co-varies with the axis, and putting it there is the `rows` trap under
another name.

~~This also makes `theme.rs` true. It has always said *hue carries the series, dash pattern carries
the capture*; the plot passed the series' position to `theme::capture_style`, so with two captures
ticked the styles walked through the series list rather than standing for a capture. Hue is now the
curve and dash is the capture, which is what draws two captures of one curve as the same colour in
two styles — the comparison the explorer exists to make.~~

**Half of that was still not true, and [F32](chart-line-identity.md) is where it became so.** The
dash half is right and unchanged: it stands for the capture on a sweep, and two captures of one curve
are still one colour in two dash patterns. The hue half only ever meant *the curve's position among
the ones being drawn*, so the sorted table was blue on its own and orange with the unsorted one
ticked ahead of it. Hue is now the **table**, at its fixed slot in `index::TABLE_ORDER`, and a
**marker** carries which of that table's curves a line is — three channels rather than two.
`Series::group` is gone; `hue` and `mark` replace it.

### What the reader sees

A workload whose axes are not in the chart's units is ~~**disabled in the picker, not hidden**, and
hovering it says what differs~~ **hidden, along with a family left with nothing in it — see
[F31](metric-availability.md), which reversed this**. The reasons are unchanged and are still
sentences: *it was measured at the smoke scale, not the full one*, *its position
on this axis is a mean width over a declared distribution, not a fixed row width*, *it has no
position on this axis*. The family header counts them — ~~`(4/24, 6 in other units)`~~
**`(4/24, 6 hidden)`, with a line under the tree totalling them** — and
`select all shown` takes the ones it can and says how many it left. The first ticked workload sets
the units, and `clear selection` is how they are changed.

Changing the metric or the key axis re-reads the selection against the new units and drops what no
longer belongs, saying how many. A strip above the chart names what it is currently drawn in.

### Two presets, and a log key axis

The two charts above are buttons. Selecting the width sweep by hand is fifty-two checkboxes, which
nobody was going to do, so `preset.rs` declares each chart as a metric, an axis, two log flags and a
selector — and the selector picks arms **by the facts each measurement recorded and by the family
that explains it, never by parsing an identifier**, which is the rule `render::arms` exists to
enforce. The explorer opens on the first of them.

`log_x` is new and the width sweep needs it: sixteen widths from 64 B to 4 MiB is six decades, and
`chart-row-size-ops` is drawn logarithmic on both axes. It is offered only for a line drawn against
a swept fact — a bar slot and a capture position are positions in a list, not quantities — and
defaults off, because the read share runs from zero and a logarithmic axis cannot place it.

## Design choices

**Units, not families, and not "every fact but the swept one".** Both alternatives were considered
and are recorded below. Units is the rule that admits the comparisons a reader legitimately wants
and refuses the ones that cannot be read, and it is the only one of the three that is a property of
the *measurement* rather than of how the documentation happens to be organised.

**The refusal happens in the picker, not above the chart.** This is the first place in the explorer
that refuses rather than warns, and it is a deliberate departure from `Index::incomparable`, which
draws two incomparable captures and puts a strip over them. That is right for captures: the numbers
exist on one axis and somebody will go looking for them. It is wrong for units, because there is no
shared axis to draw badly on. So the explanation moves to where the choice is made.

~~The refused workload stays visible, greyed, with the reason on hover: hiding it would leave a
reader hunting for a workload they know is in the corpus.~~ **[F31](metric-availability.md) reversed
that half.** It added a second reason to refuse — a workload carrying no value for the current
metric at all — and against three hundred and seventy-four workloads the two together grey out most
of the tree. What replaces the per-row tooltip is arithmetic that always adds up: a count on every
family header, and a line under the tree with the total and both reasons. The refusal is still made
where the choice is made; what moved is that the explanation is now per group rather than per row. The
rule this page argued for is back where it fits, though: [F33](chart-readout.md) greys a **capture**
that carries no value for the current metric, with the reason on hover, because twenty-seven rows a
reader knows by name are not three hundred and seventy-four they do not.

**`Source::series` enforces the rule anyway.** The picker never lets a mismatched selection reach it,
so `Index::in_one_unit` is dead code in the application. It is there because `Source` is the
contract, and a second implementor — the live server view the crate is named for — would otherwise
be free to build points out of mismatched units at the one place a reader has no way to check them.

**The first ticked workload wins.** A selection that somehow holds two unit groups draws the group
the reader started with, not the largest one. Deterministic, and it matches what the picker does.

**A workload's units come from its most recent measurement.** They could have been read per capture,
which is more precise and would flicker as captures are ticked. The newest is stable, deterministic,
and is the measurement a reader is most likely looking at.

**`Unit::Rate` was split into `QueryRate` and `RowRate`.** It covered both, and `plot::unit_name`
printed it as "queries per second" — wrong for a rows-a-second chart. The function had no callers,
so nothing was visibly broken; this feature gives it its first one, so it had to be right first.

## Alternatives rejected

**Same family.** `render::family` is the repo's own encoding of *what this measures*, and grouping by
it would have caught the pairing units allow: a grid mixture beside an isolating read arm. Rejected
because that pairing is a question worth asking, and because family granularity does not line up
with axes — `row-size` and `width-depth` are two families and both are row width sweeps, while
`grid` is one family spanning sixteen widths, six mixtures and four tables.

**Every recorded fact except the swept one.** The strictest reading of "the same axis", and it is
what a sweep literally is. Rejected because it makes the grid chart illegal: its four table kinds
differ in a fact that is not the axis. The curve split gets the same effect without the refusal —
four lines rather than four charts.

**Refusing to draw rather than refusing to select.** Everything may be ticked; the chart draws the
first group and a strip says what it left out. Less modal, but the selection and the chart then stop
agreeing, and the reader has to read a caption to find out that six of the nine boxes they ticked
are not on the chart.

**Clearing the selection when an incompatible workload is ticked.** No dead controls, at the cost of
losing a fifty-two arm selection to one misclick.

## Limitations

**The configuration sweep has no useful sweep view.** Its fifty-eight arms are one grid cell with
one setting moved, so they sit at the same position on every sweep axis. The conf half of the curve
key separates them into fifty-eight curves of one point rather than one vertical stack, which is
honest and not useful. The timeline is the right view for them, and it works. Filed in
[todos](../appendix/todos.md).

**Log ticks land where `egui_plot` puts them.** The key axis is drawn in log space and the formatter
undoes the logarithm, so a tick can read `316 B`. The book's `plotters` axis labels decades. Nothing
is wrong with the numbers; the axis is just less tidy than the static chart it reproduces.

**A workload whose facts changed between captures is judged by the newer ones.** `workload_units`
reads the most recent measurement. In the committed corpus no workload does this, and if one did,
the older capture's arm would be offered under units it was not measured in.

**Still only the macro layer.** Micro, hotpath and stages have no units model because they have no
model in the index at all — that is F29's limitation and this does not touch it.

**The interface still has not been seen.** This machine has no display and no `wasm-bindgen`, so the
picker's disabled checkboxes, the tooltips on them and the log key axis have been compiled for both
targets and tested underneath, and not looked at. That is unchanged from F29 and is the reason the
two preset tests assert against the real corpus rather than against a screenshot.

## Invariants to uphold

**A curve key must not hold a fact that co-varies with the key axis.** `rows` and `keys` are sized
from the row width against a byte budget, so a key holding them turns a sweep into a scatter of
single points. Before adding a field to `Index::curve_key`, ask whether a workload *sets* it or
*derives* it. `row_count_does_not_split_a_width_sweep` is what notices. The workload identifier is
the same trap wearing a different coat, which is why
[F32](chart-line-identity.md)'s split lives in `Index::split_curves` and not here.

**Every curve key built in one call has the same length.** `curve_names` diffs them positionally.
`conf_key` writes the same ten entries whether or not a measurement recorded a configuration,
precisely so a capture that recorded none lines up against one that did.

**`AxisUnits::differs_from` returns the sentence, not a boolean.** There is one wording per reason
and nothing else composes one, so a workload cannot be ~~disabled~~ **hidden** without saying why.
Since [F31](metric-availability.md) the sentences feed a count and a summary line rather than a
tooltip, and `picker::refusal` composes two more of them — *never measured at all* and *measured,
without this number* — but the rule is the same one: one wording per reason, and no silent refusal.

**Selection order is tick order.** `Explorer::workloads` was already documented as being kept in
tick order so learned colours do not shuffle. The units rule now depends on it too: the first
element decides what the chart is in. Sorting that vector would silently change which group of a
mixed selection survives.

**A preset selects by recorded facts and by family, never by parsing an identifier.** The same rule
`render::arms` documents. A preset that matched on a name would break the first time a workload was
renamed — and renaming already orphans every capture, so it would break quietly and at the worst
moment.

**`preset` stays outside the `ui` feature.** It names `Metric`, `Axis` and `Index` and nothing
graphical, which is what lets `shoal-bench`'s corpus tests apply a preset while entering `shoal-top`
with `default-features = false`. Putting it behind `ui` would take the two tests that check the
explorer reproduces the book's charts with it.

## Performance

Nothing measured changed, so **no capture was taken**. No workload, no `shoal.yml`, no seed and
nothing under `shoal-bench/src/workloads/` was touched; CLAUDE.md excludes renderer and test-only
changes from needing one, and `render --check` passes against the committed pages, which is the
proof that none of them moved.

The work `Source::series` does is unchanged in order. It walks the selection once more to place each
workload on a curve — a linear scan over a vector of at most a few hundred short strings — and the
projection, the index size and the wasm bundle are all untouched. `INDEX_VERSION` does **not** move:
`Unit`, `KeyUnit`, `AxisUnits`, `Selection` and `Series` are not serialized, so nothing added here
crosses the wire to a stale browser bundle.

## Tests

| Test | What breaks without it |
|---|---|
| `index::tests::a_sweep_draws_one_curve_per_table_not_one_line_per_capture` | [Item 82](../appendix/resolved/one-line-per-capture.md) returning: four tables joined into one zigzag |
| `index::tests::a_smoke_arm_may_not_share_an_axis_with_a_full_one` | The refusal `ScaleFactsLite::scale` documented since F29 and never performed |
| `index::tests::a_per_batch_latency_may_not_share_an_axis_with_a_per_query_one` | The refusal `Timing` documented since F29 and never performed |
| `index::tests::timing_does_not_split_a_throughput_chart` | Over-refusal: a rate is a count over a wall clock and is the same quotient however the samples were stamped |
| `index::tests::a_mean_width_is_not_on_the_axis_a_fixed_width_is` | A declared distribution's mean plotted beside a measured width |
| `index::tests::an_arm_with_no_read_share_is_not_offered_on_a_read_share_sweep` | An arm that is not a mixture capturing the anchor and refusing every arm that is |
| `index::tests::row_count_does_not_split_a_width_sweep` | `rows` re-entering the curve key and shattering the width sweep into single points |
| `index::tests::the_first_ticked_workload_decides_the_units` | A non-deterministic choice of which group of a mixed selection survives |
| `index::tests::a_curve_is_named_by_what_separates_it_and_nothing_else` | A legend repeating the held facts on every curve and never naming the one that differs |
| `index::tests::two_captures_of_one_curve_share_a_hue_and_a_mark_and_differ_in_the_capture` (was `..._share_a_group_...` before [F32](chart-line-identity.md)) | Hue and dash swapping back, so a dash pattern stands for a position in a list |
| `explore_index::the_grid_preset_reproduces_chart_grid_throughput` | The opening chart drifting from `render/pages/grid.rs::throughput`, on the real corpus |
| `explore_index::the_row_width_preset_reproduces_chart_row_size_ops` | The same for `render/pages/row_size.rs::rows_per_sec` |

## Related

- [F29. An explorer that draws more than one capture](benchmark-explorer.md) — the tool this changes,
  and where the defect came from
- [Resolved 82. Every selected workload was folded into one line per capture](../appendix/resolved/one-line-per-capture.md)
  — the defect, and what the unfixed tree did
- [F18. Results pages that explain themselves](results-pages.md) — the families whose prose the
  explorer shows under a chart
- [F22. Row size benchmarks](row-size-benchmarks.md) — the width axis one of the two presets draws
- [F17. The workload grid](workload-grid.md) — the arms the other one draws
- [The grid page](../performance/grid.md) and [the row width page](../performance/row-size.md) — the
  two charts this exists to make redrawable
- [F31. A metric a workload can actually answer](metric-availability.md) — the question this gate
  was missing, and the feature that reversed *disabled rather than hidden*
