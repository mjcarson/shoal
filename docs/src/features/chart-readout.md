# F33. The chart answers where you point, and the capture list says whether it can

## Context

Two questions a reader asks the explorer that it had no way of answering.

**What is the number here?** [F29](benchmark-explorer.md) draws curves and labels the axes, and the
axes are where a value had to be read off — by eye, against a tick. `plot::draw` never called
`Plot::label_formatter`, so hovering ran `egui_plot`'s own default, which answers for the point
nearest the cursor and prints `"{name}\nx = {:.3}\ny = {:.3}"`. Three things were wrong with what a
reader saw, and they are [item 86](../appendix/resolved/hover-label-reads-in-decades.md):

- the number is the **plotted** one, so with `log value axis` on, 412,500 queries a second reads
  `y = 5.615`;
- the name is the **empty string** for every run of a line after a gap and for every marker overlay,
  because [F29](benchmark-explorer.md)'s gap rule names only a series' first run so the legend keeps
  one entry per series;
- it answers for **one line**, where the question this crate exists for — which capture is ahead at
  this row width — is about all of them at once.

**Would ticking this capture draw anything?** [F31](metric-availability.md) built the notion of a
measurement that does not carry a number and applied it to workloads: the tree hides what cannot be
drawn and totals what it hid. The capture list underneath it was untouched. Five committed captures
carry no macro layer at all, `ops_per_sec` and `bytes_per_sec` are absent for eighty-eight
workloads, and the latency metrics partition the corpus outright — so a reader ticking a capture to
compare against was as likely to add nothing to the chart as to add a curve, with nothing on the row
saying which.

## What it does

### A readout of every line in the column you point at

The pointer selects a **column** — one position on the key axis — and the tooltip lists every drawn
line's value in it:

```
1 KiB
■ f28-rearchive · persistent_unsorted    412,500/s
■ f28-rearchive · persistent_sorted      388,100/s
■ f27-row-sink  · persistent_unsorted    401,700/s
■ f27-row-sink  · persistent_sorted        absent
```

Largest first, because the question is usually which is ahead. Absent last, and said as the word
`absent`, because [F29](benchmark-explorer.md)'s standing rule is that a gap is never a zero and a
tooltip is the one place a reader cannot check that against the chart. A swatch in the line's own
colour ties each row back to what drew it, and a muted rule is drawn down the column being read so
the answer and the place it came from are visible together.

The values are read out of `Series::points`, which is what was measured. The log toggles rescale
what is *drawn* and do not reach the readout, which is the whole of item 86.

### One definition of where a column is

`readout::columns` is the only thing that knows where a group sits on the key axis — a bar group's
slot, a line's key, a logarithmic axis's logarithm — and both the tick captions under the chart and
the pointer's snapping are built from it. `plot::key_ticks` was rewritten onto it rather than beside
it.

### A capture that cannot answer the metric is greyed

`Index::captures_answering` asks, once per frame for the whole list, whether ticking a capture would
put anything on the chart. Judged against the **ticked workloads**; with nothing ticked, against
everything the capture measured, which is the rule
[`metrics_for`](metric-availability.md) already follows for the metric list.

A capture that answers nothing is drawn muted with `· nothing on this metric` after its label, and
its tooltip opens with which of the two questions was asked and what the answer was. It stays
tickable. A line under the list totals what the metric is keeping off the chart, the same arithmetic
the workload tree keeps.

## Design choices

**Greyed, not hidden — the opposite of what the tree above does.** [F31](metric-availability.md)
argued hiding on the size of the tree: greying out three hundred of three hundred and seventy-four
rows is not a list anybody can read. Neither half of that argument survives the move to captures. A
capture is an identity a reader knows *by name* out of twenty-seven of them, so one disappearing
reads as a corpus that lost a run rather than as a filter doing its job — and twenty-seven rows all
fit, so nothing is being made unreadable. The asymmetry is deliberate and is written on both pages.

**Still tickable.** Ticking is how a reader lines a capture up before moving the metric onto one it
answers, and a ticked row has to stay clickable or it could never be unticked. There is nothing to
protect a reader from: a capture that answers nothing contributes nothing, which the chart already
says by drawing nothing.

**The pointer reads a column, not a point.** Snapping to the nearest column and answering for all of
it is what makes the comparison readable; answering for the nearest *point* is what `egui_plot`
already did, and it is the behaviour being replaced.

**How near counts is a distance in points, not in the key's quantity.** `REACH` is 48 points. In the
key's own units it would mean one thing on a sweep of row widths from 64 B to 4 MiB and another on a
sweep of read shares from 0 to 100, and it would change meaning again on every zoom.

**Ordering by value rather than by legend.** The tooltip is read to rank the lines, so it is ordered
by rank. The cost is that a row moves between two hovers, which is why the swatch is there: the
colour is what a reader tracks across columns, not the row's position.

**The rows are built from the same `series` vector the chart was drawn from.** Not from a second
walk of the index. Two paths to one number is two things to keep in step, and the failure mode is a
tooltip that disagrees with the curve under it.

**`captures_answering` returns a vector, not an answer per capture.** One pass over the fifteen
hundred measurements rather than a binary search per (capture, workload) pair, because the picker
asks this on every frame for every row.

## Alternatives rejected

**Reformatting `egui_plot`'s label instead of refusing it.** `HoverPosition` carries the cursor
position and the nearest point's name and index, which is enough to undo the logarithm and to write
the value in the metric's unit. It is not enough to answer for the other lines — the formatter is
handed one item — and the empty name after a gap cannot be recovered from it at all. Half the defect
would have been fixed and the feature would not exist.

**Naming every run so `plot_name` is never empty.** `egui_plot` leaves an unnamed item out of the
legend, and that is exactly why the runs after the first are unnamed. Naming them puts one legend
entry per *gap* on a chart whose whole point is that gaps are common.

**A crosshair readout that follows the value axis too.** A horizontal rule and a value at the
pointer's height. It reads out where the pointer is, which is not a measurement, and the one number
on the chart that is not a measurement is the one most likely to be quoted as one.

**Hiding captures that cannot answer, for symmetry with the tree.** Symmetry is not the goal;
readability is, and the two lists fail differently. See *Design choices*.

**Judging a capture over the whole corpus rather than over the selection.** Simpler and
selection-independent, and it greys almost nothing: most captures answer most metrics for *some*
workload. It would leave a row un-greyed that adds nothing to the chart in front of the reader,
which is the case the greying exists for.

**Putting the readout behind a click, or a toggle.** A tooltip that has to be turned on is one that
is off when the question is asked.

## Limitations

**A row names the line, not the workload.** `Series` carries no per-point provenance — a point is
`(f64, Option<f64>)` and nothing else — so a readout row says `f28-rearchive · persistent_unsorted`,
which is the curve, and not which of that curve's arms sits at this key. On a curve that was split
per workload by [F32](chart-line-identity.md)'s `split_curves` the name *is* the identifier and the
question does not arise; everywhere else it does. Filed in [the todos](../appendix/todos.md) with
what it would cost.

**Twenty-four rows, then a count.** A selection of eighty arms across four captures is more lines
than a tooltip can be tall. The rest are counted, not dropped — but which twenty-four survive is
decided by value, so the absent ones are the first to go.

**The readout is a tooltip, so it cannot be selected or copied.** Reading a number out of the
explorer and into a note is still retyping it.

**Bars snap to the group, not to the bar.** A bar chart's columns are groups, and the readout lists
every series in the group — which is the same answer a line chart gives and is usually what was
wanted, but pointing at one bar does not single it out.

**Only the macro layer, still.** F29's limitation. There is nothing to read out of a layer the index
does not carry.

**The interface still has not been seen.** This machine has no display, so the swatches, the rule
down the column and the greyed rows are compiled for both targets, tested underneath, and not looked
at. Unchanged since F29.

## Invariants to uphold

**`readout::columns` stays the only definition of where a column is.** `key_ticks` consumes it. A
second piece of the same arithmetic would drift, and the failure — a column captioned with its
neighbour's key — is unreadable and looks exactly like a correct chart.
`plot::tests::a_caption_sits_where_the_readout_reads` is what notices.

**A readout value is read from `Series::points` and never from the plotted position.** The points
are raw measurements; everything the log toggles do happens downstream in `plot::runs`. This is the
whole of item 86, and the way to reintroduce it is to make the readout convenient by reusing a
number that has already been scaled.

**An absent value stays `Option::None` all the way to the text.** `Row::value` is an `Option` for
the same reason `Series::points` holds one. A `0.0` substituted anywhere in this path is a
measurement of zero, at the one place a reader has nothing to check it against.

**Every drawn line is a row.** Including the ones with nothing in this column. Dropping them makes
the readout a list of who was measured here, which reads as a list of who exists.

**`egui_plot`'s own label stays refused.** `label_formatter(|_| None)`. Returning a string from it
puts a second, differently formatted answer on the screen beside this one.

**`captures_answering` is judged against the ticked workloads, and against everything when nothing
is ticked.** Both halves. Judging an empty selection as "nothing answers anything" greys every row
before a reader has ticked anything, which is the same trap
[F31](metric-availability.md) avoided in `metrics_for` and for the same reason.

**A greyed capture stays tickable.** The list is the only way to untick one.

## Performance

Nothing measured changed, so **no capture was taken**. No workload, no `shoal.yml`, no seed and
nothing under `shoal-bench/src/workloads/` was touched; CLAUDE.md excludes UI changes, and the
generated pages under `docs/src/performance/` are untouched.

`captures_answering` is one pass over `Index::macro_points` — 1521 entries — per frame, short
circuiting each capture once it has answered. It replaces nothing, so it is new work on the frame,
and it is bounded by the corpus rather than by the selection. `readout::rows` is one scan per drawn
series over that series' points, and runs only while the pointer is over the chart.

`INDEX_VERSION` does **not** move. `Column`, `Row` and the readout are not serialized, and
`captures_answering` reads fields the index already carried.

## Tests

| Test | What breaks without it |
|---|---|
| `readout::tests::the_label_this_replaces_read_a_logarithmic_chart_in_decades` | [Item 86](../appendix/resolved/hover-label-reads-in-decades.md) returning — it holds the label the unfixed tree drew, beside what replaces it |
| `readout::tests::a_readout_lists_every_line_largest_first_and_absent_last` | The ordering, and the rule that every drawn line is a row whether or not it measured this column |
| `readout::tests::a_bar_group_and_a_line_are_at_different_positions` | A bar chart's readout reading the wrong group, since a slot is not a key |
| `readout::tests::a_logarithmic_axis_places_a_column_at_its_logarithm` | The log axis's columns, and the refusal to place a key that has no logarithm |
| `readout::tests::the_pointer_reads_the_column_it_is_nearest` | The snap itself |
| `readout::tests::a_column_is_headed_the_way_its_tick_is` | The header naming a column differently from the tick under it |
| `plot::tests::a_caption_sits_where_the_readout_reads` | `key_ticks` and `columns` drifting into two definitions of one position |
| `index::tests::a_capture_answers_a_metric_something_in_it_carries` | The greying with nothing ticked — the state the explorer opens in |
| `index::tests::a_capture_is_judged_against_what_is_ticked` | The selection narrowing the question, which is the case the greying exists for |
| `index::tests::a_capture_that_measured_the_workload_can_still_answer_nothing` | Holding a measurement being confused with holding the number |
| `explore_index::a_capture_that_measured_no_macro_arm_answers_no_metric` | The same on the real corpus, and it asserts the flag is never true without a measurement behind it |
| `explore_index::a_selection_only_narrows_what_a_capture_answers` | The narrowing being monotonic, and being non-vacuous on the committed corpus |

## Related

- [F29. An explorer that draws more than one capture](benchmark-explorer.md) — the chart this reads
  out, and the gap rule the `absent` row keeps
- [F31. A metric a workload can actually answer](metric-availability.md) — the availability model
  this extends from workloads to captures, and the *hidden* rule it deliberately does not follow
- [F32. A colour per table, and a chart that frames itself](chart-line-identity.md) — the hue the
  swatch carries, and `split_curves`, which decides when a row's name is an identifier
- [Resolved 86. The hover label read a logarithmic chart in decades](../appendix/resolved/hover-label-reads-in-decades.md)
  — the defect underneath, and what the unfixed tree drew
- [Todos](../appendix/todos.md) — naming the workload a row came from, and what `Series` would have
  to carry for it
