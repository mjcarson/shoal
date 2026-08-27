# F32. A colour per table, and a chart that frames itself

## Context

[F29](benchmark-explorer.md) made the explorer's axes a control and
[F30](plot-axis-units.md) made a selection draw as one line per *(capture, curve)* rather than as one
zigzag per capture. Three things about the chart that produced were still wrong, and all three are
about what a reader takes off it rather than about which numbers are on it.

**The colours meant a position, not a thing.** `theme::series` was indexed by `Series::group` — the
curve's position among the ones being drawn — so the table an arm ran against carried no colour of
its own. Tick the sorted table alone and it is blue; tick the unsorted one ahead of it and the
sorted table is orange. The chart's own module doc has said since F29 that *hue carries the series -
the table, or the workload*, and the table half of that was never true.

**Two workloads whose recorded facts agree drew as one line.** F30 groups a sweep's arms by
`Index::curve_key`, the facts a caller *set* minus the one the axis reads. `macro/get_resident` and
`macro/get_archived` are one workload struct with a `residency` field that reaches no `ScaleFacts`,
so both answer to one key, both sit at 256-byte rows, and ticking both drew a single line through two
unrelated measurements at one position on the axis. That is
[item 84](../appendix/resolved/identical-facts-one-line.md), which this feature resolves.

**The chart stayed framed for whatever it had been framed for.** The plot's id is a constant, so
`egui_plot`'s `PlotMemory` outlives every change to the metric, the axis and the selection. Right
while the same numbers are on screen and wrong the moment they are not: a chart framed on a rate and
then handed a p99 is framed four orders of magnitude from its own data, and draws empty. That is
[item 85](../appendix/resolved/chart-framed-for-the-last-selection.md).

## What it does

**Identity is split across three channels instead of two.**

| Channel | What it carries | Where it comes from |
|---|---|---|
| Hue | The **table** the arm ran against | `Series::hue`, the table's fixed slot in `index::TABLE_ORDER` |
| Dash pattern | The capture, on a sweep | The capture's position among the ones drawn |
| Marker | Which of that table's curves this is | `Series::mark`, the curve's position within its colour |

So two captures of one curve are one colour with one marker in two dash patterns — the comparison
the explorer exists to make, unchanged — and two curves of one table are one colour in two markers.
`Series::group` is gone; `hue` and `mark` replace it, and they are the only fields the drawing code
reads to decide how a line looks.

`TABLE_ORDER` is `persistent_unsorted, persistent_sorted, ephemeral_unsorted, ephemeral_sorted`, the
order every chart and table on the site reads them in, mirroring
`shoal_bench::render::arms::table_kinds`. **All four slots are reserved whether or not a chart draws
them**, which is what makes a table's colour a property of the table rather than of what happens to
be ticked beside it. A table kind this build has never been told about takes one slot for itself and
shares it with every curve on that kind; a curve whose measurement records no table at all takes a
slot of its own, because nothing is known to be in common between it and another.

A timeline has no capture to encode — every series on it is a workload measured across the same set
of captures — so there the mark carries both channels: it walks the five markers first and moves the
dash only once it has run out of them. `theme::MARK_CYCLE` is that number, and `plot::stroke_of` is
the one place either rule is written.

**A curve that would draw two of its arms at one key position is split into one line per workload**,
each named by the identifier that is the only thing separating them —
`f28-rearchive · macro/get_resident` beside `f28-rearchive · macro/get_archived`. Every other curve
is carried through whole, so a genuine sweep — three widths of one grid family — is still one line
of three points. `Index::split_curves` does this over the grouped curves, after `curve_names` has
named them; the curve key itself is untouched.

**The chart is framed on its data whenever what is on it changes.** `Explorer::framing` is every
input that decides what is drawn — the captures, the workloads in tick order, the metric, the key
axis, lines-or-bars, and both log toggles. When it moves, `plot::View::refit` turns `auto_bounds`
back on for that frame. A pan or a zoom the reader made themselves sticks until the next such
change, which is what makes doing it by hand worth anything. A `fit` button in the toolbar asks for
the same thing directly, for the reader who panned and wants the whole chart back without hunting
for the double click that also does it.

## Design choices

**The split happens after the curves are named, not inside the key.** Adding the workload identifier
to `CurveKey` is the obvious fix and it is wrong twice over. It breaks the invariant
[item 82](../appendix/resolved/one-line-per-capture.md) states — *a curve key must not hold a fact
that co-varies with the axis* — because a width sweep's arms have one identifier each and would
shatter into one curve per point. And it breaks the second one, that every key built in one call is
the same length, because only some curves would need the entry. Resolving the collision downstream
leaves both alone: `curve_key` still answers *which curve is this*, and `split_curves` answers the
different question of *would drawing that curve hide anything*.

**Fixed table slots rather than slots for the tables present.** Numbering the tables that happen to
be on the chart would put the sorted table at hue 0 when it is alone and hue 1 when the unsorted one
is ticked ahead of it, which is exactly the shuffling this feature exists to end. Reserving all four
costs four of the palette's eight hues on a chart that draws no table at all, which is the price.

**The dash still means the capture on a sweep.** It would have been simpler to hand `theme` a single
flat "which line of this colour is this" index and let it pick both channels. That reads the same on
a chart of one curve per table and stops meaning anything on a chart of two: the dash would walk the
curves and the captures together, and *this pair is the same measurement twice* would no longer be
legible. Keeping the capture in the dash is what preserves F30's comparison.

**Markers are drawn on the measurements, not on a sample of the line.** A marker every *n* pixels
would be prettier at a distance and would say nothing about where a measurement is. Each marker sits
on a point the corpus recorded, so the marker layer doubles as *this line has four arms, not
forty*.

**`refit` is passed in rather than worked out.** `plot::draw` is handed one frame's series and has
nowhere to remember the last frame's. Comparing a `Framing` in `Explorer` — which does — is exact,
where a hash of the series would make a collision into a chart that quietly stopped reframing.

## Alternatives rejected

**One line per selected workload, always.** The literal reading of *selecting two workloads should
draw two lines*, and it destroys the sweep: three widths of one grid family become three lines of
one point each, and the curve F30 was built to draw stops existing. The collision split gets the
same answer where it is the right one and leaves the sweep alone.

**Colouring by results-page family.** The family is the repo's own encoding of *what this measures*
and is coarser than the table: `macro/grid/sorted/r50/1024` and `macro/grid/unsorted/r50/1024` are
one family and two tables, which is the distinction a reader is looking at the chart to see. It was
also already rejected for splitting curves, in F30, for the neighbouring reason.

**Refitting every frame.** Always exactly framed on the data, and pan and box-zoom stop having any
effect at all — including the box zoom `allow_boxed_zoom(true)` has offered since F29. The reader
who wants to look closely at one decade of a width sweep has no way left to.

**Varying the plot id with the selection.** `egui_plot` keys its memory on the id, so a new id is a
new plot with default bounds — which would have refit correctly and thrown the reader's own zoom
away every frame anything changed, rather than only when what is drawn changed.

## Limitations

**The palette wraps at eight and four slots are spoken for.** A chart drawing no table at all — most
of the isolating workloads record none — gets its first curve at hue 4, so its fifth curve wraps
onto `persistent_unsorted`'s blue. The marker is what still separates them, which is the same answer
`theme::series` has always given for a ninth series, but it is a real reduction from the eight
distinct hues such a chart used to get.

**Many curves of one table are now one colour.** The reverse of the case above, and it is real:
[F20](configuration-sweeps.md)'s fifty-eight configuration arms are one grid cell with one setting
moved, so they are fifty-eight curves against `persistent_unsorted` and every one of them is blue.
The mark still separates them, but it wraps every six, where the old *hue is the curve* rule gave
them eight rotating colours before it wrapped. The sweep view of those arms was already the wrong
view — [the todos](../appendix/todos.md) say why, and the timeline is the right one — so this makes
a bad chart slightly worse rather than making a good one bad.

**A split line's legend entry is the whole workload identifier.** No common prefix is stripped, so
two split arms of one grid family read as two forty-character strings differing in their last
segment. Stripping it would need the split set's longest common prefix and a rule for what to do
when it is the whole identifier; it is cosmetic and was left.

**An unrecorded table kind is stable per kind, not across selections.** The four named tables hold
their slots absolutely. Anything else is numbered in the order it is first seen among the curves
being drawn, so ticking a second unknown kind ahead of the first moves the first one's colour.

**Bars still colour by position.** A bar has neither of the other two channels, so two captures of
one curve drawn in one hue would be two adjacent bars of the same colour with nothing to tell them
apart. The rule in the table above is the line chart's.

**The interface has still not been seen.** This machine has no display. Everything under the drawing
code is tested and `stroke_of` is tested directly; that the markers land where they are meant to on
a real canvas is checked by compiling against `egui_plot` 0.37's signatures and by nothing else.

## Invariants to uphold

**A table's hue is its slot in `TABLE_ORDER`, not its position among the ones drawn.** All four slots
stay reserved. Renumbering them onto only the tables present is the shuffling this replaced, and it
will look like a simplification when somebody next reads `hues`.

**`series_marker(0)` is `None`.** A chart making one distinction must not look as though it is making
two. `no_marker_is_a_plain_circle` guards the other end of the same rule: a run of a single
measurement is already drawn as a circle, so no marker in the cycle may be one.

**The collision split stays out of `curve_key`.** Both of item 82's invariants depend on it — a key
must hold no fact that co-varies with the axis, and every key built in one call is the same length.

**`Framing` holds every input that changes what is drawn, and nothing that does not.** A field left
out is a chart that keeps a frame fitted to numbers no longer on it; a field wrongly added is a
reader's zoom thrown away for a change they cannot see.

**`refit` is spent the frame it is used.** Leaving it set makes the chart refit forever, which is
the *always fitted* behaviour this deliberately did not build.

**Nothing here may be added to `render::chart::palette`.** Unchanged from F29 and it now covers the
markers too: `shoal-bench/tests/css_sync.rs` asserts a bijection between that module's sentinels and
the rules in `docs/theme/charts.css`.

## Performance

Nothing measurable, and no capture. `split_curves` is one pass over the drawn curves with a
quadratic collision check inside each — the corpus's largest legitimate curve is sixteen widths, so
that is a hundred and twenty comparisons on the worst chart anybody can build. `marks` is quadratic
in the number of curves, which is bounded by the number of ticked workloads. Both run once per
frame, in the same function that already clones a `Vec<String>` per curve to name it.

## Tests

| Test | What breaks without it |
|---|---|
| `index::tests::two_workloads_at_one_key_position_are_two_lines` | The zigzag returning for two workloads the recorded facts cannot tell apart |
| `index::tests::a_split_line_is_named_by_the_workload_that_separates_it` | Two lines carrying one legend entry between them |
| `index::tests::row_count_does_not_split_a_width_sweep` | The split reaching a genuine sweep and shattering it into single points |
| `index::tests::one_table_is_one_hue_and_its_curves_differ_in_the_mark` | Hue going back to meaning the curve's position |
| `index::tests::a_table_keeps_its_hue_when_another_table_is_added` | Colours shuffling as unrelated arms are ticked |
| `index::tests::a_named_table_holds_its_slot_whether_or_not_it_is_drawn` | `TABLE_ORDER` being renumbered onto the tables present |
| `index::tests::a_marker_counts_the_curves_of_one_colour` | A chart of one line per table growing markers |
| `index::tests::two_captures_of_one_curve_share_a_hue_and_a_mark_and_differ_in_the_capture` | The comparison the explorer exists to make |
| `theme::tests::a_marker_stands_for_a_curve_within_its_hue` | Mark zero taking a marker, or the cycle repeating early |
| `theme::tests::no_marker_is_a_plain_circle` | A circle meaning both *a lone measurement* and *the first marked curve* |
| `plot::tests::a_sweep_puts_the_capture_in_the_dash_and_the_curve_in_the_marker` | F30's regression: the dash walking the series list rather than the captures |
| `plot::tests::a_timeline_walks_the_markers_before_it_moves_the_dash` | Ten workloads of one table drawn identically |
| `plot::tests::a_timeline_ignores_the_capture_position` | A dash pattern standing for a capture on an axis that has no single one |
| `app::tests::each_input_that_changes_the_chart_is_a_different_framing` | A metric change leaving the chart framed on the last metric's range |
| `app::tests::tick_order_is_part_of_the_framing` | A reordered selection, which is a different chart, not being reframed |
| `explore_index::two_workloads_the_facts_cannot_tell_apart_are_two_lines` | The same on the real corpus, against the two keyed gets |
| `explore_index::the_grid_preset_reproduces_chart_grid_throughput` | A table drawn in a colour that is not its own, on the real corpus |

## Related

- [F30. Only comparable axes share a chart](plot-axis-units.md) — the curve model this splits, and
  the hue-and-dash rule this replaced
- [F29. An explorer that draws more than one capture](benchmark-explorer.md) — the explorer, and the
  palette that is now indexed by the table
- [84. Two workloads the recorded facts cannot tell apart drew as one line](../appendix/resolved/identical-facts-one-line.md)
- [85. The chart stayed framed for the last thing it drew](../appendix/resolved/chart-framed-for-the-last-selection.md)
- [82. Every selected workload was folded into one line per capture](../appendix/resolved/one-line-per-capture.md) — the
  same defect in the case its fix did reach
