# 86. The hover label read a logarithmic chart in decades, for a line it could not name

## Symptom

Hover any point on the explorer's chart. The label that appears says:

```

x = 3.000
y = 5.615
```

Three things are wrong with it and they are independent.

**The value is the plotted number, not the measurement.** With `log value axis` on, a curve at
412,500 queries a second is *drawn* at 5.615, and that is what the label reports. Nothing on the
label says it is a logarithm. A reader who turns the toggle on to compare two decades and then hovers
gets a number four and a half orders of magnitude away from the measurement, in no unit, which will
be read as the answer because there is nothing else on offer.

**The line has no name.** The first line above is the series name and it is empty for every run of a
line after a gap, and for every marker overlay. [F29](../../features/benchmark-explorer.md) splits a
line into one `Line` per run of consecutive measurements — that is how a gap is drawn — and names
only the first, so that the legend keeps one entry per series rather than one per gap. Fifteen
hundred of ten thousand possible measurements exist, so gaps are the common case, and so is the
unnamed label.

**It answers for one line.** The nearest point's, whichever that is. The question the explorer exists
for is *which capture is ahead at this row width*, which is about every line at once.

## Cause

`plot::draw` never called `Plot::label_formatter`, so `egui_plot`'s own default ran
(`egui_plot 0.37`, `label.rs:47`):

```rust
pub fn default_label_formatter(pos: &HoverPosition<'_>) -> Option<String> {
    Some(match pos {
        HoverPosition::NearDataPoint { plot_name, position, index: _ } =>
            format!("{}\nx = {:.3}\ny = {:.3}", plot_name, position.x, position.y),
        HoverPosition::Elsewhere { position } =>
            format!("x = {:.3}\ny = {:.3}", position.x, position.y),
    })
}
```

`position` is in **plot space**, and plot space is where the logarithms live. `plot::runs` takes the
logarithm of each value as it builds the points it pushes (`plot.rs`, `runs`), and the axis
formatters undo it for the ticks — `y_axis_formatter` applies `value_of` before
`fmt::value(unit, …)`. There was nothing between the label and the same numbers.

The crate already had every piece needed to write the number correctly: `fmt::value(unit, …)` is what
the ticks are written with, and `Series::points` holds the raw measurement the log transform is
applied *downstream* of. Neither was reachable from a formatter that was never installed.

## Evidence

**Established by reproduction**, against a formatter that is public in the dependency, since the
symptom is a canvas this machine has no display to draw.
`readout::tests::the_label_this_replaces_read_a_logarithmic_chart_in_decades` calls the exact
function the unfixed tree used, with the position the unfixed tree would have handed it:

```rust
let plotted = 412_500f64.log10();
let drawn = egui_plot::default_label_formatter(&egui_plot::HoverPosition::NearDataPoint {
    plot_name: "",
    position: PlotPoint::new(3.0, plotted),
    index: 0,
})
.unwrap_or_default();
assert_eq!(drawn, "\nx = 3.000\ny = 5.615");
```

That string is the defect: an empty name, and `5.615` where the measurement is 412,500 queries a
second. The test keeps it, and asserts beside it that the readout which replaced it answers
`Some(412_500.0)` and renders `412,500/s`.

## The fix

[F33](../../features/chart-readout.md). `egui_plot`'s label is refused rather than reformatted —
`plot = plot.label_formatter(|_| None)` — and `shoal-top/src/readout.rs` answers instead.

The pointer selects a **column** rather than a point, and the readout lists every drawn line's value
in it, largest first, absent last, each written with `fmt::value` in the metric's own unit. The values
are read from `Series::points`, which holds measurements; the log toggles rescale what is drawn and
are applied in `plot::runs`, downstream of everything the readout touches. So the decades cannot come
back by the route they came the first time.

Refusing rather than reformatting is what fixes the other two halves at once. A formatter is handed
one item, so it can never answer for the rest of the chart, and the empty name is not recoverable
from inside it — the name is empty because the item genuinely has none.

## Alternatives rejected

**Reformatting the label instead of refusing it.** `HoverPosition` carries enough to undo the
logarithm and write the unit. It carries nothing about the other lines, and cannot: one hover, one
item. It would have fixed the number and left the two halves that matter more.

**Naming every run so `plot_name` is never empty.** `egui_plot` leaves an unnamed item out of the
legend, which is exactly why the later runs are unnamed. Naming them puts one legend entry per gap on
a chart whose gaps are the common case.

**Not taking the logarithm of the points, and asking `egui_plot` for a log axis instead.** It would
put raw values in plot space and fix the label at the source. `egui_plot` 0.37 has no logarithmic
axis mode — `log_grid_spacer` spaces the *marks* on a linear axis — so this is not a smaller change,
it is a different library.

**Leaving it, on the grounds that the ticks are correct.** The ticks are, and a reader who has
zoomed has one tick either side of what they are looking at. The label is what gets quoted.

## Invariants to uphold

**A readout value is read from `Series::points`, never from a plotted position.** The points are raw
measurements and every rescaling happens downstream in `plot::runs`. Reusing an already-scaled number
because it is convenient is precisely how this defect existed.

**`egui_plot`'s own label stays refused.** `label_formatter(|_| None)`. A string returned from it puts
a second, differently formatted answer on the screen beside the right one.

**An absent value stays `None` all the way to the text.** The readout says `absent`. F29's rule that a
gap is never a zero applies hardest here, because a tooltip is the one place a reader cannot check the
number against the chart.

## Still open

Nothing from this item. One neighbour is recorded in
[the todos](../todos.md) rather than here: a readout row names the **line**, not the workload, because
`Series` carries no per-point provenance. That is a limitation of what the readout can say, not a
wrong thing it says.

## Tests

| Test | What breaks without it |
|---|---|
| `readout::tests::the_label_this_replaces_read_a_logarithmic_chart_in_decades` | The defect returning: it holds the label the unfixed tree drew, and asserts the replacement answers with the measurement and its unit |
| `readout::tests::a_readout_lists_every_line_largest_first_and_absent_last` | The half of the defect that is *one line answered, not all of them* |
| `readout::tests::a_column_is_headed_the_way_its_tick_is` | The header naming a column differently from the tick under it, which is the axis half of the same confusion |
| `plot::tests::a_caption_sits_where_the_readout_reads` | The readout and the captions drifting into two definitions of one column |

## Related

- [F33. The chart answers where you point, and the capture list says whether it can](../../features/chart-readout.md)
  — the change this was fixed by
- [F29. An explorer that draws more than one capture](../../features/benchmark-explorer.md) — where
  the unnamed runs and the log transform in `plot::runs` both come from
- [F30. Only comparable axes share a chart](../../features/plot-axis-units.md) — `log_x`, which is the
  other half of what plot space rescales
