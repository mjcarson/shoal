# 85. The chart stayed framed for the last thing it drew

## Symptom

Scroll to zoom the explorer's chart once, then move the metric from `queries per second` to a `p99`,
and the chart is empty. Nothing is wrong with the series — they are built, they are handed to
`egui_plot`, and the caption above the chart correctly says how many points are absent. They are
simply drawn several orders of magnitude outside the visible range, because the range is still the
one that framed a rate.

The same holds for every other change to what is drawn: ticking a workload whose numbers are an
order of magnitude away from the ones already on the chart, moving the key axis from the read share
to the row width, switching the value axis to logarithmic, or going from lines to bars. In each case
the numbers move and the frame does not.

Double-clicking the chart fixes it. Nothing in the interface says so.

## Cause

The explorer never asked `egui_plot` to frame anything. The whole of the crate's influence over the
plot's bounds was one line, for bar charts only:

```rust
if matches!(chart, Chart::Bars) && !log_y {
    plot = plot.include_y(0.0);
}
```

Nothing called `Plot::auto_bounds`, `Plot::reset`, or `PlotUi::set_auto_bounds`. That is not itself
the defect — `egui_plot`'s `default_auto_bounds` is `true.into()`, so a plot frames itself on first
sight — but it becomes one because of where the answer is kept. `PlotMemory` is loaded and stored
against the plot's id (`egui_plot 0.37`, `plot.rs:909` and `memory.rs:67`), and the explorer's id is
a constant:

```rust
let mut plot = egui_plot::Plot::new("shoal-top-chart")
```

Every drag, scroll and box zoom writes `mem.auto_bounds = false.into()` (`plot.rs:1034`, `:1041`,
`:1151`, `:1198`, `:1239`). Since the id never varies, that `false` survives every change the reader
makes to what is on the chart — the metric, the axes, the log toggles, the whole selection. From the
first pan onwards the chart is pinned to a range chosen for numbers that may no longer be on it.

The one-constant id is right, and is why this could not fix itself: varying the id with the selection
would give each new selection a fresh `PlotMemory` at default bounds, which frames correctly and
throws the reader's zoom away on every frame anything moves.

## Evidence

**Established by reading the source**, in three places, rather than by reproducing it: this machine
has no display, and the behaviour is a property of a canvas nobody here can put on a screen.

1. `shoal-top` calls none of `auto_bounds`, `default_x_bounds`, `default_y_bounds`, `reset` or
   `set_auto_bounds`. `grep -rn` over the crate returns nothing for any of them.
2. `egui_plot 0.37` keys `PlotMemory` on the plot id (`memory.rs:67`, `memory.rs:71`) and the id is
   the literal `"shoal-top-chart"` — there is one `PlotMemory` for the explorer's whole lifetime.
3. Five sites in `plot.rs` clear `auto_bounds` on interaction and exactly one restores it: the
   double-click at `plot.rs:1014`, which is guarded by `allow_double_click_reset` and is not
   mentioned anywhere in the interface.

What is now tested is the mechanism that replaces it, not the symptom.
`app::tests::each_input_that_changes_the_chart_is_a_different_framing` walks every field of `Framing`
and asserts each one moved is a different framing, and asserts that an unchanged one is not — which
is the pair of properties the fix depends on. That is one step removed from the defect and is said so
here rather than dressed up as a reproduction.

## The fix

[F32](../../features/chart-line-identity.md), in the half of it that is this item. The decision moves
to the only place that can make it: `Explorer`, which sees this frame's inputs and last frame's.

`Framing` holds every input that decides what is on the chart — the captures, the workloads **in tick
order**, the metric, the key axis, lines-or-bars, and both log toggles — and nothing that decides how
it looks once it is there. `Explorer::ui` builds one each frame, compares it with the stored one, and
sets `refit` when it differs. `plot::View::refit` carries that into the chart, where the first
statement inside the closure is:

```rust
if refit {
    plot_ui.set_auto_bounds(true);
}
```

`set_auto_bounds` queues a `BoundsModification` that `egui_plot` applies after the build closure has
returned and before it resolves the bounds, so the frame it produces is the one fitted to the points
pushed below it. The flag is cleared the moment it has been passed, so a pan or a zoom the reader
made sticks until the next thing that changes what is drawn.

A `fit` button in the toolbar sets the same flag directly, for the reader who panned and wants the
whole chart back without knowing about the double click.

## Alternatives rejected

**Varying the plot id with the selection.** A new id is a new `PlotMemory` at default bounds, so this
frames correctly — and discards the reader's zoom every frame anything changes, including changes
that do not move the numbers. It also loses the box zoom `allow_boxed_zoom(true)` has offered since
F29 as soon as a hover changes anything.

**Refitting every frame.** Exactly framed on the data, always, and pan and box-zoom stop having any
effect at all. The reader who wants to look closely at one decade of a width sweep — which is most of
why the explorer allows zooming — has no way left to.

**Hashing the series and refitting when the hash moves.** Closer to the truth than the inputs are,
since it would catch a source whose numbers changed under an unchanged selection. Rejected because a
collision is a chart that quietly stops reframing, which is the defect back with no way to see it,
and because `Index` is immutable for the life of the explorer so the extra sensitivity buys nothing
today. The moment a live server becomes the second `Source` this is worth revisiting.

**Fitting inside `plot::draw` from remembered state.** `draw` is a free function handed one frame's
series, so it would have to keep the comparison in `egui` memory keyed by the same constant id — the
same single-slot store that caused this, with a second thing in it.

## Invariants to uphold

**`Framing` holds every input that changes what is drawn, and nothing that does not.** A field left
out is a chart that keeps a frame fitted to numbers no longer on it. A field wrongly added is a
reader's zoom thrown away for a change they cannot see. `each_input_that_changes_the_chart_is_a_different_framing`
asserts both halves.

**Tick order is part of it.** `Explorer::workloads` is kept in tick order because the first ticked
workload sets the units and so decides which half of a mixed selection survives. The same set in
another order is a different chart, and `tick_order_is_part_of_the_framing` says so.

**`refit` is spent the frame it is used.** Leaving it set makes the chart refit forever, which is the
*always fitted* behaviour that was rejected above.

**The plot id stays a constant.** It is what makes a zoom outlive a hover. The reframing is driven
from outside it precisely so that it can.

## Still open

Nothing from this item. One neighbour is worth knowing about and is not a defect: `Framing` compares
the explorer's *inputs*, so a `Source` whose numbers change under an unchanged selection would not
reframe. `Index` cannot do that, and the live-server source in [todos](../todos.md) could — the
hashing alternative above is where to start if it is built.

## Tests

| Test | What breaks without it |
|---|---|
| `app::tests::each_input_that_changes_the_chart_is_a_different_framing` | A metric, axis, chart, log or selection change leaving the chart framed on the last one's range — and, in its other half, a chart that reframes every frame and discards the reader's zoom |
| `app::tests::tick_order_is_part_of_the_framing` | A reordered selection, which is a different chart because the first tick sets the units, not being reframed |

## Related

- [F32. A colour per table, and a chart that frames itself](../../features/chart-line-identity.md) —
  the change this was fixed by
- [F29. An explorer that draws more than one capture](../../features/benchmark-explorer.md) — where
  the constant plot id and the untouched default bounds came from
- [F31. A metric a workload can actually answer](../../features/metric-availability.md) — the metric
  control whose every move is one of the changes that now reframes
