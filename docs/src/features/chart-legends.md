# F19. Charts that name their colours in one place

## Context

The twenty-one charts on the [Performance](../performance/overview.md) pages named their series by
writing each one's name just past its last data point. [F18](results-pages.md) argued for that
explicitly and [item 67](../appendix/resolved/chart-labels.md) reaffirmed it: three of the eight
series colours fall below a 3:1 contrast ratio on the light themes, so a chart that requires the
reader to match a colour to a name is a chart some readers cannot read at all.

The argument is right about colour and wrong about what follows from it. A label at the end of a
line identifies that line only while the lines end far apart. On a chart of a mixture they do not.
`chart-grid-latency` draws eight curves that end inside a six pixel band; the pass that pushes
colliding labels apart ran out of axis, clamped against the plot floor, and left two names three
pixels apart at an eight pixel font — the same symptom item 67 was filed for, on a chart that
fix could not reach. And a name pushed a third of the way down the chart to find room is not
attached to its curve either, so the colour match was being paid for anyway, without the legend
that would have made it possible.

Two other things on the same pages were hard to read for unrelated reasons. The three encryption
charts drew `(tls − plain) / plain` as a percentage, which cannot distinguish a large share of a
very small number from a small share of a large one — and this sweep contains both. And every byte
axis was drawn as a raw integer: `row-size.md` labelled its row-width axis `100 · 1,000 · 10,000 ·
100,000 · 1,000,000` while the very same chart named a *series* `1 MiB`, two spellings of the same
quantity a few hundred pixels apart.

## What it does

**One legend module, under every plot.** `shoal-bench/src/render/chart/legend.rs` lays out
`(swatch, name)` entries in pixel coordinates on a strip below the plot, wrapping into as many rows
as the names need. Seven chart modules use it: `sweep`, `encryption` (both charts), `micro_scaling`,
`noise_band`, `bars` and `stages_stacked`. `hotpath_scopes`, `micro_delta` and `macro_wall_clock`
label their own marks or draw one colour, and have no legend.

That replaced **four** independent copies of "push apart any two labels that landed together", with
four different separation constants, two of which pushed a colliding label down and one of which
pushed it up — and a fourth chart that had no collision handling at all.

**The encryption charts are drawn in nanoseconds.** `chart-encryption-by-row`, `-by-depth` and
`-by-clients` plot `tls_ns − plain_ns` on a duration axis, with zero drawn so that a curve hugging
it is visibly hugging it and a curve below it is a pair where the encrypted arm came out faster.
`Point::overhead_pct` still exists and is still what the prose and the tables quote.
`chart-encryption-absolute`, which said what the overhead was added *to* at a single load depth,
now covers every depth the sweep runs: four depths on two wires, eight curves.

**Byte axes read in binary units, and tick where the measurements are.** `sweep::Unit` gained
`Bytes`, `ByteRate` and `Percent`; `fmt::bytes_axis` is `fmt::bytes` for a value that has to be
rounded. A `sweep` axis whose data has twelve or fewer distinct x values ticks at those values
rather than at the powers of ten plotters would space a log axis with, so the row-width axis now
reads `64 B · 128 B · 512 B · 1 KiB · 8 KiB · 512 KiB · 1 MiB · 4 MiB` — the widths the workloads
were actually run at. `Unit::Rate` also gained a `G` step, which it had been missing: the payload
throughput axis was rendering `1000.0M`.

## Design choices

**The legend is laid out in pixels on its own drawing area, not in chart coordinates.**
`stages_stacked` used to draw its legend inside the plot's coordinate space, which meant extending
the y range to make room and clamping any row that did not fit onto the one above it. A separate
area removes the class of bug rather than the instance.

**`legend::height` is a pure function of the entries, callable before anything is drawn.**
`chart::draw` takes the canvas height as a parameter, so a chart has to know how tall its legend
will be before it has an area to ask. Height and layout are computed by the same function for that
reason, and a test asserts the height covers every row the layout produces.

**A swatch is an eleven by eleven filled block.** This is the surviving half of the argument F18
made. A two pixel stroke in a colour that is barely 3:1 against the page is not legible; a filled
block of that size is. Every chart still ships beside a table carrying the same numbers, which is
the relief rule the palette's contrast is permitted under.

**Text width is estimated at 0.6 em per character, deliberately generously.** plotters is built
here without a font backend, so extents are guessed rather than measured — the standing constraint
described in the header of `shoal-bench/tests/chart_geometry.rs`. The guess decides the column
width, where being wrong wide is a gap and being wrong narrow is the overlap this module exists to
remove.

**Ticks are derived inside `sweep::draw`, not passed by the caller.** Every sweep on the site has a
small discrete x set, so no page had to change to get them, and a sweep whose x becomes a scatter
falls back to plotters' spacing on its own rather than by somebody remembering to.

**The read-share axis stayed a percentage** — it is a knob a workload was set to, not a comparison
against anything — and gained the `%` suffix it was the only percentage axis on the site to lack.

## Alternatives rejected

**Keeping the end labels and adding a legend.** Twice the ink for one fact, and it keeps the 150 to
260 units of right margin the labels needed. Reclaiming that gutter took a sweep's plotting area
from 509 units wide to 697.

**Making the whole page's charts absolute.** `chart-micro-delta` and `chart-noise-band` are still
percentages, because the thirty-five benchmarks they cover span twenty nanoseconds to a quarter of
a second: on an absolute axis thirty of the thirty-five rows are a line at zero. A percentage is
the wrong choice where it stands in for a measurement nobody wrote down, and the right one where
the quantity genuinely is a ratio.

**Colour per depth with the encrypted curve dashed, on `chart-encryption-absolute`.** It would fit
eight curves into four colours and leave headroom. It also encodes two variables in two visual
channels one of which the stylesheet does not theme, and a dashed line at two pixels is hard to
tell from a solid one at the widths these charts are viewed at. Eight flat series with an explicit
error when a ninth appears is the cruder option and the one that cannot silently drop an arm.

**Letting `fmt::bytes` serve the axis.** It steps up a unit only when the value divides exactly,
which is correct for a chosen row width and would print an axis tick of 1,000,000 as `1000000 B`.
Two functions, because the two callers genuinely want different behaviour from the same units.

## Limitations

- **The legend costs vertical space**, 34 units for one row and 70 for three. A sweep canvas went
  from 420 to 454, and `chart-grid-latency`, whose eight names need three rows, to 490.
- **Eight series is still the cap**, and `chart-encryption-absolute` now sits exactly on it. A
  fifth depth in the sweep makes that chart fail to render rather than drop an arm, which is the
  intended failure but is still a failure a capture can cause.
- **Column width is set by the longest name**, so one long series name widens every column and can
  force a wrap that shorter names would not have needed.
- **Nothing measures the text.** The estimate is generous enough for the names the pages produce
  today; a name half again as long as `write, sorted, no storage` has never been drawn and is not
  covered by anything but the estimate.
- **The x-axis tick derivation is a heuristic with a constant in it.** Thirteen distinct x values
  fall back to plotters' spacing and the chart looks different from its twelve-value neighbour.

## Invariants to uphold

- **`legend::height` and `legend::layout` must agree.** The height is computed before the canvas
  exists and the layout while drawing on it; nothing but `the_height_covers_every_row` keeps them
  in step, and a disagreement draws entries off the bottom of the chart where no other test looks.
- **A series is named in exactly one place.** Re-adding an end label to any chart reintroduces the
  collision this removed, and `every_series_is_named_once` is what fails.
- **The text-width estimate must stay an over-estimate.** Lowering the 0.6 em factor to reclaim
  space converts every gap into a potential overlap, and with no font backend nothing will notice
  except a reader.
- **A percentage axis has to be a ratio the workload chose, not a measurement it produced.** The
  read share is the former. What TLS cost was the latter, and drawing it as a percentage is the
  thing this feature undid.
- **`fmt::bytes` and `fmt::bytes_axis` must agree on every power of two.** They differ on purpose
  elsewhere; disagreeing on 4096 would put one number on an axis and a different one in the table
  beside it.

## Performance

Nothing measurable, and nothing that could be: this is entirely in `shoal-bench`'s renderer, which
runs after a capture and touches no code any benchmark executes. **No capture was taken and none is
needed** — the rule in `CLAUDE.md` is that a renderer change is verified by `render` and
`render --check`, and the artifacts under `docs/perf/runs/` are untouched by it.

The generated pages grew, because eight curves' worth of names is more markup than eight end labels
and because `chart-encryption-absolute` now draws four times as many curves.

## Tests

| Test | What breaks without it |
| --- | --- |
| `legend` — `the_height_covers_every_row` | the canvas being tall enough for the strip it reserves, across five entry counts and four name lengths |
| `legend` — `long_names_wrap` | entries past the first row's capacity being drawn off the right of the canvas |
| `legend` — `one_enormous_name_still_lays_out` | a division by zero on a name wider than the chart, which the profile's scope names are |
| `legend` — `drawing_is_deterministic` | `render --check` on a tree nobody touched |
| `sweep` — `it_draws_and_labels_every_series` | a name drawn twice, which is what an end label surviving looks like |
| `sweep` — `the_canvas_makes_room_for_the_legend` | the legend being drawn over the plot instead of under it |
| `sweep` — `a_short_x_axis_ticks_at_the_measurements` | the byte axis reverting to powers of ten, spelled in binary units, which is worse than either |
| `sweep` — `a_long_x_axis_is_left_to_plotters` | forty ticks on an axis with room for six |
| `encryption` — `the_overhead_chart_is_drawn_in_nanoseconds` | the percentage axis coming back |
| `encryption` — `the_absolute_chart_covers_the_whole_sweep` | the absolute view silently narrowing to one depth again |
| `encryption` — `too_many_depths_is_refused` | a ninth curve reusing a colour, so two lines are indistinguishable |
| `chart_geometry` — `every_series_is_named_once` | the same, over the widest legend the renderer can be asked for |
| `chart_geometry` — `legend_names_stay_with_their_swatches` | the text-width estimate being narrowed until columns overlap |
| `chart_geometry` — `stacked_labels_have_room` | the legend row pitch dropping below the height of the text in it |
| `fmt` — `a_power_of_two_reads_the_same_either_way` | an axis and the table under it disagreeing about what 4096 bytes is called |
| `fmt` — `an_axis_tick_between_units_is_rounded_rather_than_spelled_out` | the case `fmt::bytes` cannot serve, which is why there are two functions |

## Related

- [F18. Results pages that explain themselves](results-pages.md) — which built these charts, and
  whose "labels at the end of the line, never a legend" this reverses
- [Resolved 67, 68. Chart labels collided](../appendix/resolved/chart-labels.md) — which
  deconflicted the labels and rejected a legend; that rejection is struck through there
- [F7. A benchmark runner that renders its own results](bench-runner.md) — which built the renderer
- [F14. Encryption in transit](encryption-in-transit.md) — which added the charts now drawn in
  nanoseconds
- [F17. The workload grid](workload-grid.md) — whose eight-curve latency chart is the one the end
  labels could not survive
