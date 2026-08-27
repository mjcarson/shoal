# 82. Every selected workload was folded into one line per capture

## Symptom

The explorer's opening chart — throughput against the read share, on the newest capture that
measured anything — drew **one line** where [the grid page](../../performance/grid.md) draws four.
The line zigzagged: at each of the six mixtures it passed through four unrelated measurements, one
per table, in whatever order the sort left them, and the shape a reader took off it was the shape of
that ordering rather than of anything measured.

The same fold applied to every other selection. Ticking a smoke-scale arm beside a full-scale one
joined a rate over two thousand rows to a rate over two hundred thousand. Ticking a per-batch p99
beside a per-query one joined a queueing delay to a service time. Ticking an arm whose payloads come
from a declared distribution of widths put a *mean* width on an axis of measured ones.

## Cause

`Source::series` had one loop for a sweep, and the loop was over captures:

```rust
Axis::Sweep(sweep) => {
    for capture in &captures {
        let mut points: Vec<(f64, Option<f64>)> = Vec::new();
        for workload in &selection.workloads {
            ...
            points.push((key, self.value(point, &selection.metric)));
        }
        points.sort_by(...);
        series.push(Series { name: self.label_of(*capture), capture: *capture, points });
    }
}
```

Every selected workload contributed a point to the same vector. The only thing that separated one
series from another was which capture it came from, so a chart of one capture was a chart of exactly
one line however many workloads were ticked.

Two of the refusals that would have caught the rest of it were **documented and never written**. The
doc comment on `ScaleFactsLite::scale` said "the explorer refuses to put the two on one axis", and
the one on `Timing` said "putting them in one series is the easiest confident wrong answer this tool
could produce, so `Source::series` refuses to". Neither field was read anywhere in `shoal-top`
outside the projection that wrote it. The claims were written when the model was designed and the
code that would have honoured them was never added; nothing failed, because nothing checked.

## Evidence

**Established by reproduction.** Three tests were written against the unfixed tree, and this is what
it did:

```
---- index::tests::a_sweep_draws_one_curve_per_table_not_one_line_per_capture stdout ----
assertion `left == right` failed: expected one curve per table, got ["one"]
  left: 1
 right: 2

---- index::tests::a_smoke_arm_may_not_share_an_axis_with_a_full_one stdout ----
assertion `left == right` failed: the smoke arm reached the axis
  left: 3
 right: 2

---- index::tests::a_per_batch_latency_may_not_share_an_axis_with_a_per_query_one stdout ----
assertion `left == right` failed: the per batch arm reached the axis
  left: 2
 right: 1
```

`got ["one"]` is the whole defect in one line: four arms across two tables, and the only name the
chart had for what it drew was the capture's.

The six tests that already existed passed unchanged against the same tree, which is the point — this
was not caught by anything, and could not have been, because nothing had ever asked how many series
a multi-workload selection produces.

## The fix

[F30](../../features/plot-axis-units.md) in full. Two changes, and they are separate:

**Which workloads may share a chart at all** is now decided by `AxisUnits` — the value axis's unit,
refined by the named scale and, for a percentile, by how its samples were stamped; and the key
axis's unit, which is absent for a measurement that has no position on the chosen axis. The first
ticked workload sets the units and `Index::in_one_unit` drops everything else, so the two documented
refusals are now performed rather than described.

**How a legitimate selection is drawn** is decided by `Index::curve_key`. A sweep emits one series
per *(capture, curve)*, where a curve is the facts a caller set — width, share, load depth, clients,
row profile, key distribution, table, and the server configuration — minus whichever one the axis is
reading. The opening chart is four curves named `f24-routing · persistent_sorted` and its three
neighbours, which is `render/pages/grid.rs::throughput` arm for arm and line for line.

## Alternatives rejected

**Refusing to draw, rather than refusing to join.** `Index::incomparable` is the house precedent and
it never refuses: it draws two incomparable captures and puts a strip above them naming what
differs. That is right for captures, because the numbers exist and somebody will go looking for
them. It is wrong here, because there is no chart to put a strip above — a mean width and a measured
one have no shared axis to be drawn badly on. So the refusal is moved earlier, into the picker,
where the disabled checkbox carries the same explanation the strip would have.

**Splitting by family instead of by units.** The family is the repo's own encoding of *what this
measures*, and it would have caught the grid-versus-read-path pairing that units allow. It was
rejected because that pairing is a real question — an insert loop and a mixture are both queries a
second and a reader comparing them is asking something answerable — and because family granularity
does not line up with the axes: `row-size` and `width-depth` are separate families and both are row
width sweeps.

**Putting every scale fact in the curve key.** The strictest reading, and it does not work.
`workloads/grid.rs` seeds a constant byte budget clamped between `MIN_ROWS` and `MAX_ROWS`, so a
width sweep's row count *follows* the width. A key holding it gives fifty-two curves of one point
each. `rows` and `keys` are therefore excluded, and `row_count_does_not_split_a_width_sweep` is what
stops them coming back.

## Invariants to uphold

**A curve key must not hold a fact that co-varies with the axis.** This is the trap the fix walked
into once. Before adding a field to `curve_key`, ask whether a workload sets it or derives it: `rows`
and `keys` are derived from the width and a byte budget, and anything else added later that is sized
rather than chosen belongs out of the key for the same reason.

**Every key built in one call is the same length.** `curve_names` diffs two keys positionally, and
`conf_key` writes the same ten entries whether or not a measurement recorded a configuration
precisely so that a capture that recorded none lines up against one that did.

**The first ticked workload sets the units, and selection order is tick order.** `Explorer::workloads`
is documented as being kept in tick order so learned colours do not shuffle; the units rule now
depends on that too. A future change that sorts it would silently change which group of a mixed
selection survives.

**A refusal is explained where it happens.** `AxisUnits::differs_from` returns the sentence, not a
boolean. The picker shows it on the disabled checkbox and nothing else composes one, so there is one
wording per reason and no way to disable something without saying why.

## Still open

~~Nothing from this item.~~ **One thing, found later and fixed by
[F32](../../features/chart-line-identity.md):** the fix stops the fold wherever the recorded facts
can tell two arms apart, and two workloads that set none of them differently are still one curve.
`macro/get_resident` and `macro/get_archived` are one struct with a `Residency` field that reaches no
`ScaleFacts`, so ticking both drew the same zigzag on a smaller set. That is
[item 84](identical-facts-one-line.md), and the two invariants stated below are what decided its fix
goes downstream of the curve key rather than into it.

The neighbouring gap this item exposed is recorded rather than fixed: the
configuration sweep's fifty-eight arms all sit at the same position on every sweep axis, because
they are one grid cell with one setting moved. They are separated into curves by the conf half of the
key, but a sweep is the wrong view for them and the timeline is the right one. Filed in
[todos](../todos.md).

## Tests

| Test | What breaks without it |
|---|---|
| `index::tests::a_sweep_draws_one_curve_per_table_not_one_line_per_capture` | The zigzag returning: four tables joined into one line at six mixtures |
| `index::tests::a_smoke_arm_may_not_share_an_axis_with_a_full_one` | The refusal `ScaleFactsLite::scale` documented since F29 and never performed |
| `index::tests::a_per_batch_latency_may_not_share_an_axis_with_a_per_query_one` | The refusal `Timing` documented since F29 and never performed |
| `index::tests::row_count_does_not_split_a_width_sweep` | `rows` re-entering the curve key, shattering the width sweep into single points |
| `index::tests::a_curve_is_named_by_what_separates_it_and_nothing_else` | A legend repeating the width four times and never saying which table |
| `explore_index::the_grid_preset_reproduces_chart_grid_throughput` | The opening chart drifting from the page it claims to reproduce, on the real corpus |
| `explore_index::the_row_width_preset_reproduces_chart_row_size_ops` | The same for the width sweep, which is the other chart this was found through |

## Related

- [F30. Only comparable axes share a chart](../../features/plot-axis-units.md) — the change this
  item was fixed by, and the model behind it
- [F29. An explorer that draws more than one capture](../../features/benchmark-explorer.md) — where
  the defect was introduced, and the two doc comments that described a refusal that was never written
- [F18. Results pages that explain themselves](../../features/results-pages.md) — the families whose
  prose the explorer shows under a chart
