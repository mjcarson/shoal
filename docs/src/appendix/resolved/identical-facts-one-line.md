# 84. Two workloads the recorded facts cannot tell apart drew as one line

## Symptom

Ticking `macro/get_resident` and `macro/get_archived` in the explorer, on a sweep against the row
width, drew **one** line. Both workloads measure 200,000 rows of 256 bytes at a concurrency of
sixteen, so both sit at the same position on the axis, and the line joined them: a vertical segment
whose two ends are a get served from memory and a get served off disk — the pair the corpus keeps
precisely so that the difference between them can be read.

The legend said `f28-rearchive`, and nothing on the chart named either workload. The pair is not
special: any two workloads that set none of the facts `Index::curve_key` holds differently fold the
same way, which includes every ephemeral control against the persistent workload it is paired with
when the pair does not record a table kind.

This is [item 82](one-line-per-capture.md) again, in the case its fix does not reach.

## Cause

`Index::curve_key` places an arm on a curve by the facts a caller *set*, minus whichever one the
axis is reading — the width, the read share, the load depth, the client count, the row profile, the
key distribution, the table kind, and ten entries of server configuration. Every one of those is
read off `ScaleFactsLite`, which is a projection of the `ScaleFacts` a workload declares in its
`WorkloadPlan`.

`KeyedGet` is one workload struct with a `Residency` field:

```rust
fn id(&self) -> &'static str {
    match self.residency {
        Residency::Resident => "macro/get_resident",
        Residency::Archived => "macro/get_archived",
    }
}
```

and that field decides what the workload *does* — whether the partition it reads is in memory or has
to come off disk — while reaching no field of `ScaleFacts` at all. Both arms declare the same rows,
the same width, the same key count, the same concurrency, and `..ScaleFacts::default()` for
everything else. So both produce the same key, land on the same curve, and `Source::series` gathered
them into one `points` vector:

```rust
for (workload, at) in &placed {
    if *at != curve {
        continue;
    }
    ...
    points.push((key, self.value(point, &selection.metric)));
}
```

The vector was then sorted on the key, with ties left alone — which is exactly the sort
[item 82](one-line-per-capture.md) describes, on a smaller set. Nothing downstream could recover:
by the time the chart saw it, two measurements had already become two points of one line.

The curve model is not wrong. It answers *which curve is this arm on*, and it answers correctly:
these two arms differ in nothing it can see. What was missing is the second question — *would
drawing that curve as one line hide something* — which nothing asked.

## Evidence

**Established by reproduction.** Two tests were written against the unfixed tree, on a fixture whose
workloads 9 and 11 record identical facts:

```
---- index::tests::two_workloads_at_one_key_position_are_two_lines stdout ----
assertion `left == right` failed: the two workloads were folded into one line
  left: 1
 right: 2

---- index::tests::a_split_line_is_named_by_the_workload_that_separates_it stdout ----
assertion `left == right` failed
  left: ["one"]
 right: ["one · macro/grid/unsorted/r50/1024/write-only", "one · macro/write/insert_row/batched"]
```

`left: ["one"]` is the same line item 82's evidence opens with, and it means the same thing: the
only name the chart had for what it drew was the capture's.

The real corpus was then probed for whether the two keyed gets actually collide, rather than assuming
it from the source:

```
PROBE macro/get_resident -> ["rows handled per second", "wall clock", ..., "get p99", ...]
PROBE scale ScaleFactsLite { scale: "full", rows: 200000, row_bytes: 256, keys: 200000,
                             concurrency: 16, clients: None, read_pct: None, row_profile: None,
                             distribution: None, table_kind: None }
PROBE macro/get_archived -> ["rows handled per second", "wall clock", ..., "get p99", ...]
PROBE scale ScaleFactsLite { scale: "full", rows: 200000, row_bytes: 256, keys: 200000,
                             concurrency: 16, clients: None, read_pct: None, row_profile: None,
                             distribution: None, table_kind: None }
```

Identical in every field, which is why `explore_index::two_workloads_the_facts_cannot_tell_apart_are_two_lines`
asserts that equality before it asserts anything about the chart: the test is worthless the day
somebody gives one of them a distinguishing fact.

The nine tests over `Source::series` that already existed passed unchanged against the same tree.
None of them had ever selected two workloads that agree.

## The fix

[F32](../../features/chart-line-identity.md), in the half of it that is this item.
`Index::split_curves` runs between `curve_names` naming the curves and `Source::series` drawing them.
For each curve it takes its members' positions on the sweep axis and asks whether two of them are
equal:

- **They are not** — the ordinary case, and every genuine sweep. The curve is carried through whole
  as one line, named by whatever separates it from the other curves.
- **They are** — the curve is split into one line per workload, each named by the qualifier the curve
  already had plus the workload's identifier, which is the only thing separating them. The chart
  draws `f28-rearchive · macro/get_resident` beside `f28-rearchive · macro/get_archived`.

An arm with no position on the axis at all collides with nothing: it draws nothing either way, and
treating it as a collision would split curves that have no problem.

## Alternatives rejected

**Adding the workload identifier to `CurveKey`.** The obvious fix, and it breaks both invariants
[item 82](one-line-per-capture.md) states. *A curve key must not hold a fact that co-varies with the
axis* — a width sweep's sixteen arms have sixteen identifiers, so the key would give it sixteen
curves of one point and no line at all, which is the exact trap that item's fix walked into once with
`rows`. And *every key built in one call is the same length* — only some curves need the entry, so
either every curve carries one and the diff in `curve_names` names it everywhere, or the keys stop
lining up positionally.

**Splitting whenever a curve holds more than one workload.** Simpler to write and it is the
*one line per workload* reading of the problem. It destroys the sweep: three widths of one grid
family are three workloads and one curve, and that curve is the thing F30 was built to draw. The
collision is the honest test because it is exactly the condition under which drawing one line hides
a measurement.

**Giving `KeyedGet`'s residency a field on `ScaleFacts`.** It would fix this pair and nothing else,
and it would be a lie about what `ScaleFacts` is: how much data a workload built and how hard it drove
it, not which internal path it took. Two workloads sharing every scale fact is a legitimate state and
the chart has to survive it.

## Invariants to uphold

**The split stays downstream of the key.** `curve_key` answers *which curve is this*; `split_curves`
answers *would drawing that curve hide anything*. Merging them re-opens both of item 82's invariants.

**A curve is split entirely or not at all.** Splitting only the colliding members would leave a
partial curve whose name no longer says what separates it from the pieces beside it.

**An absent position is not a collision.** `collides` skips `None`. An arm with no position on this
axis is not on this chart, and pairing two of them would split a curve for a reason a reader could
not see.

**A split line is named by the identifier.** It is the only thing that separates it, and it is the
key every comparison in this repository joins on. Anything shorter is a name that does not identify.

## Still open

Nothing from this item. The neighbour it touches is recorded rather than fixed: a split line's legend
entry is the whole workload identifier with no common prefix stripped, so two split arms of one grid
family read as two long strings differing in their last segment. Filed as a limitation on
[F32](../../features/chart-line-identity.md) rather than in [todos](../todos.md), because it is
cosmetic and local to one function.

## Tests

| Test | What breaks without it |
|---|---|
| `index::tests::two_workloads_at_one_key_position_are_two_lines` | The fold returning: two measurements at one axis position drawn as one line |
| `index::tests::a_split_line_is_named_by_the_workload_that_separates_it` | Two lines with one legend entry between them, which is item 82's `["one"]` |
| `index::tests::row_count_does_not_split_a_width_sweep` | The split reaching a genuine sweep and shattering it into single points |
| `explore_index::two_workloads_the_facts_cannot_tell_apart_are_two_lines` | The same on the real corpus, and the premise that the two keyed gets still agree |
| `explore_index::the_grid_preset_reproduces_chart_grid_throughput` | The split reaching the opening chart, on the real corpus |
| `explore_index::the_row_width_preset_reproduces_chart_row_size_ops` | The same for the width sweep, which is the case the split must not touch |

## Related

- [F32. A colour per table, and a chart that frames itself](../../features/chart-line-identity.md) —
  the change this was fixed by
- [82. Every selected workload was folded into one line per capture](one-line-per-capture.md) — the
  same defect in the case its fix did reach, and the two invariants that decided where this one goes
- [F30. Only comparable axes share a chart](../../features/plot-axis-units.md) — the curve model,
  and why `rows` and `keys` are kept out of its key
