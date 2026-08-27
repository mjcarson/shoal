# 83. The explorer opened on a metric that half its corpus could not answer

## Symptom

The explorer's metric box offered thirty three entries and gave no sign which of them the selected
workloads could answer. Picking the wrong one produced an empty chart with a strip over it saying
something that was not true, so the way to find the right entry was to try them.

The default was one of the wrong ones. `Metric::OpsPerSec` is what the explorer opens on and what
both presets set, and **eighty-eight of the three hundred and seventy-four workloads carry no
value for it** — every `macro/encryption`, `macro/fanout`, `macro/transport`, `macro/get_*` and
`macro/insert_*` arm. Ticking one of those:

- it was offered in the picker, undimmed, with no tooltip;
- ticking it succeeded, and it could set the units for everything after it;
- on a sweep it produced a series whose every point was `None`, so no line was drawn and the legend
  had no entry for it;
- on a timeline it was dropped inside `Source::series` with nothing said;
- and the strip above the chart read *"n of m points are absent — those captures did not measure
  that workload. They are drawn as gaps, never as zero."* The captures **did** measure it. It has no
  query counter, which is a different fact and points a reader at the wrong thing.

## Cause

`Index::units` decided comparability without reading the measurement:

```rust
pub fn units(&self, point: &MacroPoint, metric: &Metric, axis: Axis) -> Option<AxisUnits> {
    // a measurement whose scale row is missing cannot be placed on any axis, which is a
    // corrupt index rather than a state to draw
    let scale = self.scales.get(point.scale as usize)?;
    Some(AxisUnits {
        value: metric.unit(),
        key: KeyUnit::of(axis, scale),
        scale: scale.scale.clone(),
        timing: matches!(metric, Metric::Latency { .. }).then_some(point.timing),
    })
}
```

`metric.unit()` is a property of the *metric* — `OpsPerSec` is a query rate whether or not anything
counted queries — and `scale` and `timing` are properties of the measurement's facts rather than of
its numbers. Nothing anywhere asked `self.value(point, metric)`.

[F30](../../features/plot-axis-units.md) built two gates on top of that answer, `picker::refusal`
and `Explorer::reconcile`, and both inherited the hole. So did `in_one_unit`, `anchor` and
`select_all`. Six call sites, one missing question.

The metric list had the same shape of defect from the other direction. `app::metrics` built its
latency entries from **every operation name in the whole corpus** — `get`, `insert`, `read`, `write`
— crossed with seven ranks, and never narrowed them by what was ticked. A workload records at most
two of those four operations, so at least fourteen of the twenty-eight latency entries were dead for
anything a reader could have selected, and for most workloads twenty-one were.

## Evidence

Established by **reproducing it**, against the unfixed tree. This machine has no display, so the
reproduction asks `workload_units` — the function the picker calls — rather than driving the picker:

```
running 1 test
test a_workload_is_only_offered_on_a_metric_it_answers ... FAILED

---- a_workload_is_only_offered_on_a_metric_it_answers stdout ----
thread 'a_workload_is_only_offered_on_a_metric_it_answers' panicked at
shoal-bench/tests/explore_index.rs:354:5:
88 workloads are offered on a metric they carry no value for, starting with
["macro/encryption/clients/plain/1048576/1", "macro/encryption/clients/plain/1048576/2",
 "macro/encryption/clients/plain/1048576/4", "macro/encryption/clients/plain/1048576/8",
 "macro/encryption/clients/plain/256/1"]
```

The corpus counts behind it, over the 1521 committed macro measurements:

| Metric | Measurements carrying it |
|---|---|
| `rows_per_sec`, `wall_clock_ns`, `spread_pct` | 1521 — universal |
| `ops_per_sec`, `bytes_per_sec` | 785 |
| latency, by operation | `read`+`write` 148 workloads · `get` 85 · `read` 75 · `write` 64 · `insert` 2 |

## The fix

[F31](../../features/metric-availability.md). One line is the mechanism:

```rust
let scale = self.scales.get(point.scale as usize)?;
// a measurement carrying no value for this metric has no units on this chart. an encryption arm
// counted no queries, so it has nowhere to sit on a queries a second axis - which is not the
// same as sitting there at zero
self.value(point, metric)?;
```

All six call sites become correct at once, because every one of them already routed through
`workload_units`. On top of it the metric control became three lists narrowed to the intersection of
what the ticked workloads answer, and the picker hides what it cannot offer while counting it.

## Alternatives rejected

**Fixing the strip's wording.** The message at `plot.rs:84` is wrong for this case, and rewriting it
to cover both cases would have left a reader correctly informed that the chart they are looking at is
empty. The absence was the defect; the wording was how it surfaced. After the fix that message is
true again, because the only absences reaching a drawn series are genuine capture gaps.

**Declaring per-family metric lists** in `render/family.rs`, beside the four blocks. It would go
stale the first time a workload started or stopped counting something, and silently — which is
exactly how the operation names came to be `insert`/`get` in old captures and `write`/`read` in
current ones with nothing noticing.

**Making the presets pick a metric every arm answers.** It fixes the opening chart and nothing else,
and the opening chart was the symptom that happened to be easiest to see.

**Treating an uncounted rate as zero.** Never considered seriously and recorded so it stays that
way: it would put eighty-eight workloads on the axis at the origin, which reads as *measured, and
terrible*.

## Invariants to uphold

**`Index::units` must read the measurement, not only its scale row.** Removing
`self.value(point, metric)?` does not break one gate, it silently breaks six, and the only symptom
is a curve that is not drawn.

**Every metric accessor keeps returning `Option`, and every `None` stays a `None`.** F29's rule,
and this defect is what it looks like when the `None` is honoured at the point of reading and
ignored at the point of offering. The two have to agree.

**A reason a workload cannot be drawn is a sentence, and there is one per reason.** `refusal` now
distinguishes *never measured at all* from *measured, without this number*, because only the second
is fixed by changing the metric.

## Still open

**The micro, hotpath and stages layers are not in the index at all**, so nothing here applies to
them. When they are added ([todos](../todos.md)), a scope total needs a `Unit` and a `KeyUnit` of its
own so that this same rule refuses to put it beside a macro measurement — rather than a second rule
being invented for a second layer.

**A workload whose recorded operations changed between captures** is judged by its newest
measurement, so an older capture's arm can be unavailable on an axis it was really measured on. No
workload in the committed corpus does this. Same limitation `workload_units` already carried for
scale and timing.

## Tests

| Test | What breaks if the fix is reverted |
|---|---|
| `explore_index::a_workload_is_only_offered_on_a_metric_it_answers` | The defect itself, on the real corpus — this is the test whose failure is quoted above |
| `explore_index::the_presets_pick_arms_that_answer_their_own_metric` | The opening chart, which is the one nobody chose and so the one failure a reader cannot attribute to something they did |
| `explore_index::every_offered_metric_has_a_measurement_behind_it` | A dead entry returning to the metric list, for any workload in the corpus |
| `index::tests::a_metric_a_measurement_does_not_carry_has_no_units` | The missing question, on a fixture, which is where it can be stated rather than counted |
| `index::tests::an_arm_that_never_recorded_an_operation_is_not_on_that_operations_axis` | The same for the latency half, which is where the corpus actually partitions |

## Related

- [F31. A metric a workload can actually answer](../../features/metric-availability.md) — the fix,
  and the three lists that replaced the one
- [F30. Only comparable axes share a chart](../../features/plot-axis-units.md) — the gates that
  inherited the hole, and the rule this extends
- [F29. An explorer that draws more than one capture](../../features/benchmark-explorer.md) — where
  the metric list came from
- [Resolved 82. Every selected workload was folded into one line per capture](one-line-per-capture.md)
  — the neighbouring defect in the same function, with the same shape: a refusal that was documented
  and not performed
