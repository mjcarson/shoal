# F31. A metric a workload can actually answer

## Context

[F29](benchmark-explorer.md) made the metric a control, and [F30](plot-axis-units.md) decided which
workloads may share a chart. Neither asked the question underneath both: **does this workload carry
this number at all?**

`Index::units` built its `AxisUnits` from the metric's *unit* and the measurement's scale and
timing. Nothing in it read the measurement. So a workload was judged comparable on an axis it had no
value for, `refusal` and `reconcile` — F30's two gates — both waved it through, and the absence
surfaced only as a curve that was never drawn.

The metric list made it worse rather than better. `app::metrics` built the five whole-workload
numbers plus **every operation name seen anywhere in the corpus** crossed with seven ranks: thirty
three entries, unnarrowed by anything, most of them dead for whatever was ticked. Finding the live
one was guesswork, which is how this was reported.

The corpus says the two questions are different questions:

| Metric | Measurements carrying it, of 1521 |
|---|---|
| `rows_per_sec`, `wall_clock_ns`, `spread_pct` | 1521 — universal |
| `ops_per_sec`, `bytes_per_sec` | 785, missing for **88 workloads** |
| latency, by operation | `read`+`write` 148 workloads · `get` 85 · `read` 75 · `write` 64 · `insert` 2 |

So three metrics are universal, two are absent for half the corpus, and the latency metrics
partition it outright. A workload can answer at most fourteen of the twenty-eight latency entries
and most answer seven.

`Metric::OpsPerSec` is the default metric **and both presets' metric**, and the eighty-eight
workloads that counted no queries — every `encryption`, `fanout`, `transport`, `macro/get_*` and
`macro/insert_*` arm — were offered on it, ticked cleanly, and drew nothing. That is
[item 83](../appendix/resolved/default-metric-half-the-corpus-cannot-answer.md), resolved here.

## What it does

**A metric is offered only where something can answer it, and a workload is offered only where it
answers the metric.** Three changes, of which the first is the whole mechanism.

### One question, asked in one place

`Index::units` now reads the measurement before it will call it comparable:

```rust
let scale = self.scales.get(point.scale as usize)?;
// a measurement carrying no value for this metric has no units on this chart. an encryption arm
// counted no queries, so it has nowhere to sit on a queries a second axis - which is not the
// same as sitting there at zero
self.value(point, metric)?;
```

`anchor`, `reconcile`, `refusal`, `select_all`, `in_one_unit` and `Source::series` are all
metric-aware from that line alone, because every one of them already routes through
`workload_units`. There is no second rule to keep in step with the first.

Availability is read from a workload's **most recent** measurement, which is the rule
`workload_units` already followed for scale and timing, and it is followed here for the same reason:
an answer taken per capture would flicker as captures were ticked.

### The metric control is three lists, not one

`metric [ latency ▾ ]  of [ read ▾ ]  at [ p99 ▾ ]`

Six kinds — the five whole-workload numbers and `latency` — and the operation and the rank open only
underneath `latency`, because they are separate questions. No list is ever longer than seven.

Each list holds what `Index::metrics_for` says the ticked workloads can answer, which is the
**intersection** across them. A union would offer a metric only half the selection carries, and
drawing that is exactly the failure this feature exists to end — at the one place a reader cannot
tell an absent measurement from a slow one. With nothing ticked the answer is the whole corpus,
because nothing has been committed to yet, and `clear selection` is how a reader gets back there.

Changing the operation keeps the rank when the new operation has it: choosing which operation to
read is not a request to change which rank of it.

### The picker shows what can be drawn, and accounts for the rest

A workload that cannot be ticked is not shown, and a family with nothing left in it is not drawn at
all. Two things hide one — no value for this metric, or not in the units the chart is already in —
and the metric control fixes the first where `clear selection` fixes the second.

What replaces F30's per-row tooltip is arithmetic that always adds up: every family header carries
`(picked/shown, N hidden)`, and one line under the tree carries the total and both reasons. A tree
that just lost two thirds of its rows says so.

## Design choices

**Availability is a property of the measurement, not of a declared table.** The alternative was a
list of which metrics each family records, in `render/family.rs` beside the four blocks. It would be
wrong the first time a workload started or stopped counting something, and it would be wrong
silently — which is precisely how the operation names came to be `insert`/`get` in old captures and
`write`/`read` in current ones with nothing noticing.

**The intersection, not the union.** See above; the two other readings are in *Alternatives
rejected*, because both are defensible and one of them is what the request literally asked for.

**`metrics_for` lives in `index.rs`, not in `app.rs`.** `app` and `picker` are behind the `ui`
feature and `shoal-bench` enters the crate with `default-features = false`, so anything the corpus
tests need has to be outside it. This is the same argument F30 made for keeping `preset` outside
`ui`, and it is what lets `every_offered_metric_has_a_measurement_behind_it` run against the real
corpus rather than a fixture.

**`Explorer.metric` stays the single source of truth.** The kind, the operation and the rank are
derived from it for display and a whole `Metric` is written back on change, so there is no second
piece of state that can disagree with the chart about what is drawn.

**`ensure_metric_is_offered` exists for the two paths ticking cannot cover.** Ticking can only grow
the intersection, because the picker shows only workloads that answer the current metric. But
`clear selection` and a preset both move the set from underneath the control, and a combo box whose
selected text is not one of its own entries is a control lying about what is drawn. It says what it
did, the way `reconcile` says what it dropped.

**Hiding, rather than F30's disabling.** This is a reversal and it is recorded as one, on
[F30](plot-axis-units.md) where the old rule was argued. F30's reasoning — that a reader hunting for
a workload they know exists has nowhere to look — is a real cost and it is paid here; see
*Limitations*. What tipped it is that F30's rule was written when *units* were the only reason to
refuse, and units are changed by a distant act (`clear selection`), where a metric is changed by the
control immediately above the tree. Against three hundred and seventy four workloads, greying out
three hundred of them is not a tree a reader can read.

~~And it is the rule for anything the explorer cannot draw.~~ It is the rule for the **workload
tree** and deliberately not for the capture list: [F33](chart-readout.md) greys a capture that
carries no value for the metric rather than hiding it. Neither half of the argument above survives
the move. A capture is an identity a reader knows by *name*, out of twenty-seven of them, so one
disappearing reads as a corpus that lost a run rather than as a filter doing its job — and
twenty-seven rows all fit, so nothing is being made unreadable by keeping them. What carries across
unchanged is the arithmetic: the capture list totals what the metric is keeping off the chart, the
same way every family header and the line under the tree do.

## Alternatives rejected

**A union over the ticked workloads.** A metric any ticked workload carries stays offered, and
picking one drops the rest of the selection with a note, the way a units change already does. It
keeps mixed selections reachable. Rejected because the selection then shrinks under the reader as a
side effect of looking at a metric, and because the thing it protects — ticking a read arm and a
write arm together — is a comparison neither latency axis can express anyway.

**A union over the workloads currently visible.** What the request asked for literally: show
workloads sharing at least one available metric, and offer the union across them. It traps a reader.
With `read p99` chosen, `get` leaves the list entirely, and the only way back is to pick a
whole-workload metric first so that every workload becomes visible again. A control that has to be
escaped through an unrelated control is worse than a long list.

**Filtering only the latency entries.** The five whole-workload numbers are nearly universal, so
narrowing them looks like it buys nothing. It buys the eighty-eight workloads of item 83: they carry
no `ops_per_sec`, and that is the metric the explorer *opens* on.

**Keeping the flat list and just narrowing it.** Twelve to nineteen entries for a typical selection,
against thirty three. Better, and still a list whose length is a product of two independent choices.
The split makes the two choices two controls.

**Refusing to draw rather than refusing to select.** Rejected by F30 for units and rejected again
here for the same reason: the selection and the chart stop agreeing, and the reader has to read a
caption to find out that most of what they ticked is not on the chart.

## Limitations

**A hidden workload no longer says why it is hidden.** This is F30's guarantee, given up
deliberately, and it is the first thing to revisit if this proves wrong. The counts and the summary
line mean nothing vanishes unaccounted for, but a reader looking for one specific identifier is told
only that *n* rows are hidden for one of two reasons, not which reason applies to theirs. The
recovery is cheap — move the metric, or clear the selection — and that is the whole argument for
thinking the trade is right.

**Availability is judged by the newest measurement.** A workload that stopped recording an operation
between captures is judged by the newer captures, so an older capture's arm is unavailable on an
axis it was really measured on. No workload in the committed corpus does this. It is the same
limitation `workload_units` already carried for scale and timing, extended to one more field.

**The intersection can be reached from more than one direction.** Ticking A then B and ticking B then
A give the same offered set, but which *workloads* are offered along the way differs, because the
first tick sets the units. That is F30's *first ticked workload wins* rule and this does not change
it.

**A selection can still be narrowed to the three universal metrics.** Ticking arms from different
eras of the corpus — one recording `get`, one recording `read` — leaves the wall clock, the spread
and the row rate. That is correct, and it is not obvious from the interface *why* the latency
entries went away.

**~~Availability is a property of a workload.~~** Extended to captures by
[F33](chart-readout.md): `Index::captures_answering` asks the same question of a capture, judged
against the ticked workloads. The model is the one on this page; only the thing it is asked about is
new.

**Still only the macro layer.** Micro, hotpath and stages have no metrics model because they have no
model in the index at all. That is F29's limitation and this does not touch it.

**The interface still has not been seen.** This machine has no display, so the three combos, the
vanishing families and the summary line have been compiled for both targets and tested underneath,
and not looked at. Unchanged from F29 and F30.

## Invariants to uphold

**`Index::units` must read the measurement, not only its scale.** The `self.value(point, metric)?`
line is the entire mechanism. Every gate in the explorer routes through `workload_units`, so
deleting that line does not break one of them — it silently breaks all six at once, and the only
symptom is a curve that is not drawn.

**`metrics_for` is an intersection and returns the whole corpus for an empty selection.** Those are
two rules and both matter. An intersection that returned the empty set for an empty selection would
leave a reader with no metric to choose before they had ticked anything, and therefore nothing to
tick, because the picker filters on the metric.

**The offered list stays a subsequence of `corpus_metrics`.** `metrics_for` filters that list rather
than building its own, so entries cannot move under the cursor as boxes are ticked.
`the_offered_metrics_keep_the_corpus_order` is what notices.

**Every hidden row is counted.** Hiding is only defensible while the arithmetic adds up: the shown
count plus the hidden count, over every family, is every workload the filter matched. A future edit
that hides a row without incrementing `hidden` turns hiding into dropping, and takes the argument
above with it.

**`AxisUnits::differs_from` still returns the sentence, not a boolean.** F30's invariant survives.
The sentences now feed a count and a summary rather than a tooltip, and `refusal` still produces one
per reason — including the two new ones — so a workload cannot be hidden for a reason nothing names.

**`MetricKind` must stay a projection of `Metric`, never a second piece of state.** `Metric::kind`
is total and `Explorer` stores only the `Metric`. A `MetricKind` field on `Explorer` would be a
second answer to *what is on the value axis*, and the two would disagree the first time a preset set
one without the other.

## Performance

Nothing measured changed, so **no capture was taken**. No workload, no `shoal.yml`, no seed and
nothing under `shoal-bench/src/workloads/` was touched; CLAUDE.md excludes UI and renderer changes,
and `render --check` passes against the committed pages, which is the proof that none of them moved.

`workload_metrics` and `workload_answers` both go through `newest_point`, which the picker already
called once per workload per frame through `workload_units` — a reverse walk of the captures binary
searching each. `metrics_for` adds one `workload_metrics` per **ticked** workload per frame, and a
`contains` over a vector of at most thirty three short values per candidate. The index, its size and
the wasm bundle are untouched.

`INDEX_VERSION` does **not** move. `Metric`, `MetricKind`, `AxisUnits` and `Selection` are not
serialized, so nothing added here crosses to a stale browser bundle.

## Tests

| Test | What breaks without it |
|---|---|
| `index::tests::a_metric_a_measurement_does_not_carry_has_no_units` | The hole this closes: an arm judged comparable on an axis it has no value for |
| `index::tests::an_arm_that_never_recorded_an_operation_is_not_on_that_operations_axis` | The same, for the latency half, which is where the corpus actually partitions |
| `index::tests::the_offered_metrics_are_the_ones_every_ticked_workload_answers` | `metrics_for` becoming a union, so a pick draws half the selection as curves that are not there |
| `index::tests::nothing_ticked_offers_the_whole_corpus` | The intersection collapsing to the empty set before anything is ticked, leaving no metric to choose and so nothing to tick |
| `index::tests::the_offered_metrics_keep_the_corpus_order` | The list reshuffling under the cursor as boxes are ticked |
| `explore_index::a_workload_is_only_offered_on_a_metric_it_answers` | [Item 83](../appendix/resolved/default-metric-half-the-corpus-cannot-answer.md) returning, on the real corpus — this is the test that failed against the unfixed tree |
| `explore_index::every_offered_metric_has_a_measurement_behind_it` | A dead entry returning to the list, for any workload in the corpus rather than for the fixture's twelve |
| `explore_index::a_metric_list_narrows_to_what_the_whole_selection_answers` | The intersection holding on the fixture and not on the corpus it is a model of |
| `explore_index::the_presets_pick_arms_that_answer_their_own_metric` | Item 83 returning at the one place a reader has not chosen anything yet — the chart the explorer opens on |

## Related

- [F29. An explorer that draws more than one capture](benchmark-explorer.md) — the metric control
  this reshapes, and the crate it lives in
- [F30. Only comparable axes share a chart](plot-axis-units.md) — the gate this extends, and the
  *disabled rather than hidden* rule this reverses
- [Resolved 83. The metric the explorer opens on, that half the corpus cannot answer](../appendix/resolved/default-metric-half-the-corpus-cannot-answer.md)
  — the defect underneath, and what the unfixed tree did
- [F18. Results pages that explain themselves](results-pages.md) — the families the picker groups by
- [F33. The chart answers where you point, and the capture list says whether it can](chart-readout.md)
  — the same availability question asked of a capture, and the one place this page's *hidden* rule is
  deliberately not followed
- [Todos](../appendix/todos.md) — the two index sections that would bring the micro and attribution
  layers under this same rule
