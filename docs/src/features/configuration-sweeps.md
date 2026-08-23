# F20. What each setting is worth

## Context

`shoal.yml` carries a dozen tuning knobs. [Configuration](../getting-started/configuration.md)
describes every one of them, in 375 lines, accurately. It describes what each setting **is**. It has
never said what value to pick, and could not, because nothing in this repository had ever measured
one: every capture ran against the same committed file, so the configuration was a constant rather
than an axis.

That left two holes, and they are different sizes.

The small one is advice. A caller deploying Shoal chooses `latency_sensitive.write_behind` from a
doc comment that says it is "the number of write behind buffers to use", which is true and is not a
basis for choosing 8 over 128.

The large one is that **a bottleneck living in a setting was invisible to the whole harness**. The
micro layer measures functions, the macro layer measures paths, and the two attribution layers
measure where inside a query the time goes — all of them against one configuration. If the reason a
workload is slow is that the intent log flushes at 512 bytes, no layer would ever say so, and the
optimization list would fill up with entries about code.

[todos](../appendix/todos.md) had been asking for one arm of this since [F8](purpose-built-workloads.md):
an `Async` against `Fsync` capture, "the cheapest remaining item on this page", buildable "on a
config change alone, with no code change". `ConfOverrides` already carried per-workload
configuration and `ConfFacts` already recorded it into the artifact. It was never run, and the
reason is worth naming: there was nowhere to put the answer. One number, on no page, comparable to
nothing.

## What it does

**Forty-eight workloads under `macro/conf/`**, each one the grid's reference cell —
`macro/grid/unsorted/r50/1024`, the persistent unsorted table at 1 KiB rows, an even read/write
mixture, thirty-two queries outstanding — with **exactly one field** of the server configuration
moved:

| Sweep | Values | Read shares | Arms |
| --- | --- | --- | ---: |
| `durability` | `fsync`, `async` | r50 | 2 |
| `latency_buffer` | 512, 4Ki, 16Ki, 64Ki, 256Ki | r50 | 5 |
| `latency_write_behind` | 1, 8, 32, 128, 512 | r50 | 5 |
| `intent_log` | 1Mi, 10Mi, 100Mi, 1Gi | r50 | 4 |
| `throughput_buffer` | 32Ki, 128Ki, 512Ki, 1Mi | r50 | 4 |
| `throughput_write_behind` | 1, 4, 16 | r50 | 3 |
| `shards` | 1, 2, 4, 8, 12 | r50, r100 | 10 |
| `memory` | 1Mi, 4Mi, 16Mi, 64Mi, 1Gi, 4Gi | r50, r100 | 12 |
| `frame` | 1Mi, 8Mi, 64Mi | r50 | 3 |

**~~Forty-eight~~ fifty-eight, since [F22](row-size-benchmarks.md).** Every sweep above runs at the
reference cell's **1 KiB** rows, and for one of them that is the one width whose answer is no: the
intent log stages records into a 4096 byte buffer and flushes when the next one will not fit, so at
1 KiB three records already share an aligned write and the sweep measures the flat side of a step.
`StreamWriter::prep` ~~stops batching entirely once a record exceeds the buffer~~ *stopped* batching
entirely once a record exceeded the buffer ([O34](../appendix/optimizations.md)), which is where the
setting's whole effect lived — until that entry was [built](self-sizing-staging-buffer.md), on the
strength of these very arms. See below.

| Repeat | Values | Read shares | Arms |
| --- | --- | --- | ---: |
| `latency_buffer` at 8 KiB rows | 512, 4Ki, 16Ki, 64Ki, 256Ki | r50 | 5 |
| `latency_buffer` at 64 KiB rows | 512, 4Ki, 16Ki, 64Ki, 256Ki | r50 | 5 |

**What those ten arms mean has since changed under them**, and it is worth knowing before reading a
future capture of them. They were built to adjudicate [O34](../appendix/optimizations.md); they did,
and the entry was then [built](self-sizing-staging-buffer.md). `latency_sensitive.buffer_size` is now
a **floor** under a `max_buffer_size` ceiling rather than the buffer size itself, so the knob these
ten arms move no longer decides how many records share a write once it is below the size the writer
would have chosen anyway. **The 64 KiB row rungs converged**, in `f23-staging-buffer`: the spread
across the five went 1.225× to **1.010×**, since all five now resolve to the same 256 KiB ceiling.
That is a sweep flattening because its knob stopped mattering, which reads identically to a sweep
that never mattered and is the reason this paragraph exists — anybody reading a future capture of
these ten arms and finding them flat should read it as the fix working, not as the setting being
inert. No identifier moved and no arm was added, so the join against `f22-row-size` was clean.

Identifiers are `macro/conf/<section>/<knob>/r<share>/<value>`, and the value is spelled the way
`shoal.yml` spells it — `4Ki`, not `4096` — because that is what a reader has to type afterwards.
A repeat at a width other than the reference one carries it as `w<width>` before the value:
`macro/conf/storage/latency_buffer/r50/w8192/4Ki`. The asymmetry is deliberate — an identifier is
the join key of every comparison, so adding a segment to the forty-eight that already existed would
have orphaned every capture taken before them.

**A knob at two widths is two sweeps.** `arms::conf_sweeps` keys on the row width as well as the
knob and the read share, and the page labels a repeat with its width. Merging them would put a 1 KiB
arm and an 8 KiB arm in one ladder and let the difference between two *widths* be read as the
difference between two *values of the setting*, which is the one thing this whole family exists to
prevent.

**`ConfOverrides` reaches the whole configuration.** It carried `shards`, `memory` and `tls`; it now
also carries the durability barrier, both writers' buffer size and write-behind depth, the intent log
size, and the frame bound. Every one is applied in `harness/conf.rs::resolve`, which remains the
single place a workload's configuration is produced.

**`ConfFacts` records all of it, for every workload.** Six optional fields joined `shards`, `memory`,
`durability` and `tls`, so a page groups arms by what was recorded rather than by parsing an
identifier — the [F18](results-pages.md) rule. They are `Option` with `skip_serializing_if`, so the
committed corpus keeps its bytes.

**A generated page with a gate.** [Configuration](../performance/configuration.md) carries the
sweeps, a per-arm table, and a *What the data says* table naming, per knob, the value that answered
a query fastest and the value that answered the most of them — which routinely disagree. Its last
column is **Real?**, and it is the point of the page: a knob whose fastest and slowest arms have
*overlapping* observed run intervals is reported as no measurable difference, however far apart its
medians are. Four charts: the best-against-worst spread of every storage knob, the write-behind
ladder, the shard scaling curve, and the memory cliff.

**A hand-written guide.** [Tuning](../operations/tuning.md) turns the table into advice per workload
shape, and cites the row each piece of advice rests on.

## Design choices

**An arm is the grid's reference cell, and reuses its driver.** A second driver measuring "the same
thing under two configurations" would differ in the driver as well as the configuration, and nothing
would be attributable to either. `Grid` gained a `conf: ConfOverrides` field and a `Sweep::Conf`
naming variant; `conf_sweep.rs` mints the arms and owns the knob table. The payoff is that every
configuration arm is directly comparable to a cell that already appears on
[Read/write mixtures](../performance/grid.md).

**One field moves, never two.** This is [F9](ephemeral-tables.md)'s control-pair shape applied to the
configuration, and `an_arm_moves_one_field` counts the moved fields of every arm against a defaulted
`ConfOverrides` — one term per field, so a field added without a term here fails to be checked and
that is visible in the diff.

**Every sweep contains the value `shoal.yml` is set to.** `durability/r50/fsync`,
`latency_buffer/r50/4Ki`, `shards/r50/12`, `memory/r50/4Gi`, `frame/r50/64Mi`. That arm is *measured*
rather than borrowed from the grid cell it duplicates, because it has its own identifier, its own
storage subdirectory and its own port — a sweep read against an arm that ran somewhere else is not a
sweep. `every_sweep_covers_the_shipped_default` resolves the committed file and asserts the
bracketing, so retuning `shoal.yml` fails a test rather than quietly leaving the sweep short of the
value in use.

**The section is a path segment.** `macro/conf/storage/...` and `macro/conf/resources/...` rather
than a flat `macro/conf/<knob>/...`. Neither `groups.rs` nor `render/family.rs` can link the engine,
so neither can see the `Section` enum; putting it in the identifier makes splitting the sweep a
prefix check in both, which is what every other family already is.

**The resource knobs are swept at two read shares and the storage knobs at one.** More shards is more
parallelism for reads and more fsync contention for writes, and a memory limit only bites once a read
has to reach disk — so a mixture alone would blend the two effects the sweep exists to separate. The
writer knobs get no read-heavy repeat because at `r100` they are not on the path at all.

**A recommendation is gated on interval disjointness.** The macro layer's existing rule
([F7](bench-runner.md)), not a new one: the frozen baseline spread 10.5% over five identical runs, so
a flat percentage threshold would either swallow real movement or report that spread as movement. A
tuning page without this gate is a table of noise with nine confident recommendations in it.

**The throughput writer is swept even though it barely reaches the engine.**
[Item 71](../appendix/known-issues.md) records that `throughput_sensitive` is applied to the archive
*map's* intent log and not to the archive writers, which are built with glommio's defaults. A flat
line on those two sweeps is that item's evidence — measured rather than argued from reading the
source — and the page says so rather than leaving a reader to conclude the knob does nothing.

## Alternatives rejected

**A second workload family with its own driver.** Rejected above: it would have made the arms
incomparable to the grid cell that is their reference, for no gain.

**Sweeping two knobs at once.** The full cube over nine knobs is thousands of arms. The cross is 46.
This is the same trade [F17](workload-grid.md) makes and it has the same cost, stated in the same
place: an *interaction* is invisible — a write-behind depth that only pays off at a large buffer
shows here as two flat sweeps. Recorded in [todos](../appendix/todos.md).

**Sweeping the storage knobs at `r0`.** A pure-write mixture would show the writer knobs at their
loudest. It would also make every storage arm incomparable to the grid's reference cell, which is
the anchor the whole family is built on. The r50 arms are asked half a question and are readable next
to everything else on the site; that is the better half of the trade, and the cost is on the family's
*what would make it wrong* block.

**Recording the swept knob in `ScaleFacts` instead of reading it off the identifier.** [F18](results-pages.md)
says selection reads facts, not identifiers, and this is the exception that proves the rule: the
*value* a knob was set to is a fact and is read as one, but which sweep an arm belongs to is its
identity. Two arms can resolve to the same configuration — `latency_buffer/r50/4Ki` and the value
`shoal.yml` already sets — and only the identifier distinguishes them.

**Making `list --groups` estimate from the recorded wall clocks directly.** The measured phases of
every macro workload come to about two and a half minutes; the capture they come from takes four and
a half hours. Printing the sum as a duration would be wrong by two orders of magnitude, so the
listing prints a *share* and projects it onto a named constant instead.

## Limitations

**No interaction is measured.** One knob at a time, as above.

**The results are a property of this machine and this device.** A buffer size is rounded up to the
O_DIRECT alignment of the disk underneath it, and the shard curve is a curve in one box with a fixed
core count. Nothing here transfers to different hardware as a number; the *shapes* might.

**The client shares the machine with the server.** A one-shard arm leaves eleven cores idle for a
client that needs four and a twelve-shard arm does not, so the low end of the shard sweep runs under
less contention than the high end and the curve flatters the small configurations.

**The memory sweep brackets one working set, and only one.** `resources.memory` is a **per-shard**
budget, and the reference cell seeds 20,000 rows of a kilobyte — 19.5 MiB over twelve shards, about
1.6 MiB each, plus roughly half as much again from the writes the run issues. The rungs are chosen
against that: `1Mi` and `4Mi` sit either side of it and the rest are flat. **That is a sweep of this
workload's cliff, not of the setting**, and a deployment whose working set is a hundred times larger
has its cliff a hundred times further along. The number to take off the chart is the *ratio* of
limit to working set at which reads start reaching disk, not the byte count. If no rung steps at
all, the page says the working set fit rather than that the setting does not matter.

**An arm that evicts is measuring compaction too.** A partition cannot be evicted until its
generation has been compacted, which is the same confound
[`ServerNeed::RestartAfterSeed`](purpose-built-workloads.md) exists to avoid on the read workloads.

**Nothing checks that a recommendation is still true.** The page is regenerated from whatever was
captured; if `shoal.yml` changes to a value the sweep says is worse, no test notices. Only the
*bracketing* is tested.

## Invariants to uphold

**An arm moves exactly one field of `ConfOverrides`.** Everything on the page rests on this. A new
knob adds a term to `fields_moved` in `conf_sweep.rs`'s tests as well as a variant to `Setting`.

**Every sweep contains the value the committed `shoal.yml` resolves to.** Otherwise the page
recommends against a reference that is not on the chart.

**Identifiers are append-only.** A workload's position in `workload_ids::IDS` decides its TCP port,
so a value inserted into the middle of a sweep re-ports every arm after it, and a *renamed* arm
orphans every capture taken before the rename. Add and deprecate.

**A recommendation stays gated on disjointness.** Removing the *Real?* column, or reporting a
verdict when it is `false`, turns this page into a generator of confident noise. It is the only
thing standing between a 10.5% run-to-run spread and nine recommendations.

**The new `ConfFacts` fields stay optional with `skip_serializing_if`.** Un-skipping them
re-serializes the whole committed corpus.

**`seed_batch` sizes bundles from the configured frame bound, not the constant.** It reads
`ConfFacts::max_frame_bytes` off the context. Sized against `DEFAULT_MAX_FRAME_BYTES` instead, the
`frame/r50/1Mi` arm would build bundles a server that accepts 1 MiB refuses outright — a workload
that cannot run rather than one that runs slowly.

## Performance

Nothing on the measured path changed. `ConfOverrides` gained seven `Option` fields that are read
once at server startup, and `ConfFacts` gained six that are written once at the end of a run.

The capture cost did change, and knowingly: 48 arms is roughly an hour and a half on top of a four to
five hour capture, which is why [F21](benchmark-groups.md) was built in the same change.
`--group conf/storage` re-measures the writer knobs alone.

The numbers this family produces are not yet in this repository — the change that adds a workload
moves every workload's source fingerprint, so the first capture containing them is also the first
capture that describes the current code.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `conf_sweep::every_arm_is_minted_exactly_once` | Two arms claim one artifact key and one port, and the second silently wins |
| `conf_sweep::an_arm_moves_one_field` | An arm moves two settings and the difference is attributable to neither |
| `conf_sweep::every_sweep_covers_the_shipped_default` | A sweep stops bracketing the configuration everything else is measured under |
| `conf_sweep::an_arm_is_the_reference_cell_with_one_setting_moved` | An arm stops being comparable to `macro/grid/unsorted/r50/1024` |
| `conf_sweep::an_id_names_its_section_and_its_value` | The section leaves the identifier and the group table can no longer split the sweep |
| `conf_sweep::every_value_names_its_own_knob` | The sweep table and its values disagree, filing an arm under the wrong sweep |
| `workload_ids::the_declared_ids_are_the_registered_ones` | A sweep is registered and never run, or run and never listed |
| `grid::a_seed_bundle_fits_inside_a_narrowed_frame` | The `frame` arms fail their seed against a server that refuses their bundles |
| `family::every_workload_has_a_family` | A configuration arm is captured and nothing on the site explains it |
| `pages::configuration::a_binary_size_parses_back` | The numeric sweeps plot at the wrong x positions, or not at all |
| `pages::configuration::a_categorical_value_is_not_a_number` | `fsync` lands on a numeric axis at a position that means nothing |
| `render::pages` determinism tests | The page differs between two renders of one artifact and `render --check` fails for no reason |

## Related

- [F21. Benchmark groups](benchmark-groups.md) — how a capture of one sweep is asked for
- [F17. The workload grid](workload-grid.md) — the reference cell, and the cross-not-cube argument
- [F8. Purpose-built workloads](purpose-built-workloads.md) — where the `Async`/`Fsync` arm was asked for
- [F7. The benchmark runner](bench-runner.md) — interval disjointness, which the *Real?* column is
- [F18. Results pages that explain themselves](results-pages.md) — the four blocks and the facts-not-identifiers rule
- [Configuration](../getting-started/configuration.md) — what each setting is
- [Tuning](../operations/tuning.md) — what to set it to
- [Item 71](../appendix/known-issues.md) — `throughput_sensitive` reaches less than its name suggests
