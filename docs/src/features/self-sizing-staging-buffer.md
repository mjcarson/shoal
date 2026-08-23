# F23. The intent log's staging buffer sizes itself

## Context

[O34](../appendix/optimizations.md#o34-a-record-wider-than-the-staging-buffer-defeats-intent-log-batching)
was the head of Tier A in the optimization queue — the only entry on that page whose cost was both
**measured** and **contained**. `f22-row-size` captured the `latency_buffer` sweep at three row
widths and settled it: **1.22×** at 64 KiB rows, `256Ki` against `4Ki`, on run intervals that do not
overlap, against 1.06× at the 1 KiB reference cell.

The same capture **corrected the entry's shape**, and that correction is the whole design of this
feature. The entry said the behaviour was a step at the buffer size: a record that fits shares a
write, a record that does not gets one to itself. The measurement says otherwise. At 8 KiB rows,
going from a buffer that cannot hold one record (`4Ki`) to one that holds two (`16Ki`) buys
**nothing** — 36,319 queries a second against 36,465, inside the noise. The gain arrives at `64Ki`
and `256Ki`, where 8 and 32 records share a write.

| Buffer | 1 KiB rows | 8 KiB rows | 64 KiB rows |
| ---: | ---: | ---: | ---: |
| `512` | 50,492 | 36,433 | 20,885 |
| `4Ki` | 53,285 | 36,465 | 18,537 |
| `16Ki` | 52,587 | 36,319 | 19,726 |
| `64Ki` | 53,222 | 38,419 | 20,535 |
| `256Ki` | 52,499 | 38,619 | **22,701** |

**What matters is how many records share an aligned write, not whether the record fits.** That makes
the obvious patch — raise the default to just past your widest row — worth nothing, and it is why
this is a sizing rule rather than a larger constant.

## What it does

`latency_sensitive.buffer_size` is now a **floor**, a new `latency_sensitive.max_buffer_size` is a
**ceiling**, and between them `StreamWriter` sizes each staging buffer to hold about eight of the
widest record the previous buffer held.

```yaml
storage:
  default:
    filesystem:
      latency_sensitive:
        buffer_size: 4096          # the floor, unchanged
        max_buffer_size: "256KiB"  # new, and this is its default
```

The rule is one function, `staging_target`, in
`shoal-core/src/server/tables/storage/fs/stream.rs`:

```rust
pub fn staging_target(widest: usize, floor: usize, ceiling: usize) -> usize {
    // aim for a buffer several records wide so they share one aligned write
    let batched = widest.saturating_mul(TARGET_RECORDS_PER_BUFFER);
    // a ceiling below the floor is a misconfiguration, and the floor is the value somebody set
    let ceiling = ceiling.max(floor);
    // hold that aim inside what was configured
    let bounded = batched.clamp(floor, ceiling);
    // but never below one record, since a record is never split across two buffers
    bounded.max(widest)
}
```

What that resolves to, against the shipped floor and ceiling:

| Row | Buffer before | Buffer now | Records per write |
| ---: | ---: | ---: | ---: |
| 1 KiB | 4 KiB | 8 KiB | 4 → 8 |
| 8 KiB | one record | 64 KiB | 1 → 8 |
| 64 KiB | one record | 256 KiB | 1 → 4 |
| 4 MiB | one record | one record | 1 → 1 |

`prep` asks for `staging_target(size)` instead of `max(default_buffer_size, size)` and records the
record's width; `write` hands that width to the next buffer and starts measuring again. The five
other places that flushed at `default_buffer_size` — `consume`, `sync`, `sync_blocking`, `refresh`
and `close` — ask for the same target, so a rotation does not make the writer relearn a width it
already knew.

Setting `max_buffer_size` equal to `buffer_size` turns the sizing off and gives back exactly the old
allocation, `max(default_buffer_size, size)`. That is both the escape hatch for an operator with a
memory budget and the control the tests are written against.

## Design choices

- **Eight records, and it is a `const` rather than a knob.** `TARGET_RECORDS_PER_BUFFER = 8` is read
  straight off the capture: two records buy nothing over none, eight buy 6% at 8 KiB rows, and
  thirty two buy nothing over eight. A knob nobody has a reason to move is config surface that has
  to be documented, swept and defended forever.
- **The ceiling is a knob, because it is the memory bound.** A writer may hold up to
  `write_behind + 1` buffers of this size at once, per table, per shard, and `write_behind` defaults
  to 128. That is a number an operator has to be able to bound, and unlike the target it is a
  genuine tradeoff rather than a measured answer.
- **Sized from the last buffer, not from a high-water mark.** `widest_staged` describes the buffer
  being filled and is reset on every flush; `widest_flushed` carries it to the next one. So one
  outlier record costs one oversized buffer and then decays, instead of pinning every buffer after
  it wide for the life of the process.
- **A record wider than the ceiling still gets a buffer of its own.** `staging_target` ends in
  `.max(widest)`. Records are never split across buffers — the reader frames whole records and the
  writer has no continuation — so this is a correctness property of the log's framing that no
  configured ceiling may override.
- **`shoal.yml` is not touched.** The default resolves to 256Ki either way, and the committed
  benchmark config is the record of what the baseline measured: editing it invalidates every number
  in [Baseline](../performance/baseline.md) and moves the value
  `conf_sweep::tests::every_sweep_covers_the_shipped_default` brackets.
- **`shoal-bench` is not touched either.** No workload, no `ConfOverrides` field, no `ConfFacts`
  field. Any of those would move the source fingerprint of every workload and orphan the very F22
  arms this is measured against. The benchmark that adjudicates this feature is
  `macro/conf/storage/latency_buffer/r50/{,w8192/,w65536/}*`, unchanged, and it has to stay that way
  to be joinable with `f22-row-size`.

## Alternatives rejected

- **Raise the default `buffer_size`.** The obvious patch, and the measurement says it buys nothing:
  at 8 KiB rows a buffer holding two records is worth 0.4% *less* than one holding none. A default
  sized "just above the widest expected row" is exactly the case the capture measured as flat.
- **Make records-per-buffer configurable.** The capture already answered it, and a sweep of a knob
  whose answer is known is a capture spent on a flat line.
- **A monotone high-water mark of the widest record ever seen.** Simpler by one field, and it makes a
  single wide record permanent: a table that takes one 4 MiB row and then a million 200-byte rows
  would allocate a 4 MiB DMA buffer per flush forever.
- **Size the buffer from the row type at schema time.** The writer sees framed bytes — a size header,
  a checksum and an rkyv payload — and a table's rows are not a fixed width anyway, since `String`
  and `Vec` fields make the archived size a property of the row rather than of the type.
- **Bound the memory by total staged bytes rather than by one buffer.** A budget divided across
  in-flight writes is a second bound interacting with `write_behind`, which is the kind of
  two-knob interaction [F20](configuration-sweeps.md) explicitly cannot measure. A per-buffer
  ceiling is a number an operator can multiply out for themselves.

## Limitations

- **Only fifteen arms are captured.** `f23-staging-buffer` is the `latency_buffer` sweep at three
  widths and nothing else, so what is measured is this knob against this reference cell. The grid,
  the isolating pairs and the other five storage knobs all ran against the old writer and have not
  been re-taken; a full capture would say whether sizing the buffer changed anything they measure.
- **The generated pages do not draw that capture, and cannot.** `render` picks its current capture
  as the newest one carrying a micro layer, and a `--group` capture has none — so
  [Configuration and what each setting is worth](../performance/configuration.md) still reports
  18,537/s for the `4Ki @ 64 KiB` arm from `f22-row-size` while `f23-staging-buffer` says 22,619/s
  for the same identifier at the current commit. Found by rendering, filed as
  [item 77](../appendix/known-issues.md#77-a-macro-only-capture-can-never-reach-the-pages-it-was-taken-for).
  The numbers on *this* page are the newer ones.
- **Rows above the ceiling behave exactly as before.** A 4 MiB row still gets one write and one DMA
  allocation per insert. That is deliberate — it is where the memory cost of batching would be
  worst — but it means O34's mechanism is still fully in force above 256 KiB unless an operator
  raises the ceiling.
- **A workload alternating widths never settles.** Sizing reads the last buffer, so a table taking
  wide and narrow rows in an alternating pattern oscillates between two buffer sizes. It never
  behaves worse than the floor, but it never converges either.
- **The 4096 boundary is still unbracketed.** The width axis jumps 1024 → 8192 with the staging
  buffer at 4096 inside that gap, so the shape of the transition right at the old threshold is
  still not measured. Filed in [TODOs](../appendix/todos.md#the-row-size-axis).
- **The interaction with `write_behind` is unmeasured.** The worst-case memory is
  `(write_behind + 1) × max_buffer_size` per table per shard, and whether a deep queue of large
  buffers is better or worse than a shallow one is exactly the two-knob question the configuration
  sweep is a cross rather than a cube over.

## Invariants to uphold

- **`staging_target` never returns less than `widest`.** Records are never split across buffers; a
  return below the record width would hand `prep` a slice it cannot fill and corrupt the log's
  framing. `a_record_wider_than_the_ceiling_gets_its_own_buffer` is what fails if the `.max(widest)`
  is dropped for being redundant.
- **`usable()` keeps one alignment block of slack.** Buffers are allocated
  `align_up(usable, alignment) + alignment` so a partial flush always has room for its pad region
  and its sentinel. Sizing changed what `usable` is asked for, not that rule.
- **The ceiling is floored at `buffer_size` at build time.** A config with `max_buffer_size` below
  `buffer_size` must not shrink a buffer below the size somebody deliberately set. `build` takes the
  max of the two and `staging_target` takes it again, because the function is public and testable on
  its own.
- **`widest_staged` is reset on the flush path and only there.** It describes the buffer being
  filled. If it stopped being reset it would become a high-water mark, which is the alternative
  rejected above.
- **The `latency_buffer` sweep must keep bracketing `shoal.yml`.** The floor is still what that
  sweep moves, and `conf_sweep::tests::every_sweep_covers_the_shipped_default` still has to pass —
  which is why the committed configuration was left alone.

## Performance

**Captured, as `f23-staging-buffer`** — the fifteen `latency_buffer` arms at all three row widths,
taken on a clean tree at `57b44d7` with the `performance` governor, and joined against
`f22-row-size` with no identifier moved.

| Buffer | 1 KiB before | after | 8 KiB before | after | 64 KiB before | after |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `512` | 50,492 | 52,993 | 36,433 | 38,474 | 20,885 | 22,592 |
| `4Ki` | 53,285 | 53,156 | 36,465 | 38,497 | **18,537** | **22,619** |
| `16Ki` | 52,587 | 51,863 | 36,319 | 38,182 | 19,726 | 22,665 |
| `64Ki` | 53,222 | 52,551 | 38,419 | 37,768 | 20,535 | 22,813 |
| `256Ki` | 52,499 | 53,052 | 38,619 | 38,305 | 22,701 | 22,691 |

**The sweep converged, which is what a knob that stopped mattering looks like.** Across the five
rungs the spread went 1.225× → **1.010×** at 64 KiB rows, 1.063× → 1.019× at 8 KiB, and
1.055× → 1.025× at 1 KiB. Every rung is now within one percent of the rung that used to be the only
good one.

**For the shipped configuration — `buffer_size: 4096` — that is +22.0% at 64 KiB rows** (18,537 →
22,619, run intervals disjoint) and **+5.6% at 8 KiB rows** (36,465 → 38,497, disjoint). At the 1 KiB
reference cell it is −0.24%, intervals overlapping: no change, which is the important null. The
capture that settled O34 said that width is flat and it still is.

**Three shapes in that table are worth reading, because each is a prediction that held.** The
`256Ki` rung at 64 KiB rows did not move at all (22,701 → 22,691) — at that width the ceiling is
what sizing resolves to anyway, so there was nothing to change. The `64Ki` and `256Ki` rungs at
8 KiB rows did not move either (within noise, and slightly down), for the same reason: eight 8 KiB
records is 64 KiB, so those two rungs already held eight. And the `512` rung improved at every
width, because a floor below one record is where the old writer had the least to work with. **The
fix moved the bad rungs up to the good ones and left the good ones alone**, which is the signature
of a floor replacing a fixed size rather than of a general speedup.

The reproduction is `shoal/tests/intent_log_batching.rs`: one shard, a table with 8 KiB rows, one
bundle of 128 inserts arriving in a single frame, and then the shard's intent log read off disk.
Against the tree before this change:

```
128 rows were written in 128 flushes, which is one record per write
```

128 records, 128 DMA writes, 128 `alloc_dma_buffer` calls, and 1,114,112 bytes of log. After:
**16 flushes**, eight records each, and 1,056,768 bytes. Eight times fewer writes and eight times
fewer allocations for the same data, and 5.1% fewer bytes on disk because the per-record pad regions
are gone — every flush now lands block aligned on its own.

The write count is the point rather than the byte count. Each of those writes is what the group
commit in `start_sync` has to amortize an `fdatasync` across, and a buffer holding one record leaves
it nothing to group.

**What the capture was predicted to say**, written down before it was taken: the five
`w65536` rungs all resolve to the same 256 KiB ceiling, so they should converge at or above the
22,701 that `256Ki` alone reached; the `w8192` rungs should converge near 38,619; and the 1 KiB
reference cell should not move beyond a couple of percent. All three held — 22,592–22,813,
37,768–38,497, and −0.24%. Recorded because a prediction that is only written down afterwards is
not one.

**What it is still not:** a capture of anything but these fifteen arms. Nothing here says what the
sizing does to a workload that is not the reference cell with one knob moved, and in particular
nothing measures it against `write_behind`, which is the other half of the memory this feature
spends.

```bash
cargo run -p shoal-bench --release -- run --label <label> --group conf/storage latency_buffer
cargo run -p shoal-bench --release -- compare <label> --against f23-staging-buffer --layer macro
```

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `stream_tests::staging_target_batches_eight_records` | A kilobyte row gets an 8 KiB buffer and an 8 KiB row a 64 KiB one — the sizing rule itself |
| `stream_tests::staging_target_never_drops_below_the_floor` | A 64 byte record, and a writer that has staged nothing, still get the configured floor |
| `stream_tests::staging_target_is_bounded_by_the_ceiling` | Eight 64 KiB records is half a mebibyte, and a 256 KiB ceiling is honoured |
| `stream_tests::a_record_wider_than_the_ceiling_gets_its_own_buffer` | Records are never split — a 4 MiB record gets 4 MiB whatever the ceiling says — and a ceiling below the floor shrinks neither |
| `stream_tests::a_ceiling_at_the_floor_is_the_old_behaviour` | The escape hatch reproduces `max(default_buffer_size, size)` at nine widths either side of the floor, which is also the definition of what this replaced |
| `intent_log_batching::wide_records_share_an_aligned_write` | A bundle of 128 rows wider than the buffer lands in at most a quarter as many writes. **This is the reproduction** — it fails on the tree before this change with one write per record |
| `intent_log_batching::a_ceiling_at_the_floor_batches_nothing` | The control, differing in one configuration field: a pinned ceiling gives back exactly one write per record, so the test above is measuring the sizing and not something else that happens to batch |
| `conf_sweep::tests::every_sweep_covers_the_shipped_default` | The `latency_buffer` sweep still brackets what the committed `shoal.yml` resolves to, which is what leaving that file alone protects |

## Related

- [O34](../appendix/optimizations.md#o34-a-record-wider-than-the-staging-buffer-defeats-intent-log-batching) — the entry this closes, and the capture that corrected its shape
- [F22](row-size-benchmarks.md) — the benchmarks that made it adjudicable, and the numbers above
- [Row size and what it costs](../tables/row-size.md) — the page that asked for them
- [The Intent Log](../storage/intent-log.md) — the staging buffer, the pad regions and the durability barrier
- [F20](configuration-sweeps.md) — the sweep that moves the floor, and why it cannot see a two-knob interaction
- [F9](ephemeral-tables.md) — the control-pair shape the integration tests borrow
- [Tuning](../operations/tuning.md#if-your-rows-are-wide) — what an operator should now set, which is less than it was
- [Configuration](../getting-started/configuration.md#storage) — the two fields
