# 13. Eviction logging can underflow

Filed as a panic waiting to happen. It is that, but the reproduction attempt turned up something
the filing got wrong, and the more useful half of the fix came out of the correction.

## Symptom

A shard evicts partitions, and the statement that reports how much memory the eviction freed
takes the shard down:

```
thread 'shard-3' panicked at .../persistent/sorted.rs:1079:
attempt to subtract with overflow
```

Nothing about the eviction failed. The partitions were dropped, the counter was updated, and then
the shard died describing what it had just done. In a release build there is no panic and no
signal either: the subtraction wraps, and the log reports having freed about 18 exabytes.

## Cause

Both persistent tables ended `evict` with the same line:

```rust
let post = *self.memory_usage.borrow();
event!(Level::INFO, pre, post, diff = pre - post, ...);
```

`.../persistent/sorted.rs:1079`, `.../persistent/unsorted.rs:850`

`pre` and `post` are two reads of `memory_usage`, the shard-wide counter shared by every table on
the thread (`shard.rs:292`). `pre - post` is a plain `usize` subtraction, so it is a claim that
the counter can only have gone down across the loop.

That claim is not the eviction's to make. The counter is an estimate that drifts by design —
`UnsortedPartition` uses two different size bases, sorted sizes are maintained by delta and never
recomputed, and sorted tombstones are uncounted ([item 22](../known-issues.md#22-size-accounting-inconsistencies)).
Every other site that touches it says so, using `saturating_sub` or `saturating_add_signed`; the
log statement is the one place that assumed instead.

The second cost is the one that survives the panic being latent. `pre - post` reports the
*counter's* movement, and the counter is exactly the thing that may be wrong. When it has drifted
low and floors at 0, an eviction that dropped a gigabyte reports having freed whatever was left on
the counter, and nothing anywhere says the two disagreed. This line is the only window onto shard
memory — [item 6](memory-accounting.md#evidence) could not reproduce its own defect against a live
server for precisely that reason — so the window both broke on bad input and hid the badness.

## Evidence

**Established by reading the source, and the expression reproduced directly.** The arithmetic was
extracted to `eviction_totals` in its filed form (`let reclaimed = pre - post;`) and the unit test
below run against it before any fix:

```
thread 'server::tables::persistent::tests::eviction_logging_cannot_underflow' panicked at
shoal-core/src/server/tables/persistent.rs:146:21:
attempt to subtract with overflow
```

`persistent.rs:146` is `pre - post`, the expression item 13 named.

**The filing's claim that it is reachable was wrong, and is worth correcting rather than quietly
dropping.** Item 13 read "any accounting drift leaving `post > pre` panics the shard … given the
accounting inconsistencies in item 22, this is reachable". It is not, on the current code. Every
mutation inside `evict`'s loop is `memory_usage.borrow().saturating_sub(partition.size())`, the
loop contains no `.await` so nothing else on the shard can interleave, and a saturating subtraction
cannot raise the counter. `post <= pre` holds for every input. No sequence of evictions, drifted
sizes, or floored counters produces the panic today.

So what was fixed is a latent panic and a live blind spot, not a live crash. Both are real: the
panic is one non-saturating edit to that loop away, in a statement whose failure has nothing to do
with the logic it reports on, and the blind spot is there on every eviction right now.

## The fix

**The arithmetic moved into a total function.** `eviction_totals`
(`.../tables/persistent.rs:144-166`) sits beside `adjust_memory_usage`, which
[item 6](memory-accounting.md#the-fix) added for the same reason — one home for arithmetic on the
shard counter, reachable from a unit test:

```rust
pub(crate) fn eviction_totals(pre: usize, post: usize, removed: usize) -> (usize, usize) {
    // what the shard counter actually moved by
    let reclaimed = pre.saturating_sub(post);
    // what the dropped partitions were accounted for beyond that, which is only
    // non zero when the counter had already drifted low and floored at 0
    let drift = removed.saturating_sub(reclaimed);
    (reclaimed, drift)
}
```

Both subtractions saturate, so the function is total for every triple of `usize` and no counter
state can panic or wrap a log line.

**Eviction counts what it dropped, not what the counter says.** Both `evict` methods
(`.../persistent/sorted.rs:1059-1092`, `.../persistent/unsorted.rs:830-863`) now accumulate the
size of each partition they actually removed — a number the loop already had in hand for the
decrement — and log it:

```rust
if let Some(partition) = self.partitions.remove(&victim) {
    // get the size this partition was accounted for at
    let size = partition.size();
    let decreased = self.memory_usage.borrow().saturating_sub(size);
    *self.memory_usage.borrow_mut() = decreased;
    // track what this pass freed independently of the shards counter
    removed += size;
}
```

The event carries `pre`, `post`, `removed`, `reclaimed`, and `drift` in place of `pre`, `post`,
and `diff`. `removed` comes from the partitions; `reclaimed` comes from the counter; `drift` is
the gap. A non-zero `drift` means the counter had already floored — the first direct reading of
item 22 that does not require attaching a debugger to a shard.

The decrement itself was left alone. It was already saturating and already correct, and the point
of the change is that the log stops making claims the loop never made.

## Alternatives rejected

**`diff = pre.saturating_sub(post)` and nothing else.** The one-word fix, and it is what the
filing implies. It cannot panic, but it answers the drifted case with `0` — the single reading
that most needs to reach a human is the one it erases. Given that this event is the only
observability the memory counter has, trading a crash for a silent zero is not obviously the
better failure.

**A signed diff via `cast_signed`.** `pre.cast_signed() - post.cast_signed()` shows the direction
of the anomaly, and the repo already uses `cast_signed` for size deltas
(`.../persistent/sorted.rs:326`). But it still overflows in principle, it reports "negative bytes
freed" — a quantity with no meaning — and it describes the counter's disagreement with itself
rather than the eviction's disagreement with the counter, which is the fact worth having.

**`WARN` or `debug_assert` when `drift` is non-zero.** Tempting, and it is what an invariant
violation would deserve. Drift here is not a violation: item 22 makes it ordinary, so the warning
would fire on healthy runs and the assert would fail test runs for a condition that is not this
code's defect. The same reasoning rejected an assert in [item 6](memory-accounting.md#alternatives-rejected).
It becomes the right change once item 22 is closed, and is recorded in
[Todos](../todos.md) rather than done here.

**Recompute `post` as `pre - removed` instead of re-reading the counter.** It makes the two agree
by construction, which is the problem — the log would then be internally consistent and unable to
show that the counter had drifted. The disagreement is the signal.

**Sweep the other counter sites onto helpers in the same change.** Eleven sites spell out
`saturating_add_signed` by hand. Item 6 deliberately left them, so that the diff is only the
defect; the same applies here.

## Invariants to uphold

- **Nothing in a log statement may be able to fail.** A shard that dies reporting a successful
  eviction is worse than one that never reported it. Arithmetic in an `event!` must be total —
  saturating, or computed by a function that is.
- **`eviction_totals` is total for every input, including nonsensical ones.** It takes three
  independent `usize` and orders none of them. Adding an ordering assumption to it, in any
  direction, reintroduces item 13 in the place it was moved out of.
- **`removed` is derived from the partitions, never from the counter.** It is the one number in
  the event that stays correct when the counter is wrong, which is the entire reason it is there.
  Computing it as `pre - post` would make all five fields say the same thing.
- **`evict`'s loop only ever decreases the counter, and saturates when it does.** This is what
  makes the panic latent rather than live, so it is load-bearing for the claim in *Evidence*, not
  merely for correctness. Any edit that can raise `memory_usage` inside that loop — or that
  introduces an `.await` into it, letting another table's accounting interleave — makes `post >
  pre` genuinely reachable and must be weighed as such.
- **The counter is still an estimate.** Nothing here makes it exact; it makes the inexactness
  visible. `drift` is a measurement, not a repair.

## Still open

- [Item 22](../known-issues.md#22-size-accounting-inconsistencies) is untouched — the drift this
  now reports is still there to report. `drift` is the instrument for closing it: a run under
  memory pressure that logs a persistently non-zero drift localizes the undercount to a table.
  Item 22 now names a first candidate to test against: recovery adds a partition to the counter
  in archive bytes and eviction takes it off in deep size, so a shard that recovered from a log
  naming many partitions should show drift proportional to how many, and one that started clean
  should show none. That is a sharper prediction than "some table under-reports" and it is
  falsifiable in one run.
- The eviction event is still the only window onto shard memory. There is no gauge for resident
  bytes, LRU depth, or eviction rate ([Memory and Eviction](../../tables/memory-and-eviction.md#limitations)).
- Promoting drift to a `WARN` once item 22 is closed is filed in [Todos](../todos.md).

## Tests

| Test | Fails without |
| --- | --- |
| `eviction_logging_cannot_underflow` (`.../tables/persistent.rs`) | The saturating form. `eviction_totals(600, 1000, 0)` panics with `attempt to subtract with overflow` instead of reporting nothing reclaimed. Its other two cases pin the ordinary pass and the floored-counter drift reading |
| `delete_survives_eviction`, `writes_survive_eviction` (`shoal/tests/persistent_sorted_table.rs`, `persistent_unsorted_table.rs`) | Nothing here directly — they are the only tests that drive `evict` under real memory pressure, so they are what says the `removed` accumulator did not change what eviction drops |

The unit test was confirmed to fail against the unfixed expression, with the output in *Evidence*,
rather than being written afterwards and assumed to cover it.

## Related

- [Memory and Eviction](../../tables/memory-and-eviction.md#eviction) — the eviction path and the log line
- [Memory accounting collapsed to zero on a partition load](memory-accounting.md) — the other defect on this counter, and the one whose reproduction this line blocked
- [Known Issues #22](../known-issues.md#22-size-accounting-inconsistencies) — the drift `drift` measures
- [Partitions](../../tables/partitions.md#sizes) — where the sizes `removed` sums come from
