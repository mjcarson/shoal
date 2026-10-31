# 347. An encode step of small rows held its loop past 100 µs

## Symptom

On M23's quick lab pass (2026-10-31), scans of `Keyword` held their shards' loops past W12's 100 µs a
step ([W12](../../future-work/scans/performance.md#the-budgets)). `Keyword` holds 171 bytes a row and
16.7 rows a partition. Europa's scans passed the bound in 42 steps, hyperion's in 118 and titan's in
160, the longest 151, 366 and 477 µs. Scans of `Big`, a row of a kibibyte to each partition, never
held a step past 24 µs.

## Cause

F126 bounds an encode step by bytes: at most 64 KiB of a partition's rows by the partition's own
count (`SortedPartition::piece`), and a first step takes a partition whole if it fits 64 KiB
([F126](../../features/encodes-in-steps.md)). A step's time is mostly per row, though. Each row is
cloned into the piece and encoded with rkyv. On a Zen1 host, encoding alone costs 80 to 280 ns a row
past its partition's (X17), and the copy costs about as much again. 64 KiB of `Keyword`'s rows by
that count is about three hundred of them, 200 to 500 µs of a Zen1 core.

X17 timed stepped encodes of whole partitions, 13 µs each, but never cut a partition into pieces of
64 KiB ([X17](../../future-work/scans/page-rebuilt.md#a-page-captured-against-planned)). So the byte
bound's hold on small rows was first measured at the exit.

## Evidence

**Reproduced**, on the lab and in process. In the quick pass's captures, steps past 100 µs came only
in the intervals where the scan class copied resident bytes. In one two-second sample, 89 of titan's
176 steps passed the bound while 4.3 MB was encoded
(`target/lab/m23/quick/captures/m23-r1-keyword`). `an_encode_step_takes_the_rows_its_time_holds`
(below) says a row costs 5 µs and counts the steps a 400-row partition takes. Against the unfixed
tree:

```text
7 steps for 400 rows
```

## The fix

A step is bounded by rows as well as bytes. `PieceAsk::rows` is the most rows it encodes, and a first
step takes a partition whole only if it fits in rows too.

Each shard learns what a row of each table costs its encode steps (`EncodeCosts`,
`shoal-core/src/server/shard/walk.rs`). A step of at least eight rows says it: the first outright,
and each after it a quarter of the way. A step slowed by something else counts at most four times
what was learned, so one preempted step moves the cost at most 1.75 times.

A step takes as many rows as `ENCODE_TIME`, 50 µs, half the budget, holds at that cost; before the
table's steps have shown a cost it takes 32. `Captured::Piece` carries how many rows it took, and a
whole partition its live rows, which the shard learns from. `faults::fix_encode_row_cost` sets a cost
for a test.

## Alternatives rejected

- **A smaller byte bound**: a row's cost is mostly per row, so a bound small enough for `Keyword`'s
  rows would take `Big`'s kibibyte rows in many more steps than their time needs.
- **A fixed number of rows a step**: how many rows a step holds depends on the host and the row. One
  number is too few for small rows on europa and too many for heavy rows on a Zen1 host.
- **A deadline inside the copy**: the encode after it cannot be cut, and a first step that takes a
  partition whole copies nothing to cut.
- **The encode moved into the scan queue**: the queue gives its core up only at a yield, so a long
  encode there holds the foreground just as long.

## Invariants to uphold

- **No encode step takes more rows than its table's steps have shown `ENCODE_TIME` holds, nor more
  than 64 KiB by the partition's count, and every step takes at least one row.**
- **A step's rows are learned from the step's own time on the loop**, never from its page's wait for
  it.
- **Only a step of enough rows teaches a row's cost**, so a step's lookup is never mistaken for one.

## Still open

- **A row whose own encode passes the budget is still a step of one row.**
- **A large partition of small rows takes more steps**, each a message through its loop
  ([O145](../optimizations.md#o145-a-planned-partitions-encode-step-is-a-message-through-its-loop-each)).
- **The bound does not hold every step of a large resident partition.** On M23's exit's rounds
  (2026-10-31), steps of `Keyword`'s scans alone passed 100 µs 90 to 208 times a capture on europa,
  whose resident partitions were handed out at about 20 KiB each, and whose shards were preempted
  least. Which step held its loop, a page's capture or an encode step, the counters do not say
  ([item 350](../known-issues.md#350-a-scans-steps-of-large-resident-partitions-hold-their-loop-past-100-µs)).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `an_encode_step_takes_the_rows_its_time_holds` (`shoal/tests/scan_encodes.rs`) | With a row said to cost 5 µs, a 400-row partition takes at least forty steps of at most ten rows; by its bytes alone it took seven |
| `an_encode_steps_rows_are_learned_from_its_tables_steps` (`shoal-core/src/server/shard/walk.rs`) | A table's steps take 32 rows until a step of eight or more says a row's cost. They then take as many as 50 µs holds, one slow step moves the cost at most 1.75 times, and quicker steps bring it down |
| `a_planned_partition_is_encoded_a_step_at_a_time` (`shoal/tests/scan_encodes.rs`) | F126's row, kept: a large resident partition is encoded in steps and handed out as a driver's capture hands it out |

## Related

- [F126, a planned partition encoded a step at a time](../../features/encodes-in-steps.md), whose
  bound this is
- [F123, a node's walk and scan counters](../../features/walk-and-scan-counters.md), whose step
  counts showed it
- [X17, a page rebuilt](../../future-work/scans/page-rebuilt.md), whose row costs these are
- [M23's exit](../../future-work/scans/milestones.md#m23-the-walk-rebuilt-for-load), which found it
- [Item 346](paced-reads-alone.md), found by the same pass
