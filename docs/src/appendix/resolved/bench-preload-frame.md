# 210. The bench preloaded wide rows in bundles no node accepts

## Symptom

`shoaladm bench` ([F66](../../features/dataset-benchmarks.md)) preloaded rows of a mebibyte or
more in bundles larger than a node accepts. A run asked for small bundles and was accepted, then
its preload sent up to sixty-four rows at a time, which for such rows is past the 64 MiB frame.
The client refused the frame, and the refused bundle's rows disappeared from the record
([item 211](../known-issues.md#211-a-bundle-refused-at-its-send-loses-its-queries-from-the-benchs-record)).
The preload reported success with fewer rows than the file held. The arms after it counted the
missing keys as misses, and the run finished with no error. A run of 100 rows of 1.1 MiB preloaded
36 of them and went on.

Whether it happened depended on timing. A bundle fills only when the file is read faster than
the cluster takes rows. On the lab's 1 GbE a release build reads several times faster than the
link carries, so [X3](../../object-storage/spikes.md#x3-bytes-through-the-tablet-groups), which
benchmarks rows of 1 MiB and 4 MiB through the tablet groups, would have met it on every run.
That is how it was found: checking what the object storage spikes need before they start
([S1](../../object-storage/prerequisites.md#the-order)).

## Cause

`orchestrate` (`shoaladm/src/bench/orchestrate.rs`) chose the preload's bundle like this:

```rust
let bundle = spec.bundles.iter().copied().max().unwrap_or(64).max(64);
```

The frame check that runs before the cluster exists judges only the run's own bundles. It
multiplies the file's mean row by the largest bundle and by `FRAME_HEADROOM` (4), and compares the
result with `MAX_FRAME_BYTES` (64 MiB). So a run at `--bundles 1` passes the check with any row
under 16 MiB, and the preload then sends sixty-four of them at once.

A worker in the driver (`shoal-loadgen/src/driver.rs`) fills a bundle with whatever the feed has
parsed, up to the bundle size. So the bundle reaches sixty-four only when the feed is ahead of the
cluster. The client refuses a body past the server's bound before it writes any of it
(`ProtocolError::PayloadTooLarge`, `shoal-proto/src/shared/protocol.rs`).

## Evidence

**Reproduced.** `wide_rows_preload_in_bundles_a_frame_carries` in
`examples/bench_dataset/tests/bench_run.rs` sets up the failure:

1. It writes a dataset of 100 items whose descriptions are each 1.1 MiB.
2. It runs `bench run --addr … --bundles 1 --preload 100%` against a node in process.
3. The node sits behind a relay that stops once, for five seconds, after the first MiB.

The stop is needed because a debug build parses these rows more slowly than a node on loopback
takes them, so without it no bundle fills. The first form of the test had no stop and passed on
the unfixed tree: four bundles, the largest about 54 rows and 59 MiB, before the file ran out.

Against the tree at `8b304a3`:

```text
preloaded 36 rows in 6.0s
[1/1] done: read 147/s p50 4.43ms p99 45.18ms misses 247 | bundle p50 2.40ms p99 44.99ms | …
test wide_rows_preload_in_bundles_a_frame_carries ... FAILED

thread 'wide_rows_preload_in_bundles_a_frame_carries' panicked at examples/bench_dataset/tests/bench_run.rs:314:5:
Some(WindowSummary { secs: 6.008283678, read: KindSummary { ok: 0, … }, insert: KindSummary {
ok: 36, per_sec: 5.99172774278585, … errors: {}, … }, … })
```

The run's log names no failure. A print added to the driver's error path for one run, then
removed, showed the refusal with sixty-four queries staged and none left in the buffer:

```text
Protocol(PayloadTooLarge { len: 73824924, max: 67108864 }) staged 64 buffer 0 outstanding 0
```

With the fix the preload loads all 100 rows in bundles of at most fourteen, and the test passes
in eleven seconds.

## The fix

**The preload's bundle is judged against the frame by the rule a run's bundles already are.**
`preload_bundle(bundles, widest_row)` returns the larger of two values:

- the run's largest bundle, which the frame check has already passed, so it is never cut;
- the floor, `PRELOAD_BUNDLE` (64), cut to what a frame carries of the widest table's mean row
  with the same headroom. That is `MAX_FRAME_BYTES / (widest × FRAME_HEADROOM)`, and at least 1.

The widest table decides because one preload bundle can mix rows from several tables. For rows
of 1.1 MiB the result is 14, not 64. For rows of a kibibyte it is still 64.

## Alternatives rejected

**Drop the floor and preload at the run's own bundle.** A run measured at `--bundles 1` would then
preload one row a round trip, and on the lab the floor is what lets a preload of twenty thousand
rows take seconds. The floor was right for narrow rows, so it is kept and cut for wide ones.

**Refuse the run, as the frame check refuses a bundle too large.** The run's bundles are the
user's choice, and refusing a choice the frame cannot carry is right. The preload's bundle is the
bench's own choice, so refusing the run for it would refuse the user for the bench's mistake.

**Cut each bundle at the frame in the driver, by the archived size.** The client archives a
bundle as it sends it. Measuring each query before adding it would archive every row twice, on
the path that is being measured. The mean row with headroom is how the bench already judges a
bundle, and one rule is easier to keep than two.

**Raise the frame.** The bound is the server's `networking.max_frame_bytes`, which an inventory
does not render. A larger client frame would be refused by the node all the same.

## Invariants to uphold

- **Every bundle the bench chooses is judged against the frame by one rule.** That rule is the
  file's mean row × the bundle × `FRAME_HEADROOM` ≤ `MAX_FRAME_BYTES`. A run's bundles are judged
  before the cluster exists, and the preload's where it is chosen. A third place that picks a
  bundle of rows needs the same judgement. The verify pass bundles reads, whose requests carry
  keys and whose answers come one frame a query, so it does not.
- **The preload's bundle is never smaller than the run's largest.** That one passed the check,
  and cutting it would slow a preload for nothing.
- **`FRAME_HEADROOM` is a guess, not a bound.** It covers a row's archived form being wider than
  its text, and rows wider than the mean. A file whose rows vary by more than four times can still
  make a bundle past the frame.

## Still open

- **A refused bundle's queries vanished from the record**, which is what turned this into a
  silent short preload. That is [item 211](../known-issues.md#211-a-bundle-refused-at-its-send-loses-its-queries-from-the-benchs-record),
  and it is open.
- **The bench judges against the default 64 MiB, not the node's own bound.** A node configured
  with a smaller `networking.max_frame_bytes` tells the client so at the hello
  (`peer_max_frame_bytes`, `shoal-client/src/client.rs`), and the bench never reads it.
- **A read's answer is not judged.** An answer is one frame a query. At `--read-keys k` that is k
  rows, and nothing compares k × the row with the frame. The default is one key.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `wide_rows_preload_in_bundles_a_frame_carries` | `examples/bench_dataset/tests/bench_run.rs` | Rows of 1.1 MiB are preloaded in a bundle of sixty-four, past the frame, and the preload stops short of the file |
| `the_preload_bundle_fits_the_frame` | `shoaladm/src/bench/orchestrate.rs` | The preload's bundle is not cut to the frame, or is cut below the run's own |

## Related

[F66. Dataset benchmarks](../../features/dataset-benchmarks.md), whose preload this bounds;
[item 211](../known-issues.md#211-a-bundle-refused-at-its-send-loses-its-queries-from-the-benchs-record),
found with it; [Resolved #202](append-batch-bytes.md), the bound on what a member is fed, for the
same wide rows; [X3](../../object-storage/spikes.md#x3-bytes-through-the-tablet-groups), the spike
that needed it.
