# 276, 277. An object preload counted in objects put half of them, and a put that wrapped was unmarked

Two defects of the object driver `shoaladm bench` runs ([F80](../../features/object-wire.md)),
found while [F94](../../features/bench-object-endpoints.md) sized the comparison's dataset. A
preload given as a count of objects put half of each description's objects whatever the count
said (276). An arm's puts that ran past the description's last object put earlier objects again,
and no run said so (277). Filed and fixed in one change.

## Symptom

- **276.** `--preload 1024` on a run of object arms preloaded 50% of each description: 512 of a
  description of 1024 objects, eight million of one of sixteen million. The flag's help says a
  preload is rows or a percent, and a table run reads a count as rows; an object run read any count
  as half.
- **277.** A put arm at 4 KiB and sixteen in flight puts tens of thousands of objects a run. Once a
  description's objects ran out, the next put was of the description's first object again, the
  same path and the same bytes. Its latency and rate were counted as a new object's, and
  `RunResult::wrapped`, which says a table run's inserts started over, stayed false. A capture's
  put rate could be a rate of overwrites, and nothing in it said so.

## Cause

- **276.** `drive` in `shoaladm/src/bench/orchestrate.rs` mapped the spec's preload to the driver's
  percent by `Preload::Rows(_) => 50.0`, a placeholder from F80's first object arms: the object
  driver took only a percent.
- **277.** `ObjectDriver::run_arm` took a put's object as `next % description.objects`, by design,
  so an arm never runs out. `result` set `wrapped` from the table feeds alone, and the object driver
  kept no flag.

## Evidence

- **276: established by reading the source**, the match at `orchestrate.rs:764-767` at `e440bb07`.
  Not reproduced on a node: the mapping is one line between the spec and the driver's constructor,
  and no test reached it.
- **277: reproduced** against the tree before the fix (`e440bb07`), by a test written for it and
  kept out of the tree (`target/lab/stores/repro`). It put to a node of `bench_dataset`'s images,
  whose description holds 400 objects, half of them preloaded, at eight in flight for four
  seconds:

  ```text
  puts acknowledged in the arm: 1596; distinct objects acknowledged, preload included: 400
  thread 'puts_wrap_unmarked' panicked at examples/bench_dataset/tests/wrap_repro.rs:57:5:
  1596 puts made only 400 distinct objects: 1396 were put twice
  ```

## The fix

- **276.** The object driver takes an `ObjectPreload`: `Percent`, or `Objects`, that many of each
  description's objects, never more than it holds. A spec's `Preload::Rows(n)` is
  `ObjectPreload::Objects(n)` in both orchestrators.
- **277.** Each namespace keeps a `wrapped` flag, set by a put whose next object is past the
  description's last. `ObjectDriver::took_wrap` takes every namespace's flag, which clears it, and
  each run ORs it into `RunResult::wrapped`. `compare` already reads a run that wrapped as
  incomparable with one that did not.

## Alternatives rejected

- **276: reading a count as a percent of it.** It is what the placeholder did in effect, and a
  count is what a reader of `--preload 1024` means.
- **277: refusing an arm that wraps.** A description sized for an arm is the operator's to fix,
  and a run that says it wrapped is worth more than a run that stops. The comparison's dataset
  holds sixteen million objects, so none of its arms wraps.
- **277: stopping the arm at the description's end**, as a table arm's `on_exhaust: end` does. An
  object arm would then end at a different second on every store, and its windows would cover
  different times.

## Invariants to uphold

- **A put's object is the namespace's next past its preload**, and only a put past the
  description's last is a wrap. A cleanup between arms (F94) leaves `next` where it was, so no
  path is put again after its delete.
- **`took_wrap` is asked once a run, after its arm**: it clears the flags it reads.

## Still open

Nothing of either.

## Tests

| Test | What breaks if it is reverted |
| --- | --- |
| `objects::tests::a_wrapping_put_is_marked` (shoal-loadgen) | A put past the description's last object goes unmarked, or the flag is not cleared for the next run |
| `objects::tests::patch_is_refused_without_writes_in_place` (shoal-loadgen) | A count of objects to preload is read as a share, or a count past the description puts more than it holds |

## Related

[F94](../../features/bench-object-endpoints.md), which found both; [F80](../../features/object-wire.md),
the object arms; [F66](../../features/dataset-benchmarks.md), whose `wrapped` a table run sets.
