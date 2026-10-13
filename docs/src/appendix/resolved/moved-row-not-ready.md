# 240. A write in place whose row moved under it was answered NotReady

A write in place stages on every holder, or asks every holder to confirm its base, and needs
`k + f` of them before it commits ([F86](../../features/replicated-pools.md#writing-in-place-appending-truncating)).
When fewer than that took it, it answered `NotReady` whatever the holders said. A holder that
refused because it already held a newer label means another write committed and applied while this
one staged: the row moved, and the write should read it again and try again. Found beside
[Resolved #239](record-bases.md) by its test, once writes were sent once.

## Symptom

F86's patch leg sends each write once (`retries: 0`). Eight writers in place on 32 objects counted
`NotReady` among their errors:

```text
stripe 0 of s1m/39/15/0000000000000002 was staged by 1 holders and a write needs 2: position 1:
Some(Refused { code: CannotMake, message: "the stage expects Some(Label { sequence: 373, .. }) and
the sl...
```

`NotReady` says a pool cannot take a write. Nothing was wrong with the pool: the write had lost a
race. A client that retries hid this, since it retries `NotReady`; one that sends once saw it.

## Cause

`write_stripe` counted the holders that staged, and `small_write` the ones that confirmed. Below
`k + f` both returned `StripeStep::Refused` with `NotReady`. A holder answers `CannotMake` when it
cannot make the label the stage expects. It cannot when it is behind, which is a pool short of
holders, and also when it is ahead, which is a row that moved. The write never asked which.

## Evidence

**Reproduced** by `small_writes_from_eight_writers_on_one_stripe` with each write sent once, run
against the tree with [item 239](record-bases.md)'s fix and not this one:

```text
writer 2 round 18: Object { ..., outcome: NotReady, msg: "stripe 0 of contended.bin was staged by 0 holders and
a write needs 2: position 0: Some(Refused { code: CannotMake, message: \"the stage expects Some(Label { sequence:
135, .. }) and the slice holds Some(Label { sequence: 139, .. }), from which it cannot make it\" }); position 1: ..." }
```

The holders held sequence 139 and the write had read 135.

## The fix

**A short write reads its row again before it refuses.** `row_moved` reads the stripe's row at
`Quorum` and compares its sequence with the one the write read. If it moved, the step is `Retry`:
the loop reads every row again and tries again, as it does after a refused commit, bounded by
`MAX_TRIES` rounds and then `Busy`. Only a row that did not move is answered `NotReady`.

## Alternatives rejected

**Retrying every short write.** A holder that is behind would be asked again `MAX_TRIES` times and
then answered `Busy`, which tells a client to retry a write the pool cannot take.

**Reading the holder's label from its refusal.** `CannotMake` names it only in its message. The row
is the fact a write commits against, so it is the one to read.

## Invariants to uphold

- **`NotReady` from a write means the pool could not take it at the row it read.** A row that
  moved is a retry, and many retries are `Busy`.
- **The re-read is only on failure.** A write that met its `k + f` never pays it.

## Still open

Nothing of this item.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `small_writes_from_eight_writers_on_one_stripe` | `shoal/tests/objects.rs` | A write sent once is refused `NotReady` when another write's commit applied while it staged |

## Related

- [Resolved #239](record-bases.md), found by the same test.
- [F86](../../features/replicated-pools.md), whose patch leg sends each write once.
