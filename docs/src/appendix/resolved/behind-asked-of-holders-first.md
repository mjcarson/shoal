# 262. A write that fell short was refused `Behind` on a stripe that was not behind

A stripe's write that reaches fewer than `k + f` holders asks whether its row is behind its chunks,
which [Resolved #256](stripe-ahead-of-its-row.md) refuses by name, `Behind`. It read the row again
and only then asked its holders, and took a read that failed for a row that had not moved. A write
that committed and applied between the two, or a read lost to a leader change, made a healthy
stripe's write `Behind`: final for the client, and a message naming a `ResolveStripe` that an
operator following it could end in declaring the stripe lost. Filed by a code review of F87 to F93,
which found two of the three sites; the second reading found the erasure coded write's.

## Symptom

Under writes in place racing on one stripe, a write was refused `Behind` ("a holder of stripe 0 of
raced.bin applied a write its row does not name: the row, at sequence 36, is behind its chunks,
which a ResolveStripe settles") on a stripe whose row named every label its holders held. A
`ResolveStripe` at the sequence named was refused `StaleVersion`, and at the next its `Accept` found
no holder ahead and said to declare the stripe lost instead.

## Cause

The fall-short branches of `write_stripe`, `small_write` and `write_erasure` read the row again
(`row_moved`) before asking the holders (`holders_ahead`). A label is applied only after its commit,
so a holder past the row is either a write that committed since the row was read or a row behind its
chunks, and only a strong read made *after* the holder's answer tells them apart: one before it lets
a commit and its apply land in between. And `row_moved` answers "not moved" when its read fails
(`let Ok(rows) = .. else { return false }`), so a lost read took a holder holding another write's
label for a row behind. Before F93 the branch ended in `NotReady`, which a client retries. The read
path asks in the right order, and so does the write's own loop for `StripeStep::Ahead`: holders
first, the rows again after them, and `Behind` only when the next round finds them the same.

## Evidence

**Reproduced** by `a_write_that_fell_short_is_never_behind_a_moving_row`, written first and run on
the unfixed order: six writers in place on one stripe through three nodes, with every row a write
that fell short reads again failing (`FAIL_ROW_REREADS on`), as a leader change or a deadline fails it.

```text
19 of 150 writes were refused Behind on a stripe that was not, beside 131 committed:
Some("a holder of stripe 0 of raced.bin applied a write its row does not name: the row, at sequence
36, is behind its chunks, which a ResolveStripe settles")
```

The race without a failed read, a commit between the read and the holders, was established by
reading and is the same order closed by the same change.

## The fix

- **The holders are asked first.** In all three branches a holder past the row read answers
  `StripeStep::Ahead`, and the write's loop reads the row again, after that answer, and refuses
  `Behind` only when the next round finds the row unchanged and a holder past it again. A row that
  moved is a write to try again.
- **A read again that fails is `NotReady`**, as before F93: `row_moved` is asked only once no holder
  is past the row, and its failure answers what a client retries.
- **A hook**, `faults::fail_row_rereads` and the fixture's `FAIL_ROW_REREADS on|off`, which takes
  every such read for one that failed.

## Alternatives rejected

- **Reading the row again after `holders_ahead` inside the branch.** It repeats the loop's rule
  beside it, and the loop's is the one the read path shares; one rule in one place.
- **Treating a failed read as a row that moved.** A retry against an unread row is sound, but a
  stripe really behind its chunks then answers `Busy` after its rounds instead of `Behind` by name,
  which is what Resolved #256 fixed.
- **Making `Behind` retryable.** A stripe really behind stays behind until an operator resolves it;
  retrying it for thirty seconds is what #256 removed.

## Invariants to uphold

- **`Behind` is answered only by the write's loop**, after a strong read made since a holder was
  found past the row. A branch that finds a holder past it answers `StripeStep::Ahead`.
- **A row read that fails never decides a refusal.** It is `NotReady`, or the loop's next round.

## Still open

Nothing.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `cluster_fixture::a_write_that_fell_short_is_never_behind_a_moving_row` | Writes racing on a stripe whose row reads fail are refused `Behind` |
| `cluster_fixture::rows_older_than_chunks_are_refused_by_name` | (guards the other way) a write to a stripe really behind is no longer refused by name |

## Related

[Resolved #256](stripe-ahead-of-its-row.md), which made `Behind`; [Resolved #239](record-bases.md)
and [Resolved #240](moved-row-not-ready.md) for a write whose row moved;
[item 265](../resolved/accept-counts-empty-chunks.md)
for the resolution the refusal named.
