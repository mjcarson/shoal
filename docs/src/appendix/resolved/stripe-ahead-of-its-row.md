# 256. A row older than its chunks was answered as a retryable failure, never refused by name

A stripe whose row names a label its holders are past, which a `force_recover` that lost a write
leaves, was read again sixteen times and answered `Shedding`, and a client retried it for thirty
seconds before it gave up; P10 says such a row is refused by name.
[F93](../../features/operations-and-the-real-cluster.md) refuses it `Behind`, keeps its chunks, and
lets an operator settle it with `ResolveStripe`.

## Symptom

On the lab, in [Q31](../../object-storage/contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17)'s
run (E2), after a restore of the tmdb cluster's posters into a fresh cluster, every one of the 200
striped posters was answered `Unavailable`, "no current holder of stripe 0 of reclaim/000003.bin
answered … `Held { label: None }`", each after the client's thirty seconds of retries: 24 m 44 s for
the 200 at four in flight. After a `force_recover` that lost a write its holders applied, a read
answered `Shedding`, "the stripes of … moved under this read 16 times; try it again".

## Cause

`read_range` took a holder answering a label newer than its row's as the row having moved, read the
rows again up to `MAX_TRIES`, and then asked the client to try again. `newer` compared sequences
alone, so a holder at the row's sequence under another tag was stale to the reader. A write retried
the same way, and a rebuild stopped on "a source holds a label past the row: it moved". Nothing
told a row that moved from a row that was behind.

## Evidence

**Established by reading the source** for `force_recover`'s case, and **reproduced on the lab** for
the restored rows' case (E2, above), recorded in Q31's record. The fixture's
`rows_older_than_chunks_are_refused_by_name` was written with the fix: a write in place committed by
two nodes and applied on a third's chunks, whose copy of the row never heard of it, the two lost and
the third recovered alone.

## The fix

- **`Label::ahead_of`** is the one judge of a holder past its row: not the label named, and not
  older. The read, the write, the erasure coded read and the rebuild use it.
- **A row unchanged by a second `Quorum` read is behind its chunks**, since a label is applied only
  after its commit, and the read or write is refused **`Behind` (13)**, its message naming each
  stripe's row sequence; the client does not retry it.
- **Its chunks are kept**: a rebuild stops over an ahead holder, both scrubs count it `behind` and
  remove nothing, and a reclamation leaves it.
- **`ResolveStripe`** accepts the label `k + f` holders agree on or declares the stripe lost, after
  which it is answered **`Lost` (14)**.
- The restored rows' case cannot arise: a bucket's rows never travel as table files
  ([Resolved #255](restore-judged-by-table.md)).

## Alternatives rejected

**Moving the row to the newest label on its own.** Nothing on a holder says what the object's
entry, its size after an append among it, should be; only an operator knows whether to trust the
holders.

**Treating every ahead holder as behind at once.** A holder past the row is usually a write that
committed meanwhile; refusing it without a second strong read would refuse ordinary concurrency.

## Invariants to uphold

- **A label is applied only after its commit.** The refusal rests on it: a holder ahead of a row
  that a second strong read still names is a commit the row lost.
- **A chunk ahead of its row is never removed** by a scrub, a rebuild or a reclamation.
- **A resolution is versioned by the row's sequence** and answered by its identity on a repeat.

## Still open

A placement group holding such a stripe waits for its resolution before its move and its backfill
go on; nothing lists the stripes but the scrubs' `behind` count and the refusals.

~~A write that fell short reads its row again before it asks its holders, and takes a read that
failed for a row unchanged, so a concurrent commit or a lost read refuses a healthy stripe `Behind`
(item 262).~~ A write that fell short asks its holders first, and its loop reads the row again after
them ([Resolved #262](behind-asked-of-holders-first.md)).

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `cluster_fixture::rows_older_than_chunks_are_refused_by_name` | The stripe is served mixed or answered `Shedding`, its chunks removed, or the resolution misjudged |
| `shoal-proto objects::tests::a_label_is_ahead_at_another_tag_or_a_later_sequence` | A holder at the row's sequence under another tag is taken for stale |

## Related

[F93](../../features/operations-and-the-real-cluster.md), [F49](../../features/backup-and-recovery.md)'s
`force_recover`, [P10](../../object-storage/contract.md#the-contract).
