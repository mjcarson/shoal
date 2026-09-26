# 162. A late cut of a scrub could answer for a newer one of the same operation

## Symptom

In the first lab run of [#160](unreadable-partition-stalls-one-copy.md)'s fix, titan's stalled
copy of group `3910f1cf1a04c102` was repaired automatically, and the repair failed its own
verification:

```text
a group's repair is done ... outcome=Failed { reason: "after the install the copies still
  disagree: Divergent { quarantined: [(… titan …, shard: 4 }, Divergent)] }" }
```

A second repair ten seconds later judged all three copies equal and lifted the quarantine. The
installed copy had been right all along. What was wrong was the digest the leader read for it.

## Cause

A repair scrubs twice under one operation: once to judge, once after the install to verify. A
replica keeps one digest slot per operation (`MachineState::digests`). `note_scrub` replaces the
slot with `Pending` when the scrub is applied, and `record_digest` fills it when the cut's task
posts.

Titan's install boundary (66066) was below the repair's first scrub (68373). The leader's
checkpoint lagged its applied index under the load, and its cut is at its checkpoint. So when titan
restarted its group from the snapshot, its replay applied the first scrub again, and then the
verifying scrub at 219834. Both cuts' tasks ran. The first finished last (02:37:06, reading 64,552
archived partitions against 2,060 resident), and `record_digest` overwrote the verifying scrub's
pending slot with the report at 68373. The leader polled the operation, got a verified report,
compared it with the others' digests at 219834, and judged the copy divergent.

## Evidence

**Found on the lab, established from the journals of all three nodes.** Titan's own lines, in
order:

```text
02:36:51.32 applied a scrub  op=81bf9820… index=68373  resident=2060  archived=64552
02:37:03.40 applied a scrub  op=81bf9820… index=219834 resident=44138 archived=60448
02:37:06.44 a scrub's digest is in op=81bf9820… boundary=68373  digest="6a1582de53f88e7e"
02:37:13.25 a scrub's digest is in op=81bf9820… boundary=219834 digest="a6dde35d1a9caf44"
```

The leader judged at 02:37:06.54, between the two, and europa and hyperion reported `a6dde35d…` at
219834. `a_late_cut_does_not_answer_for_a_newer_scrub` (`shoal-core`, `replication/machine.rs`)
replays the sequence against the state. It was written with the fix, so against the unfixed
`record_digest` it is argued from the source, not run: with one slot per operation, the late
report at 10 replaced the pending one at 20.

## The fix

- **A digest slot carries the index its scrub was applied at** (`digests: (op, index, answer)`).
  `note_scrub(op, index)` replaces the slot, and `record_digest(op, index, answer)` fills it only
  when the index matches. A cut of an earlier application is late and says nothing.
- **The leader takes only a report at the boundary it scrubbed at** (`scrub_group`). A report of
  another boundary is treated like `Pending`, and the member is asked again. Either half alone
  closes the defect. Both are kept, because the replica is not the only place a stale answer can
  come from: a digest cached from before a restart would be another.
- `ServerMsg::Digested` carries the index, so the loop can match it.

## Alternatives rejected

- **A fresh operation for the verifying scrub.** It would sidestep this case, but not a replay that
  applies one operation twice for another reason, and it would split one repair's record over two
  operations.
- **Refuse to replay scrub entries.** A scrub is an entry in the log like any other, and every
  replica has to apply the log in order. The digest bookkeeping is where the fix belongs.

## Invariants to uphold

- **A digest answers for the application of its operation at the index it was cut at, and no
  other.** Everything that records one names the index.
- **The leader never counts a report whose boundary is not its scrub's.**

## Still open

Nothing of this defect. Why the cut was so far below the scrub, the leader's checkpoint lagging,
is what [O70](../optimizations.md#o70-a-snapshot-cut-reads-its-records-one-at-a-time) measured on
the same run.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_late_cut_does_not_answer_for_a_newer_scrub` (`shoal-core`, `replication/machine.rs`) | A late report of an earlier application replaces the pending slot of a newer one |
| `an_unreadable_partition_stalls_one_copy_and_repairs_it` (`shoal/tests/cluster_fixture.rs`) | A repair's own verification of an installed copy reads the wrong cut |

## Related

- [Resolved #160](unreadable-partition-stalls-one-copy.md), whose repair found it.
- [Resolved #164](replayed-scrub-cuts.md), the replayed scrubs' other cost.
- [F44](../../features/repair.md), the scrub and its digests.
