# 286 (in part). The erasure coded crash row armed a stage's crash on a node its write never staged to

`an_erasure_coded_write_is_atomic_at_every_crash_point` (`shoal/tests/cluster_fixture.rs`) writes one
stripe's object in place at each of the stripe write's crash points, then reads every node's copy of
it whole. One point, a holder's stage synced, is armed on node one, and the row waits for node one
to die there ([F90](../../features/erasure-coded-pools.md)). The item's other shape, a write with a
member killed not acknowledged, stays open on
[Known Issues](../known-issues.md#286-an_erasure_coded_write_is_atomic_at_every_crash_point-finds-a-write-with-a-member-killed-not-acknowledged-under-load).

## Symptom

`stage_synced: none of nodes [1] died`, in F109's and F111's workspace runs and in the confirmation
loop `w10`'s first round (2026-10-28): the node armed to crash after its stage synced never reached
the point in the row's 60 s.

## Cause

The row's 3+3 object is one data chunk's worth, so a write in place stages its data position and
the three parity positions, and the two data positions past the object's end are never stored.
With one position a device and two devices a host, node one's two devices hold exactly those two
in one run of fifteen. Then node one is never staged to and never reaches the point. The row armed
node one whatever it held. Its replicated twin, three copies on three nodes, always stages on
every node.

## Evidence

**Established by reading the row, then reproduced** by a run of the row alone, eleven times, printing
which nodes store a chunk of the object after its put. Ten passed with node one storing one or two,
and the eleventh failed with node one storing none:

```text
run 10:  DIAG the object's chunks by node (node, devices storing one): [(0, 2), (1, 1), (2, 1)]
run 11: test result: FAILED DIAG the object's chunks by node (node, devices storing one): [(0, 2), (1, 0), (2, 2)]
Error: NotReady("stage_synced: none of nodes [1] died")
```

The runs' summary is in `target/lab/i286/runs/summary.txt`, and the failing run's output in
`target/lab/i286/failed-run-11.txt`.

## The fix

**The row arms the stage's crash on the first of nodes one and two that stores a chunk of the
object**, read from their devices once the put's chunks are in place. Node zero stores at most two
of the four positions, so one of the others always stores a chunk. On the replicated twin that is
node one, as before.

## Alternatives rejected

- **Arm both nodes one and two.** The row's rounds kill one holder at a time, and two dying at
  once is a round of its own, which the apply's points already are.
- **Place the object by hand.** The fixture has no way to choose a placement group's holders, and
  a row should not depend on the draw it is given.

## Invariants to uphold

- **A crash point is armed on a node the round's write reaches.** A row that arms one by node number
  assumes a placement it was never given.

## Still open

- **The item's other shape**: a write with a member killed beside a held stager, not acknowledged
  by the members left, twice under load. The row keeps the write's answer since 2026-10-27, and its
  next failure says why.

## Tests

| Test | What fails if the fix is reverted |
| --- | --- |
| `an_erasure_coded_write_is_atomic_at_every_crash_point` (`cluster_fixture`) | In one run of fifteen, node one stores no chunk and the stage's point is never reached |

## Related

- [F90](../../features/erasure-coded-pools.md), the erasure coded write and its parity delta.
- [Tests That Fail Under Load](../flaky-tests.md), the row's record.
