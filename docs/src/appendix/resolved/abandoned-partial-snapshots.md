# 194. A partial snapshot its sender gave up on was held for good, against every later stream

Filed and fixed in one change, from round 15 of the lab testing
([a step that outlasts both retentions](../../cluster-testing/correctness.md#a-step-that-outlasts-both-retentions)).

## Symptom

On [#192](forced-build-deferred.md)'s fix, hyperion rebuilt again under the mixed bench with every
node's streams at 4 MiB/s and both retentions small, so every step outlasted them. The rebuild
moved nothing in 49 minutes. Three of its moves failed on their catch-up deadline, and the plan
stood blocked on tablets 0 and 1. europa logged the destination refusing snapshots it was sent,
820 times:

```text
ERROR openraft::replication::snapshot_transmitter: ReplicationError while sending snapshot:
  ... the replication link failed: 1731724f-.../0 refused the snapshot: 1173622511 bytes of
  partial snapshots are held and 1245710529 more would pass the 2147483648 byte bound
```

## Cause

A shard bounds the bytes of partial snapshots it holds (`replication.install_bytes`, 2 GiB by
default) and refuses a stream that would pass it (`handle_snapshot_begin`). A partial left that
count in only three ways: a new stream for the same group replaced it, its install finished, or
the copy was retired. A stream its sender abandoned, because the move it served failed or the
sender's transfer timed out, did none of those. Its partial, and its file, stayed until the
node restarted.

A Movie set at ten copies is about 1.2 GB. Once one move's partial was stranded, no other Movie
set's stream fit beside it under 2 GiB, and every later step onto the node was refused at its
begin until the plan gave up on it.

## Evidence

**Established by the lab's journals and by reading the begin**, at `f7ef5e4`
(`target/lab/r15/rb4/`): 820 refusals naming 1,173,622,511 bytes held, the same figure every time,
while the plan's moves failed and nothing was being assembled for the group that held them. The
begin sums `manifest.total` over every other group's partial with no test of whether its stream
was alive.

## The fix

**A partial whose stream brought nothing for longer than `replication.snapshot_timeout` and is
not installing is dropped, with its file, before a begin judges the bound**
(`Partial::touched`, set at the begin and by every chunk; `Partial::is_abandoned`). Past
`snapshot_timeout` its sender has given up on the transfer, so no chunk of it will come. Each
drop is a `WARN` and counts on `SnapshotStats::abandoned`.

## Alternatives rejected

- **Dropping a partial when its move fails.** The move is the source's, and the destination is
  told nothing when a source gives up; a sender that died would strand a partial the same way.
- **A larger `install_bytes`.** It would strand more before it refused, and a stranded partial's
  file stays on the disk either way.
- **Dropping partials on a timer.** The begin is where the bound is judged and the only moment a
  stale partial costs anything; a sweep would add a task for no reader.

## Invariants to uphold

- **A partial counts against the bound only while its stream could still finish.** Anything held
  past the sender's own deadline is garbage.
- **A partial being written or installed is never dropped**: `is_abandoned` is false while its
  writer runs, and the begin skips groups with an active install.

## Still open

- A partial abandoned when no other stream begins is dropped only at the next begin or a restart.
  It holds disk, not memory, meanwhile.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `replication::install::tests::a_partial_nothing_came_for_is_abandoned` | A stream that brought nothing for longer than a transfer may take is not seen as abandoned, so nothing drops it |
| The lab arm, `target/lab/r15/rebuild.sh` with `outlast.yaml` | A failed move's partial refuses every later stream of a set its size |

## Related

- [#192](forced-build-deferred.md), found in the same runs.
- [F43](../../features/node-recovery.md), a member fed by snapshot, and
  [F46](../../features/capacity-rebalancing.md), the stream budget and the disk reserve judged at
  the same begin.
