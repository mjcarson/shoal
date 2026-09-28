# 188. A forced purge at `retained_bytes` passed the boundary a new copy was installing

Filed and fixed in one change, from round 14 of the lab testing
([the byte retention](../../cluster-testing/correctness.md#the-byte-retention)). It is the half of
[#185](snapshot-outrun-by-purge.md) that page left open: #185 held a group's unforced snapshot
builds while a member took a snapshot, and said a forced purge, the one `retained_bytes` asks
for, was not held.

## Symptom

hyperion was rebuilt from its peers under the mixed bench with `retained_bytes` at its floor of
20 MiB a shard and every node's snapshot streams throttled to 2 MiB/s, which together stand in
for a set too large to send within a shard's retained WAL. One group was installed twice, 22 s
apart, each time with a whole cut:

```text
06:53:23.61 a snapshot was received whole; installing  group=f420c0340c34bd2f boundary=105068 bytes=43078823
06:53:23.98 a snapshot is installed                     group=f420c0340c34bd2f boundary=105068
06:53:45.64 a snapshot was received whole; installing  group=f420c0340c34bd2f boundary=137034 bytes=46011340
06:53:46.10 a snapshot is installed                     group=f420c0340c34bd2f boundary=137034
```

Between the two, the group's leader had purged its log past 105,068, so the entries the new copy
asked for next were gone.

## Cause

The retention sweep (`Shard::enforce_retention`, `shoal-core/src/server/shard/groups.rs`) bounds
a shard's sealed WAL at `retained_bytes`. Past it, every group with frames in the oldest sealed
segments is made to build a snapshot and purge through its checkpoint, so the segments can be
deleted. It did so whether or not a member was taking a snapshot of the group. #185's hold defers
only unforced builds (`try_create_snapshot_builder` with `force = false`), and the sweep's build
is forced, so nothing stopped it.

It also purged further than the budget asked. A segment can be deleted once every group with
frames in it has purged past its last frame there. The sweep purged through the group's whole
checkpoint instead, which on a busy group is thousands of entries past the segment.

At 40 MiB and unthrottled streams nothing looped. The byte budget there held about 19,000 of a
busy group's entries, about 19 s of its writes, and a step took 5 s. At 20 MiB and 2 MiB/s a step
took about 50 s, longer than the retention's span.

## Evidence

**Established by running it**, on the lab at `0d24968` (`target/lab/r14/tb/ab.sh`), against the
fix on the same inventory (`slow-unfixed.yaml` and `slow-fixed.yaml`, which differ only in the node
program). A first run at 40 MiB with unthrottled streams (`bytes-unfixed`) did not reproduce it:
every group installed once, and the rebuild took 200 s.

| Arm | Rebuild | Installs per group on hyperion | Forced purges (europa, titan, hyperion) | Lost |
| --- | --- | --- | --- | --- |
| 40 MiB, unthrottled, before the fix | 200 s | 36 once | 12,643, 8,429, 1,705 | 0 |
| 20 MiB, 2 MiB/s, before the fix | 1,068 s | 35 once, 1 twice | 50,158, 12,549, 6,124 | 0 |
| 20 MiB, 2 MiB/s, the fix | 1,042 s | 36 once | 5,313, 10,396, 5,718 | 0 |

Both throttled arms streamed about twice what they moved (1.7 and 1.6 GiB for 878 MiB). That is
not the loop: a step that takes 50 s feeds its new copy 50 s of the set's writes from the log as
well as the snapshot. Every arm read back every acknowledged insert and the whole csv through each
member alone.

## The fix

**The sweep passes over a held group while the WAL is within an allowance of its budget.** A new
setting, `cluster.replication.hold_bytes` (1 GiB by default, the same as `retained_bytes`), is how
far past `retained_bytes` a shard's sealed WAL may grow while members are taking snapshots.
Within it, the sweep forces no group that #185's hold says a member is taking a snapshot of.
Past it, every group is forced alike (`retention_spares_held`,
`shoal-core/src/server/replication/network.rs`). Each group passed over is counted in
`SnapshotStats::forced_held`.

**A forced purge goes only as far as the segment needs**: through the group's last frame in the
segment being dropped, not through its checkpoint. The entries after the segment stay for any
member the budget does not have to drop.

The inventory's `replication:` block names `hold_bytes`, and `stream_bytes_per_sec` too, which is
how the lab staged a slow step ([F51](../../features/cluster-deployment.md)).

## Alternatives rejected

- **Holding forced purges without a bound.** A member that never finishes, or a transfer that
  takes hours, would pin every shard's disk. #185 rejected this for the same reason, and the
  allowance is the bound it lacked.
- **Raising `retained_bytes`.** That keeps the extra log on every shard all the time. The
  allowance is spent only while a member is taking a snapshot.
- **A per-group allowance.** The disk is the shard's, and so is the WAL. One group's allowance
  says nothing about the other groups writing into the same segments.
- **Purging through the checkpoint and holding the snapshot boundary.** A purge through the
  checkpoint is what the budget did not need. Stopping at the segment is both smaller and exact.

## Invariants to uphold

- **A shard's sealed WAL is bounded at `retained_bytes + hold_bytes`**, whatever members are
  doing. The allowance is judged on the whole shard's sealed bytes at every sweep, and past it
  a held group is forced like any other.
- **A hold always lapses**, by expiry, by lift, or by its member's silence (#185). The allowance
  is spent only while one is live.
- **A forced purge never goes past the dropped segment's last frame for its group.** The
  budget's reason to purge is that segment, and nothing after it.
- **`hold_bytes` of zero is #185's behaviour**: a forced purge is never held.

## Still open

- **A step that outlasts `retained_bytes + hold_bytes`** still loses its log and is sent
  another snapshot. At a terabyte a node, the allowance is the operator's to raise.
- **`cluster.migration.timeout`** bounds a whole step, snapshot included, and a set whose
  transfer outlasts it fails and is retried. It is not related to the retention, and it is the
  next bound a terabyte step meets.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal-core` `server::replication::network::tests::retention_spares_held_groups_within_the_allowance` | The allowance is not judged on the shard's sealed bytes, overflows, or holds past its bound |
| `shoalctl` `deploy::inventory` and `deploy::render` tests | An inventory's `hold_bytes` or `stream_bytes_per_sec` is not validated or not rendered |
| The lab A/B (`target/lab/r14/tb/ab.sh`) | A group installs a second snapshot during a rebuild whose steps outlast the byte retention |

## Related

- [#185](snapshot-outrun-by-purge.md), the unforced half.
- [F43](../../features/node-recovery.md), how a copy is fed a snapshot and what `retained_bytes`
  bounds.
- [Rebuild and move at terabyte scale](../todos.md#rebuild-and-move-at-terabyte-scale).
