# 105. A volatile group never checkpointed, so its log was never purged

## Symptom

An ephemeral table's tablet groups grew their in-memory logs without bound. Every write to the
table was an entry the group kept forever: openraft never built a snapshot for it, never purged
behind one, and the volatile logs reached `volatile_log_bytes` - a quarter of a gibibyte per
shard by default - after which every write to the table was shed, for good, until the process
was restarted and the table was empty again. A member of such a group that restarted could not
have caught up past a purge point either, since there never was one.

## Cause

A group's checkpoint is what its snapshot names, and `try_create_snapshot_builder` in
`shoal-core/src/server/replication/machine.rs` offers a builder only once the checkpoint has
moved past the last snapshot and is durable. For a persistent group the checkpoint moves when
the compactor finishes a WAL segment. A volatile group has no segments and no compactor, so
`rebuild_groups` gave it `checkpoint: None` and nothing ever moved it: the builder was never
offered, `LogsSinceLast(checkpoint_entries)` fired into a refusal on every check, and
`max_in_snapshot_log_to_keep` had no snapshot to keep the log behind.

## Evidence

**Reproduced.** `a_volatile_group_purges_its_log` in `shoal/tests/cluster_fixture.rs` starts
three nodes with `checkpoint_entries` at eight and `retained_entries` at sixteen, writes
ninety-six rows to the ephemeral table through node zero, and polls its groups for a purge
point. Against the tree before the fix it timed out after twenty seconds with every volatile
group at a checkpoint of zero and a purge point of zero, however far it had applied:

```text
thread 'a_volatile_group_purges_its_log' panicked at shoal/tests/cluster_fixture.rs:3644:9:
no volatile group ever purged its log (applied, checkpoint, purged): [(36, 0, 0), (31, 0, 0), (32, 0, 0)]
```

With the fix every group has a purge point within a few seconds of the writes, behind its
applied position by the retained count.

## The fix

For a volatile group the checkpoint *is* the applied position: `run_apply` moves the checkpoint
with `applied` on every entry, with the membership as of then, and marks it durable at once,
since a log that lives in memory has nothing to wait for. From there the policy builder fires
every `checkpoint_entries`, the snapshot is the metadata at that position, and the purge follows
`retained_entries` behind it. The checkpoint file is not touched: `write_checkpoint` still skips
volatile groups, and one starts from nothing on every restart, as it always did.

The snapshot such a group *sends* is [F43](../../features/node-recovery.md)'s: the loop cuts it
from the ephemeral table's resident partitions of the group's tablets, since there is no archive
to cut it from, and a member that comes back empty behind the purge point installs it.

## Alternatives rejected

**Let a volatile group's log grow, and raise the bound.** The bound exists so a group nobody
drains cannot take the process with it; a log that is never purged reaches any bound.

**Never purge a volatile log, and make a returning member replay it whole.** The log is what is
being bounded; keeping every entry to avoid needing a snapshot keeps the problem.

**Move the checkpoint on a timer rather than on every apply.** A timer is a second trigger
beside openraft's policy, with nothing to say that the policy does not already say. The
checkpoint following `applied` costs two clones per entry and keeps one rule.

## Invariants to uphold

- **A volatile group's checkpoint equals its applied position after every apply.** The report's
  `checkpoint` reads that way, and `try_create_snapshot_builder`'s "moved and durable" test
  depends on it moving.
- **`write_checkpoint` never records a volatile group.** Its checkpoint is not on disk and must
  not be read back at open as though it were: a volatile group starts from nothing.
- **The volatile snapshot is cut on the shard loop from resident partitions.** An ephemeral
  table has no other stable view; the pause it costs is bounded by the table's size on that
  shard, which is what [Q3](../../distributed/protocol.md) allows as the fallback.

## Still open

- Nothing about the volatile log's bound changed: a group whose writers outrun its purge still
  sheds at `volatile_log_bytes`. The bound is now reachable only by writing faster than
  `retained_entries` are purged, which is what it was for.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `a_volatile_group_purges_its_log` | `shoal/tests/cluster_fixture.rs` | No volatile group ever reports a purge point; the test times out with the view above |
| `volatile_replication_uses_common_encoding` | `shoal/tests/cluster_fixture.rs` | A volatile group's apply path changed shape |

## Related

[F40. Replication](../../features/replication.md), whose volatile groups these are;
[F43. Node recovery](../../features/node-recovery.md), which sends and installs their snapshots;
[F9. Ephemeral tables](../../features/ephemeral-tables.md).
