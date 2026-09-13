# 108. A cluster arm's server overrides were dropped on the way to every node

## Symptom

The catch-up arms of [F43](../../features/node-recovery.md) shorten the groups' checkpoint and
retention counts so the returning node falls past the purge point inside the run - the
`RetentionOverride` on the snapshot arm's `ClusterOverride`. Every node of the arm ran at the
defaults instead: a thousand entries between snapshots and ten thousand kept behind one, on
node zero and on every peer. The arm's page recorded what the smoke run showed - a returning
node behind by a handful of entries and inside the log, `by: none`, no snapshot cut on either
arm - and read it as the smoke scale being too small to show a catch-up, which was true, and
was not the only reason. Any setting a cluster arm put on its block this way was dropped the
same way; [F45](../../features/replica-migration.md)'s `retire_after`, without which a move
cannot be done inside a run, was the one that made it visible.

## Cause

`harness::conf::resolve` builds a node's `cluster:` block from the arm's override and puts the
retention on it. `harness::cluster::apply`, which every node runs afterwards to become the
staged node - its ports, its seeds, its control core, whether it bootstraps - replaced the block
whole with `Cluster::default()` and set only those fields on it:

```rust
conf.cluster = Some(
    Cluster::default()
        .bootstrap(node.index == 0)
        .seeds(node.seeds.clone())
        ...
);
```

The arm's `replication` block, and everything else `resolve` had put there, went with it. Node
zero runs `apply` as well as the peers, so no node kept the override.

## Evidence

**Established by running it.** The first smoke run of `macro/cluster/migration/move` on
2026-09-13 set `retire_after` to three seconds through the override and recorded the move
`unfinished` at the end of a twenty-four second run with every group `Activated` for fifteen
seconds: the source was waiting out the default grace of five minutes, since the override had
never reached it. With `apply` keeping the resolved block the same run finished the move in
seven seconds, `retiring` at 3.8 s. The catch-up arms' records on the F43 page are consistent
with it - `snapshots: 0` on the snapshot arm - and nothing there was re-measured; the finding
is recorded on that page's smoke section.

## The fix

`apply` takes the block `resolve` built (`conf.cluster.take().unwrap_or_default()`) and moves
only the node's own identity on it: bootstrap, seeds, control core, replication factor, data and
control ports. Everything else the arm put there rides to every node.

## Alternatives rejected

**Carry every override on `StagedNode` and reapply it.** A second copy of the same settings,
kept in step by hand. The block is already on the resolved configuration every node builds
from the same overrides; keeping it is one line.

**Apply the overrides after `apply`.** The same duplication from the other side.

## Invariants to uphold

- **`apply` moves a node's identity and nothing else.** A field it sets is one the placement
  decided per node; a field an arm sets is on the block already and has to survive.
- **Every node resolves the arm's own overrides.** A peer child runs `resolve` with the same
  overrides node zero did, then `apply`; a setting that has to differ per node is a
  `StagedNode` field, and there is none yet.

## Still open

- The catch-up arms have not been smoke-run again with the retention actually in force; the
  snapshot arm's smoke record on the F43 page describes a run at the defaults. The capture is
  the benchmark host's either way.

## Tests

| Test | Where | What breaks if this is reverted |
| --- | --- | --- |
| `the_migration_arm_places_a_fourth_node` | `shoal-bench/src/workloads/cluster_migration.rs` | The arm's `retire_after` is on its override; with `apply` replacing the block it would reach no node, and the arm's move would not finish inside its run - which the smoke run showed, and which a unit test cannot |

## Related

[F43](../../features/node-recovery.md), [F45](../../features/replica-migration.md),
[F36](../../features/cluster-harness.md).
