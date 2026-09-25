# 137. A rolling upgrade judged a node back before its shards had started

## Symptom

The repair of titan ([item 136](upgrade-a-down-node.md)) printed `back and caught up` and
finished 5.1 seconds after it began. Titan's journal says otherwise: the restarted process started
at 08:15:21, and its shards were still in `Shard::new` at 08:15:27, removing the leftover temp
maps of [item 135](leftover-temp-map.md). For about six seconds, a node the upgrade had declared
back served nothing.

In a rolling upgrade that is how two replicas end up down at once: the next node is restarted
while the previous one is still starting.

## Cause

`caught_up` asked the restarted node three things: are default writes admitted, is the largest
committed-to-applied gap zero, and is nothing installing. The answers come from its control
thread's readiness view. The control thread starts, rejoins and answers before the shards do, and
the replication figures in that view are folded over the shard reports that have arrived. With
none arrived, the node hosts no groups, so the gap is zero, nothing is installing, and writes are
admitted by a cluster that still has two other members up. The check could not tell "caught up"
from "has not started".

## Evidence

**Established by running it**, on the lab, from the upgrade's own timing against titan's journal
as above. After the fix, a forced rolling upgrade of all three nodes took 33.6 s where the first
one took 18.7 s. Each node was restarted only after the previous one printed
`SHOAL_NODE_SERVING` (hyperion 08:19:35, then titan restarted at 08:19:43; titan 08:19:46, then
europa restarted at 08:19:53).

## The fix

- The readiness view's replication fold counts `starting`, the groups whose handle is not up yet
  (`NodeReplication::fold`, `shoal-core/src/server/replication/report.rs`). It is `serde(default)`,
  so an older node reads as zero.
- shoalctl's model carries `starting` and `shards_reporting`, the number of shard reports the fold
  holds (`shoalctl/src/cluster/model.rs`).
- Before restarting a node, `restart_and_wait` reads that node's shard and group counts, trying
  for three seconds. A node that does not answer is being repaired, and the fallback is its
  configured cores and one group. `caught_up` requires the node to report at least those shards
  and groups, with none starting, on top of the three checks it made before.

## Alternatives rejected

- **Waiting a fixed time after the unit is active.** Start time is the rehome, the recovery of
  every table and every group's start, which ranges from a second to minutes with the data. Any
  fixed wait is either too short on a large node or dead time on a small one.
- **Waiting for `SHOAL_NODE_SERVING` in the journal.** That line is the client listener, which
  binds before a group's handle is up. It is the same mistake one layer down.
- **Deriving the expected groups from the tablet map.** Exact, but it needs the map on the
  operator's side of the admin interface. The node's own count before its restart is the same
  figure for an upgrade, which does not move placement.

## Invariants to uphold

- **A node's readiness view reports a shard only after that shard has built its groups, and a
  group as `up` only once its handle is.** `caught_up` counts on both.
- **An upgrade does not change what a node hosts.** The pre-restart counts are what it must come
  back with; an upgrade that also changed the core count would be a rehome and could legitimately
  come back with a different shard count.

## Still open

- A node being repaired has no pre-restart counts, so the wait falls back to its configured cores
  and one group. If `resources.cores` is unset, a repair could still move on once one shard has
  reported with no group starting.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_node_still_starting_its_shards_has_not_caught_up` (`shoalctl/src/deploy/upgrade.rs`) | A node with no shard reported, some shards reported, or groups still starting is judged caught up |
| `caught_up_needs_no_lag_and_no_install` (`shoalctl/src/deploy/upgrade.rs`) | Lag, an install or refused writes stop being checked |

## Related

- [F55](../../features/cluster-upgrade.md), whose "wait until it is back" this corrects.
- [F39](../../features/membership.md), the readiness view.
