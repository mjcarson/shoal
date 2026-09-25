# 136. A node crash-looping on a defect could not be given the program that fixes it

## Symptom

With titan crash-looping on [item 135](leftover-temp-map.md), the fixed program was built and
`cluster upgrade` was asked to deliver it, first to every node and then to titan alone:

```text
$ tmdb-dataset-loader cluster upgrade -i tmdb_cluster.yaml titan
Error: tmdb is not ready for a rolling restart: titan is down, not up
```

No shoalctl command could put the fix on the one node that needed it. The alternative was copying
the program by hand, which is the thing [F55](../../features/cluster-upgrade.md) exists to replace.

## Cause

`judge_health` refused an upgrade unless every recorded node was an up member. That is right for
the nodes an upgrade is about to restart: taking a node down on top of one that is already down
can cost a set its quorum. It is wrong for a node that is already down and was named to be
upgraded. Replacing that node's program cannot cost a quorum anything its being down does not
already cost, and a crash loop on a defect is exactly when a new program is needed.

## Evidence

**Established by running it**, on the lab, with the output above. The unit test below was
written against the new signature, so it has no failing run of its own against the old code.

## The fix

`judge_health` takes the nodes named on the command line. A member that is `down` and was named
is let through as a repair (`shoalctl/src/deploy/upgrade.rs`). Nothing else changes:

- a down node that was not named still stops an upgrade of the others,
- a named node that is leaving or removed is still refused, and
- refused writes, an under-replicated set or a running plan still stop everything.

`cluster upgrade -i tmdb_cluster.yaml titan` then pushed the program, restarted the unit and
waited for titan to come back, which it did.

## Alternatives rejected

- **A `--repair` flag.** Naming a node that is down is already the whole instruction. A flag that
  must accompany it is one more way to fail at the moment an operator is least patient.
- **Letting `--force` skip the health check.** `--force` means "restart even if the program is
  unchanged". Overloading it to also mean "ignore the health gate" would let a forced upgrade of a
  healthy cluster restart a node while another is down.

## Invariants to uphold

- **Only a named node may be down.** The gate's point is that no restart it allows can reduce a
  set below its quorum. A down node being repaired is not restarted from up, so it cannot.

## Still open

Nothing.

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_down_node_named_for_upgrade_is_a_repair` (`shoalctl/src/deploy/upgrade.rs`) | A named down node is refused, or an unnamed down node, a named leaving node or refused writes are let through |

## Related

- [F55](../../features/cluster-upgrade.md), the rolling upgrade.
- [Resolved #137](upgrade-waits-for-groups.md), found on the same repair.
