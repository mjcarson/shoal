# F56. Rebuilding a node from its peers with `shoalctl cluster rebuild`

## Context

A node whose disk will not start it has to be emptied and filled again by its peers. On the lab
that happened twice: titan, with a hole in its WAL ([#151](../appendix/resolved/purge-ahead-of-its-marker.md)),
and hyperion, with a torn map entry ([#159](../appendix/resolved/map-ahead-of-archive.md)). Both
times the only tool was `cluster destroy` of the whole deployment and a reload, since the
alternative, a removal and a re-add, was a hand-made sequence of steps across the host, the
inventory state and the admin plane.

The [todo](../appendix/todos.md#rebuild-a-node-from-its-peers) recorded a lab experiment that
wiped hyperion's data but kept its identity. It worked on an idle cluster, and was unsafe as a
procedure: an emptied voter under its old identity grants its vote to any candidate. If an entry
was committed and held by one other member alone, and that member failed during the refill, a
candidate without the entry could win and the committed write would be gone.

## What it does

```
shoalctl cluster rebuild -i <inventory> <node> --yes
```

1. **Gate.** `rebuild_refusal` refuses a node that is not a plain member, any other member that is
   not an up `Member`, and an open plan. The rebuilt node's copies are gone until the plan
   refills them, so every other copy has to be there.
2. **Stop.** `systemctl disable --now` the node's unit (so `Restart=on-failure` cannot bring it
   back mid-wipe), and wait until the cluster commits its old identity `down`.
3. **Wipe.** `preflight(wipe)`: every storage root and the TLS directory.
4. **Join as a new identity.** Stage with `Entry::Join` through every placeable member's control
   address, `claim` a new node id, issue a leaf that names it, record it in the deployment's
   state, start the unit, and wait until it is an up member.
5. **Replace.** `Remove { node: old, replacement: Some(new) }`, followed until the plan is done.
   Every set the old identity held is moved onto the new one by the leader's planner, the old
   identity is tombstoned, and the control group promotes the new node to a voter.
6. Wait until every deployed node is up and the control group has its voters, and print the time
   each step took.

## Design choices

- **A new identity, always.** A new identity has no vote in any group until the plan makes it a
  member of the group's set, and it gets there the way any move's learner does, through
  catch-up, before it can vote. The safety argument is F45's, not a check of its own.
- **The node is committed down before it is removed.** `Remove` refuses a live member, and
  waiting for the detector's verdict is what makes the order hold: the old identity cannot answer
  anything after its removal starts.
- **The inventory record is rewritten before the new node starts.** A rebuild interrupted after
  the claim leaves the record naming the identity on disk. A rerun of `rebuild` then sees a member
  the cluster knows, not a stale id.
- **It reuses `add`'s pieces** (`preflight`, `stage`, `claim`, `provision_tls`, `start_unit`) and
  `rebalance`'s plan follower (`follow_plan`), so a deployment has one way to do each step.

## Alternatives rejected

- **Keep the identity, and check that it is safe first.** The check is per group: every other
  voter matched to the leader's committed index before the wipe. It holds only on a cluster that
  is idle or nearly so, and it is racy the moment writes arrive. A new identity needs no such
  check, and costs a tombstone.
- **A `Replace` operation in the control plane.** `Remove` with a replacement already is one
  ([F46](capacity-rebalancing.md)). What was missing was the host half, which is shoalctl's.
- **Rebuild without stopping the node first.** A node that is running but broken still holds its
  identity's vote. Stopping it is what lets the old identity be removed.

## Limitations

- **The node's data is thrown away**, including anything only it held. At factor one there is
  nothing to rebuild from, and the gate refuses unless every other member is up.
- **It takes as long as moving every set the node held**, one plan step at a time: 6 to 12
  minutes on the lab for 1.4 GiB under load (below).
- **The old identity's tombstone is permanent**, like every removal's.
- **It is not proven past the lab's size, and the defaults are not sized for terabytes.** Steps are
  replica sets, so their number is fixed by the placement (18 on the lab) and their size grows with
  the data: about 55 GB each at a terabyte a node. Extrapolated, **not measured**: under load the lab moved
  about 3.4 MiB/s, which would be more than three days a terabyte. Idle, the stages ran at 50 to 80
  MB/s each, one after another, about 25 to 30 MB/s end to end, or about ten hours a terabyte. The
  node's 64 MiB/s stream budget and a 1 GbE link bound it at three to five hours. Under writes, two defaults would likely
  stop it converging. `snapshot_timeout` is 5 minutes a transfer, where a transfer would take 15 to
  35. And `retained_entries` (100,000) and `retained_bytes` (1 GiB) a group are seconds of a busy
  group, so the leader would purge past the snapshot a new copy is installing, and send it another.
  Filed in [todos](../appendix/todos.md#rebuild-and-move-at-terabyte-scale).
- **A cluster restored before [#168](../appendix/resolved/restored-rows-outside-the-log.md)'s fix**
  can feed the new node logs that hold none of the restored rows. On such a cluster, repair every
  table in `repair` mode first.

## Invariants to uphold

- **A rebuild never reuses an identity.** `rebuild` refuses to continue if the claim returns the old
  id, which would mean the wipe did not happen.
- **The old identity is down before `Remove` is sent**, and the node's unit is disabled before its
  roots are wiped.

## Performance

On the lab, rebuilding hyperion while the mixed bench ran against all three nodes:

| | Cluster loaded from the csv | Cluster restored from a backup (#168 fixed) |
| --- | --- | --- |
| Committed down after stop | 14 s | 13 s |
| Joined as a new identity after | 22 s | 21 s |
| Plan, 18 sets, 1.4 GiB | 6 min 3 s, 1.1 GiB streamed | 11 min 22 s, 1.8 GiB streamed |
| Whole rebuild | 392 s | 710 s |
| After it: rows missing through the rebuilt node alone | 331,350: the defect #168 | 0 |

The restored cluster's rebuild streamed more because every set was sent as a snapshot, which is
#168's fix working. The earlier run fed about half of the groups from their logs, and those groups
were missing the restored rows.

**Where the time goes.** 1.8 GiB in 710 s is 2.6 MB/s, but no transfer ran that slowly. One step
of the second run, a Movie group of 181,679 rows and 82 MB: the cut took 1.5 s, the stream and
install about 1.5 s (about 80 MB/s), catch-up and the two membership changes about 1 s, and the
planner about 5 s to notice the move was done and issue the next step (`plan_interval`). Steps
whose cut queued behind the source compactor's merges under the bench waited up to 15 s more. The
plan moves one set onto a node at a time (`rebalance.moves_per_node`, 1), so these fixed costs add
up, about 35 s a step, and bandwidth is never the limit at this size.

**At a larger size the per-record work is the limit, and more steps at once do not help.** With
the benches' inserts the cluster grew to about 240 MB a set, and hyperion was rebuilt twice more
under the same bench, once with `moves_per_node` at 6 (one per shard) and once at 1:

| `moves_per_node` | Moved | Plan | Rate | Per step |
| --- | --- | --- | --- | --- |
| 6 | 3.5 GiB | 17 min 26 s | 3.4 MiB/s | 1 min 48 s, six at a time |
| 1 | 4.3 GiB | 21 min 56 s | 3.4 MiB/s | 1 min 9 s |

One step at 1, a Movie set of 710,444 rows and 279 MB: the cut took 41 s (17 MB/s, where an idle
cut of 320,561 rows took 4 s), the stream 9 s, and catch-up 36 s, while hyperion also applied the
bench's writes for every set it already held. The source compactors and the four core destination
were already busy, so six steps at once only stretched each step. About 10,000 records a second
end to end is what these hosts moved under this load. The one waste found in these runs, cuts for
the old identity while it was still a member, is [O73](../appendix/optimizations.md#o73-a-snapshot-is-cut-for-a-member-that-cannot-be-reached).

## Tests

| Test | What breaks if this is reverted |
| --- | --- |
| `a_rebuild_needs_every_other_member_up_and_no_plan` (`shoalctl`, `deploy/rebuild.rs`) | A rebuild starts with another member down, with a plan open, or of a node that is leaving |
| `a_copy_moved_in_after_a_restore_holds_the_restored_rows` (`shoal/tests/cluster_fixture.rs`) | The moves a rebuild is made of feed a new member from a log without the restored rows |

The command itself is proven on the lab ([Rebuilding a node under load](../cluster-testing/correctness.md#rebuilding-a-node-under-load)),
not in a test: it drives systemd and ssh on real hosts.

## Related

- [F46](capacity-rebalancing.md), `Remove` with a replacement.
- [F45](replica-migration.md), the moves that fill the new node.
- [F51](cluster-deployment.md), the deployment it acts on.
- [Runbook 3](../operations/runbooks.md), replacing a node.
