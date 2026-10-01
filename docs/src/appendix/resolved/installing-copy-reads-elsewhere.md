# 184. A read through a copy that is installing a snapshot was refused, not sent to another holder

## Symptom

In round 13 of the lab testing, hyperion was cut off from its peers for 120 s and then for 300 s
under the mixed bench ([longer partitions](../../cluster-testing/correctness.md#longer-partitions-and-a-flapping-one)).
After each heal it caught up, partly by snapshot: 12 installs after the shorter cut and 18 after
the longer. While they ran, gets through hyperion were refused `Unavailable` (4,795 after the
120 s cut, 12,926 after the 300 s one), every one with the same message:

```text
group … is installing a snapshot; its tablets are not readable until it is installed
```

The two other members of each group held the tablet and could have answered at `One`.

## Cause

A copy installing a snapshot answers no read, and must not: what is resident is the old
generation and what is on disk is half of the new one (`installing_group` in `execute_query`,
[F43](../../features/node-recovery.md)). The fault was in routing. A shard routes each query with its
read ring (`TabletMap::read_ring_for`), which sends a tablet this node holds to this node's copy
whatever state that copy is in. The ring is built from the map, and an install is not in the map:
it is one shard's own state, begun when a snapshot arrives whole and ended when the compactor
finishes with it. So the node catching up kept routing its clients' reads to the copies it was
rebuilding, and refused them.

A quarantined copy is in the map (it is committed through the control plane), and the read ring
already routes around it. An installing copy had no such path.

## Evidence

**Established by running it**, on the lab at `9f06f2e`: `target/lab/r13/part/cut300`, the bench's
summary samples one message per code per second, and every refused get after the heal carried the
message above. No write was refused for it: a write goes through the group's leader, whatever the
local copy is doing.

**On the fix**, the same 300 s cut on a freshly loaded cluster (`target/lab/r13/part/rerun184.sh`):
hyperion installed 18 snapshots after the heal, and not one get was refused. The only refusals
after the heal were 255 writes answered `NotLeader` and 128 `OutcomeUnknown`, while leads moved
back. The rate was back 17 s after the heal, where the unfixed run was still at 40–60% at 40 s.
2,967,310 acknowledged inserts were read back through each member alone, and none was lost.

## The fix

- **A node-wide count of installing tablets** (`server/installing.rs`, `InstallingTablets`), one
  `Arc` shared by every shard through `PeerSetup`. The shard installing a group counts the group's
  tablets when it records the install (`begin`), and uncounts them when the install completes or
  fails (`end`). Every change moves a generation.
- **A routing ring beside the replica ring.** `Shard::route_ring` is the replica ring with every
  counted tablet that the ring sends to this node pointed at the holder
  `TabletMap::preferred_holder` names without this node (`TabletMap::steer_installing`). A shard
  rebuilds it when the generation it last routed under has moved, checked once per bundle, and
  whenever a map is installed. Every query is routed with it.
- With no other holder up, the tablet stays local, and the copy's refusal says why.
- **A read a peer forwarded is handed back.** A peer that holds no copy routes to the tablet's
  preferred holder, which may be the node installing it. The installing copy now answers such a
  read `StaleTopology` on a frame of its own (`answer_elsewhere`, which a retired copy's refusal
  also goes through), and the origin sends it once to another holder under the same attempt and
  slot (`reroute_pending`, [F45](../../features/replica-migration.md)). A read from a client of this
  node that still reaches the copy, one routed before the install began, is refused
  `Unavailable` as before.

## Alternatives rejected

- **Forwarding a client's read from the installing shard to another holder.** The installing
  shard is often not the one the client is connected to, and a share handed across the node's
  mesh has no pending record to reroute from. Routing it right the first time is simpler, and it
  costs one atomic load per bundle. A peer's read does have a pending record on its origin, which
  is why that half is handed back instead.
- **Committing installs to the map, as quarantines are.** An install lasts seconds, and a map
  change is a control-plane commit pushed to every node. Only this node's routing needs to know.
- **Waiting for the install inside the read.** It would turn a refusal into a wait as long as the
  install, when another member can answer now.
- **A per-table count.** The ring is per tablet, not per table, so a tablet installing for one
  table sends the other tables' reads of that tablet elsewhere too, for the length of the
  install. A remote read is still a correct answer, and the ring stays one structure.

## Invariants to uphold

- **Every `begin` has an `end`.** The count is taken where the install is recorded in
  `active_installs` and released wherever it is removed, completed or failed. A path that drops an
  install without either leaves its tablets routed away for the life of the process. That is safe,
  but it is a node reading remotely for no reason.
- **The routing ring is rebuilt from the replica ring, never patched in place.** An install's end
  restores a tablet's route by rebuilding, so the map's own routing (quarantines, down holders,
  unplaced members) is never undone by an install.
- **The installing copy still refuses what reaches it.** Routing is advice. A share already on its
  way still meets `installing_group`, and a peer's is handed back rather than answered.

## Still open

- **The forwarded half is proved by reading, not by a run.** It needs a member with no copy
  coordinating while another installs, which the lab's rerun did not stage. The path it takes is
  the one a retired copy's refusal has taken since F45.

## Tests

| Test | What breaks if the fix is reverted |
| --- | --- |
| `shoal-core` `server::map::tests::installing_tablets_are_steered_to_another_holder` | An installing tablet is still routed to this node, a tablet not installing is moved, or one with no other holder up is sent nowhere |
| `shoal-core` `server::installing::tests::installs_are_counted_per_tablet` | Two installs over one tablet clear it when the first ends, or the generation does not move |
| The lab rerun (`target/lab/r13/part/rerun184.sh`) | Gets through the node catching up are refused again |

## Related

- [F43](../../features/node-recovery.md), the install.
- [F44](../../features/repair.md), whose quarantined copies were already routed around.
- [Longer partitions, and a flapping one](../../cluster-testing/correctness.md#longer-partitions-and-a-flapping-one),
  where it was found.
