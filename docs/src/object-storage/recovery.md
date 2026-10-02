# S10. Failure, recovery and rebalancing

## Context

A pool's devices fail, fill, are replaced and are added to. A write that committed while a
holder was away has left that holder's piece stale. A device that died has left every stripe
it held one piece short. A device that was added holds nothing and should hold its share.
All three are the same work: **make a piece current on a device where it is not**, by
copying it or by rebuilding it from the others, without disturbing the writes that go on
meanwhile.

This page is how the cluster knows which pieces those are, who does the work, how bytes that
are no longer wanted are removed, and what holds the work to a budget.

## What exists today

For tables, each of these has an answer built on a tablet group's own log.

- **A node that is down keeps its placement through a grace**, thirty minutes by default
  (`shoal-core/src/server/conf/cluster.rs:80`), and only then becomes a removal plan
  ([C3](../distributed/membership.md#what-follows-from-down-and-when)).
- **A replica that returns is fed its group's log, or a snapshot** when it is behind the
  purge point ([C7](../distributed/failover.md#a-returning-node)). What it missed is, by
  construction, the log's suffix.
- **A move is the group's own membership change**, driven phase by phase from a committed
  record (`MovePhase`, `shoal-core/src/server/control/migrate.rs:64`), with the source's
  copy retired for a grace of five minutes (`cluster.rs:757`).
- **A repair is driven by the group's leader**, each step committed before it is taken, so
  that the next leader resumes it (`drive_group`,
  `shoal-core/src/server/shard/repair.rs:827`).
- **Budgets**: 64 MiB/s of stream bytes, two streams assembling a shard, and a reserve of
  1 GiB checked by the receiver before a stream is accepted
  (`cluster.rs:767-784`, `shoal-core/src/server/shard/snapshots.rs:465-484`).
- **A group's derived state survives a restart by a sidecar.** The retry table is written as
  `retries.bin` beside the checkpoint and seeded from it
  (`shoal-core/src/server/wal/mod.rs:97`); it also rides in a snapshot's trailer.

None of it reaches a device of a pool. A piece is in no group's log, a snapshot stream
carries a group's rows and is judged against a group, and the budget is not a device's. The
book says of it: "A node with two storage devices shares one budget"
([F46](../features/capacity-rebalancing.md#limitations)). The code builds it for each shard:
[item 204](../appendix/known-issues.md#204-the-stream-budget-is-built-for-a-shard-and-documented-for-a-node).

## The design

### What can happen to a holder

| Event | How it is known | What follows |
| --- | --- | --- |
| A stage is refused, fails or is late | The stager did not get its answer | The write commits without that holder if the acknowledgement rule allows, and the commit records that it missed |
| A device reports errors, or is gone | Its node marks it failed and reports it; the control group commits the state | Its pieces are rebuilt on other devices at once. A dead disk does not come back holding data, so there is nothing to wait for |
| A device is full | Its node refuses stages past the reserve | It keeps what it holds and takes no more. The planner moves placement groups off it |
| A node is down | The detector, as today | Every device of the node is down with it, and the **member's grace applies**. Nothing is rebuilt until it expires; reads decode and writes proceed while the rule can be met |
| A device or a node returns | A status report | It learns what it missed, and is brought current |
| A device is added | A join, or a restart with a new device | It holds nothing. The planner moves placement groups onto it |

The grace is the Distributed chapter's R3, kept: a node that is down for ten minutes does
not cause a pool's worth of rebuilding.

### How a device learns what it missed

It does not find out by itself. **The tablet group that owns the placement group knows**,
because every commit said so.

1. A commit names the holders its write touched that did not stage
   ([S7](write-path.md#the-preferred-direction-step-by-step)).
2. Every replica of the group derives from that, at apply, a **missed record** for each
   placement group and position: the stripes whose piece at that position is now stale. It
   is derived state, as the retry table is.
3. When the pool map shows the device up, the group's leader runs a **rebuild driver** for
   each of its placement groups with a record against that device. For each stripe it asks
   the holder what label it holds (a lost acknowledgement leaves a piece that is current
   after all), copies or rebuilds the piece if it is stale, and commits the piece's label in
   the stripe's row, conditional on the row's sequence so that it never overwrites a newer
   write. Its progress is committed as it goes, so a new leader resumes.
4. **The record is bounded.** Past the bound the position is marked as needing a backfill,
   and the record is dropped.

This is the reason a placement group is a sub-range of a tablet
([S5](placement.md#a-placement-group-is-a-sub-range-of-a-tablet)): the group that orders a
stripe's writes is the one place that saw every one of them.

Two things about the record are [Q17](contract.md#questions-to-answer)'s. It has to survive
a checkpoint and reach a new replica through a snapshot, as the retry table does, or a
replica built from a snapshot would call stale pieces current. And its granularity is open:
a record of stripes rebuilds a whole piece for a 4 KiB write that was missed; a record of
unit ranges rebuilds less and is larger.

### Backfill

When the record has overflowed, or a device is new or was replaced, the driver compares what
should be there with what is.

- **What should be there** has two sources, because a stripe written only at its object's
  creation has no row ([S3](objects.md#the-two-rows)): the engine's walk of the tablet's
  stripe rows ([S1](prerequisites.md#required)), and the inventories of the placement
  group's other holders, which are directory listings with labels
  ([S6](device-store.md#what-is-in-a-devices-directory)).
- **What is there** is the device's own inventory.

Whatever is missing or stale is rebuilt as above. A backfill costs a walk of the placement
group, where a record costs only what was missed.

### Rebuilding a piece

- **Replicated**: read a current copy, write it whole.
- **Erasure coded**: read `k` current pieces, compute the missing one, write it whole
  ([S8](erasure-coding.md#decoding-and-rebuilding)).

Either way the new piece is staged as a whole piece and made current by a commit of its
label. A rebuild is therefore a write of one piece under [S7](write-path.md)'s rules, and it
inherits them: it is refused if the row has moved, and it never mixes labels.

A corrupt piece is never a source ([P15](contract.md#the-contract)). When fewer than `k`
current, verified pieces remain, the stripe is reported lost by name and nothing is
invented.

### Moves

A change to the pool map that alters where a placement group belongs makes a new
generation. The placement group's pieces are still where the old generation says, and a
**move** takes them to where the new one does.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Planned: the pool map records the move
    Planned --> Both: the group commits that it is moving to the new generation
    Both --> Copied: every piece that changes place is copied or rebuilt
    Copied --> Switched: the group commits that it is at the new generation
    Switched --> Retired: the old holders drop their pieces, after a grace
    Retired --> [*]
```

While the group is in `Both`, a write stages on the devices of both generations and its
commit names both, so nothing written during the move is missing from either side
([S5](placement.md#generations)). Reads use the old generation until the switch. The copy is
the rebuild above with a holder to copy from, and a move whose old holder is gone is a
rebuild.

The planner decides which placement groups move and in what order, one move a device at a
time, by the bytes each holds and the space each device has, as it plans replica sets today
([C8](../distributed/rebalancing.md#plans)). It is the same pure function over a different
unit.

### Reclamation

Bytes that are no longer wanted:

| What | The committed fact that allows it |
| --- | --- |
| A staged write that lost | The stripe's row names another tag at a higher sequence |
| The pieces of a replaced or deleted object, or of a put that was abandoned | The path's entry names the object's id as retired, or names it nowhere |
| The stripes past a truncate | The entry's floor covers them |
| The pieces left behind by a move | The group's generation has moved past the one they sit under |
| A piece nothing explains | The same facts, asked for by a light scrub ([S11](scrub.md)) |

[P16](contract.md#the-contract) is the whole of the rule: a holder discards on a committed
fact that cannot be undone, and never on a timer.

**Absence is judged by a strong read.** "The entry names this id nowhere" is safe only
because an id is recorded before any piece is written for it and is never reused, so once
it has been recorded and removed it stays gone. But a lagging replica may simply not have
applied the recording yet. A holder that discarded on that replica's word would destroy a
put in flight. So the question is asked behind a read barrier, and a default read is never
an answer to it.

**Readers get a grace, and a grace is not a permission.** The pieces of a retired object
are kept for a while so that a reader that began before the replace can finish. When they
go, a reader still at it fails by name; it is never given other bytes, since an id is never
reused.

### Budgets

Recovery, moves and scrubs are held to **bytes a second for each device**, as a source and
as a destination, and to a number of rebuilds a device runs at once. A node's single budget
cannot be right for both a rotational disk and an SSD beside it: sized for the SSD it
saturates the disk, and sized for the disk it leaves the SSD's rebuild slow and its pool
exposed for longer.

The reserve is checked where the bytes land, before a rebuild's piece is staged, as a
snapshot stream's is today. What the budgets should be is
[Q29](contract.md#questions-to-answer), and how they sit beside foreground work is
[S13](isolation.md).

## Alternatives rejected

**A log on every device**, as a RADOS placement group has. It is what makes Ceph's recovery
local and fast, and it is a second replicated log to keep consistent with the first. The
group already has the facts.

**Finding stale pieces by walking every row.** No scan exists for a client, and the
engine's walk is of a whole table on a shard. It is the fallback, not the method.

**A stand-in device** that takes a down holder's writes and hands them back. It is another
place a piece might be, and another thing a read has to consult.

**Rebuilding the moment a node is called down.** R3, again.

**Reclaiming by age.** A put that takes longer than the age loses its pieces before it
commits.

**A commit for every stripe a move touches.** It makes the move's cost linear in commits.
The generation is the placement group's, and one commit moves it.

## What it costs

- **State in each tablet group**: a generation and a bounded missed record for each of its
  placement groups, persisted with the checkpoint and carried in a snapshot.
- **A commit for every piece rebuilt**, since a rebuild makes a piece current through its
  row. A stripe with no row gains one when a piece of it is rebuilt.
- **`k` reads for one piece** under an erasure code, most of them over the network. On the
  lab's 1 GbE that bounds a rebuild near 117 MiB/s divided by `k`
  ([X12](spikes.md#x12-recovery-and-scrub-rates)).
- **Space on both sides of a move** until the old side is retired.
- **A backfill walks a placement group**, and today's walk filters a whole table's keys on a
  shard to find one tablet's.

## What it breaks

- "A group's persisted state is its checkpoint and its retry table."
- "A move is a group's membership change": a placement group's move changes no membership.
- "A returning replica is fed a log": a returning device is fed pieces, chosen by a record.
- "The stream budget is the node's" ([Q8's remainder](../distributed/open-issues.md#not-settled)):
  a device has its own.

## Invariants to uphold

- A piece becomes current only by a commit of its label in the stripe's row. A rebuild, a
  move and a write are alike in that.
- A missed record is derived at apply, by every replica, from committed commands alone.
- A driver asks a holder what it holds before it moves a byte.
- A corrupt or stale piece is never a source.
- A placement group's generation moves only after every piece that changes place is current
  on its new device.
- A holder discards only on a committed fact, and absence is established behind a barrier.
- A node that is down is not rebuilt around until its grace expires; a device that failed
  on a live node is.
- The reserve is checked where the bytes land.

## Prerequisites

[S1](prerequisites.md#required): the engine's walk of a tablet's rows, free bytes for every
root, and the fixture's device faults. [S5](placement.md), [S6](device-store.md) and
[S7](write-path.md).

## How it would be measured

[X12](spikes.md#x12-recovery-and-scrub-rates): bytes a second a device is rebuilt at, for a
copy and for a decode, on SSD and on a rotational disk, under a budget; from which, how long
a device of a given size is exposed. In a running cluster, the event arms of
[S15](performance.md): a device killed, a node killed and returned, a device added, each
recording what the foreground's tail did before, during and after, as the rebalance arms
record it today.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `returning_device_rebuilds_exactly_what_it_missed` | A device isolated through a run of writes is brought current by rebuilding those stripes and no others | M16 |
| `missed_record_survives_restart_snapshot_and_leader_change` | A replica restarted, or built from a snapshot, or newly leading, holds the same record | M16 |
| `overflowed_record_falls_back_to_backfill` | A device away past the bound is brought current by a backfill, including stripes that have no row | M16 |
| `rebuild_never_overwrites_a_newer_write` | A rebuild racing a write to the same stripe is refused and tried again against the new row | M16 |
| `move_serves_reads_and_writes_throughout` | Every acknowledged write during a move is on the new devices after the switch; no read fails for the move | M16 |
| `node_down_inside_its_grace_rebuilds_nothing` | A node killed and returned inside the grace causes no rebuild, only the catch-up of what it missed | M16 |
| `discard_requires_a_committed_fact` | No holder drops a piece on a timer, on a default read's absence, or on a stager's word | M20 |
| `retired_object_is_reclaimed` | After a replace and the grace, the old object's pieces and rows are gone and a reader of them fails by name | M20 |

## Related

[S7](write-path.md) for the commit a rebuild uses; [S5](placement.md) for generations;
[S11](scrub.md) for how damage is found; [S14](operations.md) for adding and removing a
device; [C7](../distributed/failover.md) and [C8](../distributed/rebalancing.md) for the
tablet versions of this; [F44](../features/repair.md) for the driver pattern.
