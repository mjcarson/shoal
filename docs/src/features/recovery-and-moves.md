# F88 — M16, recovery and moves

## Context

[M16](../object-storage/milestones.md#m16-recovery) is the object store's sixth gate. After
[F86](replicated-pools.md) a holder that missed a write stayed stale for good: reads went round it
and nothing rebuilt it. A failed device was only flagged, its chunks one copy short until an
operator did something no operation offered. And every pool was **pinned** at the generation it was
first ready at, so a device that joined after the pin was never placed on and nothing could move.
This gate builds [S10](../object-storage/recovery.md), reclamation apart, and
[S5](../object-storage/placement.md)'s generation and position rows: a stale chunk made current
again, a failed device rebuilt elsewhere at once, a down node's devices held through its grace, and
placement groups moved between pool map generations while writes go on, all of it paced by a
slice's idle time under a ceiling for each device ([X12](../object-storage/recovery-scrub-rates.md)).

Every row of [S1](../object-storage/prerequisites.md#required) was done before it; the last,
the tablet walk, was [F87](tablet-walk.md). What stood open was S18's: Q17's bound and how its
record survives a checkpoint and a snapshot, and Q29's objective for an SSD. Four decisions were
taken with the user on 2026-10-14, before anything was built:

- **The stripe model is extended to the move first.** [X1](../object-storage/stripe-model.md)
  moved one stripe's position at a time with a generation on every row, and listed the `Both`
  phase as unchecked. The gate's rule is that an unchecked path is not a candidate, so the model
  took a placement group's state, its record, a rebuild and a move under continuous writes before
  any engine code ([the record](../object-storage/stripe-model.md#a-placement-groups-state-and-its-moves)).
- **Q17's granularity is built as X12 recorded it**: unit ranges up to the destination's
  break-even, past which the chunk is rebuilt whole.
- **Q17's bound is 4,096 stripes a position of a placement group**, configurable on the pool.
- **An SSD's foreground through a rebuild or a move is held to a p99 of at most
  `max(2 × its own, its own + 1 ms)`**: X12 found no pace that rebuilt anything kept an SSD under
  twice its own, whose own is tens to hundreds of microseconds.

The other decisions were mine, and are under **Design choices** with their reasons.

## What it does

### Configuration

Nothing new is required. Wire version 10 is the recovery wire: until a cluster activates it no
placement group's state changes, nothing is rebuilt or moved, and a pool stays at its pin, as F86
left it. `shoaladm` bootstraps a cluster at the version its build speaks, which is 10 now.

```yaml
storage:
  devices:
    - {path: /xfs/shoal-devices/d0, class: ssd, recovery_ceiling: 256MiB}   # 64MiB on a disk
  pools:
    fast: {class: ssd, redundancy: {replicas: 3}, missed_record_stripes: 4096}
  object_timings:
    recovery: true            # off only for a test of a holder that stays stale
    recovery_poll: 500ms      # how often a shard reads its led groups' records for work
    recovery_inflight: 4      # rebuilds a shard runs at once
    moves_inflight: 16        # placement group moves a shard runs at once
```

`recovery_ceiling` is the bytes a second a device's rebuilds and moves move together, as a source
and as a destination: 256 MiB/s for an SSD and 64 MiB/s for anything else unless set. Since
[F91](rotational-devices.md) an SSD is a device the kernel calls solid state, where it was a class of
`ssd`.
`missed_record_stripes` is policy, committed with the pool, since every copy of a tablet group
derives the record and has to bound it alike.

### A placement group's state, in its tablet group

A placement group is a sub-range of one tablet's keys ([S5](../object-storage/placement.md#a-placement-group-is-a-sub-range-of-a-tablet)),
so the tablet group that orders its stripes' writes is the one place that saw every one of them.
Each copy of the group's `MachineState` holds, for every placement group that ever moved or missed
anything (`shoal-proto/src/shared/placement_state.rs`):

- an **epoch**, moved by every command that changes where the group's chunks are staged or read;
- the **generation** its chunks sit under and its **positions** there, a permutation of at most
  sixteen, and the **move** in flight, its id, its generation and the positions it will have;
- the **missed record**: for each position, the stripes it is stale on, each entry the label the
  row named when it began (`since`) and the label and units of every write it missed since, up to
  sixteen, past which it is the whole chunk; past `missed_record_stripes` stripes the position is
  **overflowed**, and its drops are counted.

A placement group with none of these is at its pool's **base**, the generation the pool was pinned
at in the rule's order, so nothing is kept for one that never moved or missed anything, and an
apply never reads the pool map.

**It is persisted as of the checkpoint exactly.** Replay above a checkpoint applies the commands
again, so what is saved must be the checkpoint's own state, or a claim accepted at the checkpoint
plus one would be refused on replay. The state is kept as a base and the changes applied since
(`PlacementHistory`), folded as the checkpoint advances and written **inside the retry table's
sidecar** (`GroupRetries`, `wal/mod.rs`), so staging, promotion, recovery and rehome carry it under
the one atomic rename they already had. A snapshot's trailer is **tagged sections**, format 3, with
the placement state as of the snapshot's boundary beside the retry table; an install, a restore and
a rehome set it, a restore since [F93](operations-and-the-real-cluster.md#a-restore-judged-table-by-table)
to an empty state, since a source group's placement names the source's pool map and never travels. A sidecar that does not describe the checkpoint leaves the copy's placement
**unknown**, and a stripe commit on a bucket's table **stalls** the copy for a snapshot rather than
being judged against a base that may be wrong.

### The commands, and what they are never

`MoveBegin`, `MoveSwitch`, `MoveAbort` and `BackfillDone` are proposed on a reserved tablet,
`PLACEMENT_TABLET`, `u16::MAX - 1`, in a fixed little-endian encoding, as a scrub is on its own.
Each names the state it expects and is refused, changing nothing, at any other: a begin names the
epoch its driver read and a permutation, a switch and an abort the move's id, a backfill's done
the drops its record had counted when it began. They skip deduplication and the write counters,
and, like a scrub, **are never indexed for compaction**: a compactor that met one would read its
payload as a row.

### Every stripe commit claims an epoch

A stripe row carries three new fields: `placement_epoch`, the epoch the commit was staged under;
`changed`, the units it changed (whole for a chunk or a put, the pending units for a clear); and
`prior`, the label the row named before. **After deduplication and before the row is applied**, a
stripe commit whose epoch is not its placement group's is answered `Refused(Placement)`
(`ConditionRefusal::Placement`), remembered in the retry table as F68's refusals are. A no-op, a
truncate's fence and a reclamation change no label and claim any epoch (`ANY_EPOCH`).

A refused commit's frame is still in the WAL, and a compactor re-judges only F68's condition, so
**the frame is excluded from compaction** (`ShardWal::exclude`), honoured by every pass and every
re-merge of a repair's tail until the checkpoint passes it. Without the exclusion the refused row
would be folded into the archives.

After a stripe commit applies, the record is derived from the committed row alone: a position the
row marks missed gains an entry for the write (`RecordInput::of`), a position the row calls current
loses its entry. Every copy derives the same record, so a new leader holds what the old one did.

### How a writer learns its group's state

Each shard's metadata client keeps the state it last read of each group (`MetaClient`); one it never
read is taken at its base while its pool's target has not moved past its pin, and read once it has
([Resolved #246](../appendix/resolved/moved-group-taken-at-base.md)). A stale state is never unsafe: a commit that claims an epoch its group
moved past is refused. A refusal makes the writer read the group's state again, linearizably,
through `ReplicateKind::Placement` (13): the group's leader takes a read barrier, waits for its own
apply through it, and answers each group asked for. A put reads it linearizably once its pieces are
durable, always.

**While a group moves**, its `StripeSet` (`shard/objects/place.rs`) is the old generation's holders
and each moving position's arrival. A write, a small write's confirmation, a fold, a clear and a
put's piece go to all of them, and **a moving position counts as holding a label only when both its
slices hold it**. Reads stay on the old generation until the switch; a read none of whose holders
gives the bytes reads the group's state again, linearizably, and asks where a group that moved is
now.

**A put checks after its pieces are durable.** A put writes stripes that have no row. Once its
pieces are durable it reads the group's state again; a group that began moving, or moved, since it
placed gets a row for the stripe at the epoch now, with every position its pieces did not reach
under that state marked missed. A stripe held at fewer than `k + f` positions under it fails the
put `NotReady`, which the client retries under the put's identity.

### Rebuilding a stale position

Every shard runs one recovery task (`shard/objects/recover.rs`) over the record of every bucket
stripe group it leads, every `recovery_poll`, `recovery_inflight` at once:

1. a stale position's row is read at `Quorum`, and its group's state linearizably; a group that
   is moving is left until it switches or aborts, and a holder that is not serving (its device
   failed, its member not up) is skipped;
2. the holder is asked which label it holds (`Confirm`). One already at the row's label, a lost
   acknowledgement, is only committed current;
3. otherwise the bytes are read from a holder the row calls current and written to it: **the units
   of every write the record says it missed since the label it holds**, when the entry knows them,
   the holder is at or past where the entry began, and the run is at most half the chunk, as a
   stage over that label (`StageUnits`, whose `expects` names the label it holds and every write's
   label between) applied at once; and otherwise **the whole chunk, in place at the row's label**,
   in pieces (`Write`);
4. the row is committed with the position current (`rebuilt`), conditioned on the sequence read
   and claiming the epoch read. A refusal leaves the position marked, for the next poll.

**A source is read at the row's label exactly**, so over a small write's pending bytes only a holder
that folded them gives its bytes, and a chunk is never copied from their base under the row's label
(X1's `rebuild_copies_the_base`); a move's copier reads the same way. **A rebuild is never staged and
discarded**: a whole chunk is written at a committed label, and a stage of units is applied at once. **A slice never applies a whole chunk older than the one it
holds**, which `apply_whole` did not check before; a holder already past the label refuses the
chunk and keeps what it has.

### Backfill

A position whose record overflowed is walked instead: F87's walk of the placement group's range of
its tablet, filtered by consumer, rebuilding every row that marks the position. Once every one of
them was rebuilt or already current, and none was put off, the group commits `BackfillDone` with
the drops it read, which clears the overflow only if nothing was dropped meanwhile. A stripe a
backfill could not reach leaves the position overflowed for the next.

### Budgets

Every request a rebuild or a move makes of a slice is **background**: a flag on the object lane's
`ChunkRequest`. Each object executor counts its slices' foreground requests in flight (`SliceState`,
`devices/mod.rs`); a background request waits until its slice has none, and since
[F91](rotational-devices.md) no apply in flight either, then draws its bytes from
the device's **ceiling**, a token bucket every executor serving one of the device's slices shares.
A confirmation and a listing move no bytes and do not wait ([Resolved #243](../appendix/resolved/move-slots-on-the-map.md)).
A source's executor gates its own reads the same way, so a rebuild waits on both ends. A slice
that is never idle rebuilds nothing, which `pools` shows as stale positions that do not fall.

### A failed device, a down node

- **A device fails on a node that is up**: the node reports it, the control group commits the
  failure (F85's `ReportDevices`), and the leader commits `RetireDevice`, which ends the device's
  era and moves the generation. Its placement groups move off at once; a move whose old holder is
  gone copies from another position, and marks the stripe missed where none can give it.
- **A node is down**: nothing moves inside its grace. Writes go on without its holders, which are
  marked missed in the rows, and when it returns the rebuild task catches up exactly what the record
  names. Past the grace F46's expiry tombstones the member, its eras end, and its groups move.
- **An operator fails a device**: `fail_device <device>` (admins only) commits an order on the
  device's record; its node carries it out on its next tick as an I/O error would, and its report
  of the failure clears the order. The device then retires as any failed device does.

### Moves

A pool's **target** is the newest generation it is fitted and ready at, which the control leader
commits (`TargetPool`), forward only and never past the map. Every shard runs a move task
(`shard/objects/moves.rs`) over the groups it leads: a placement group whose holders at its own
generation differ from the target's is moved; one whose holders are the same, as a set, stays where
it is with nothing committed.

1. **Admission**: the control group admits the move (`AdmitMove`) unless sixteen admitted moves
   already arrive on one of its devices (`MOVES_A_DEVICE`); the pool map's `moves` holds the slot.
   A driver reads the slots its control thread has applied (`ControlRequest::MoveSlots`) and asks
   only for devices with room, and a slot moves no topology version: no shard routes by it.
   ~~Every move is admitted.~~ Since [Resolved #250](../appendix/resolved/empty-groups-move-without-a-slot.md)
   a group whose old holders list nothing of it begins with no slot, since nothing arrives. Its
   copier takes one before its first byte moves if a put landed since, and is given up if refused.
   Those moves run first, within a budget of their own (`moves_inflight` again), the groups still
   placing on a slice that serves nothing ahead of the rest.
2. **Begin**: `MoveBegin`, at the epoch read, with the target's positions: staying members keep
   theirs and arrivals take the vacated ones in rule order. The group is in both generations.
3. **Copy**: every row the walk of the group's range finds, at the label it names, and every stripe
   a put wrote whole and no row names, which the old holders' listings find (`List`, a page of a
   placement group's chunks a request), goes onto its arrival whole. A stripe no source can give is
   marked missed if its old slice is gone; otherwise the move is aborted, for a later one. **A move
   is aborted too if no old holder but a moving position's own was listed.**
4. **Switch**: once every arrival is serving, `MoveSwitch` records the generation and positions in
   one commit, and the slot is freed (`MoveDone`) where the move took one. An arrival not serving is waited for up to 30 s,
   the map read again every half second, since a node the control leader's detector calls down for a
   moment under load is up again in seconds; one still down then gives the move up
   ([Resolved #281](../appendix/resolved/move-waits-for-its-arrival.md)).

A group found moving is one a driver began: a new leader copies and switches it again if the control
group still holds its slot, as its control thread answers, and aborts it if not, so a move begun with
no slot is its own driver's alone, and a new leader gives it up and lists the group again; a slot held for a
group that is not moving, left by a `MoveDone` that was lost, is freed. The old holders' chunks
stay where they are; removing them is M20's. ✅ Since [F92](reclamation-and-device-lifecycle.md#what-a-move-leaves)
a switch names the slices it leaves in the group's state, and the group's leader releases each past
its grace.

### Operations

- **`pools`** shows each pool's target, the moves admitted, the groups still behind their target,
  the stale and overflowed positions the records name, what was rebuilt, backfilled, switched and
  aborted, and the devices retired. The figures are counted on the shards that lead each group
  (`ShardReplication.recovery`), summed in `NodeStats.recovery` and again by the leader.
- **`fail-device <device>`** in the cluster tab and `shoaladm admin`.
- **The bench** has two new events, an object arm's: `device_fail`, the operator's order on one of
  the victim's devices; and `device_add`, a node restarted with a device added, set aside before
  the arm's preload by failing it and emptying its directory at the event, as a new disk's is. Both
  are done once every pool's moves settled: its target moved, nothing in flight or behind, two
  reads in a row, the stale positions then the done mark's note. A loaded pool may hold some:
  a put marks a holder slower than its straggler wait missed, for a rebuild in idle time ([O112](../appendix/optimizations.md#o112-a-put-under-load-marks-its-slow-holder-missed-and-hands-recovery-a-rebuild)),
  and one run's climbed after its moves ([item 247](../appendix/known-issues.md#247-a-failed-devices-pool-gathered-stale-positions-after-its-moves-settled)). Object arms carry `kill` and `stop` too, and an event arm's windows count an
  object arm's operations, which are kinds of their own.

### The fixture

`PLACEMENT` answers every group's state on a node; `PLACEMENT begin|switch|abort` drives a move by
hand. `RECOVERY on|off` pauses every rebuild and move driver, and `HOLD_REBUILD <ms>` holds each
rebuild between its copy and its commit. `OBJECTS` reports each slice's background bytes. A
cluster built with `.recovery(false)` rebuilds nothing, which the four M15 tests of a holder that
stays stale use.

### Found on the way

Each found by an acceptance test before anything was committed:

- **A refused begin leaked its slot**, and a group that had moved and aborted, at an epoch past
  zero, was left out of the groups a driver leads, so the driver kept proposing a begin from epoch
  zero. Both are fixed, and a walk page shed under load is asked again (the backfill's too, since
  [Resolved #345](../appendix/resolved/backfill-shed-page.md), every driver's through its walk).
- **A retired device was observed again**: its node still names it, failed, and the map began an
  era for it, which the leader retired again. The generation climbed to 54 before the test caught
  it. The map skips a device retired for a failure the map still holds, and its node no longer
  proposes it.
- **A writer refused for its epoch forgot the group's state** and took it at its base again,
  claiming the base's epoch forever; it reads the state again now.
- **A backfill declared its record whole** while stripes it had put off were still stale. Its
  rebuilds say now whether each was done, current or put off.
- **A placement group's moves were too slow to finish a failed device's** within the test's
  minutes: one move a device and one a shard. Sixteen of each, and a poll that found work looks
  again after 20 ms.

## Design choices

- **An epoch, not a generation, is what a commit claims.** A generation says where a group's chunks
  sit; an epoch moves on every command that changes where they are staged or read, an abort among
  them, which leaves the generation where it was. The model held X1's check of the generation, moved
  to the epoch, to P8.
- **The state is the group's, the record derived.** Nothing new is replicated for a record: every
  copy derives it from committed rows, which is what lets a snapshot, a restart and a new leader
  agree on it ([X1](../object-storage/stripe-model.md#a-placement-groups-state-and-its-moves)).
- **A record a position, up to a bound, then a walk.** 4,096 stripes is about a second of a group's
  top commit rate ([X10](../object-storage/stripe-row-costs.md)), minutes at ordinary rates; past
  it, a walk of one placement group costs what F87 measured, a page's tenth of a millisecond.
- **Units to the half chunk.** X12's break-even was 3 to 4 units on a disk and 12 to 38 on the
  lab's SSDs; half a chunk is past all of them, so it is the bound a rebuild by units cannot be
  worse than a whole one under.
- **The driver reads and writes, and the destination never fetches.** S10 sketched a request to the
  destination naming its source. A driver on the group's leader that reads a current holder and
  writes the other keeps every request a destination serves one it already served (a write, a stage,
  an apply), and the background flag is the one change to the lane's requests.
- **Unit checksums are computed from the bytes read.** The source verifies each unit's checksum as
  it reads it, and the write computes them again, so nothing rebinds a checksum to another
  position's identity.
- **A move's id is the epoch it begins at.** The epoch is unique to the group and only rises, so the
  id needs no minting of its own and a switch or an abort names exactly one begin.
- **Admission counts moves arriving on a device**, sixteen at once: each moves what one placement
  group holds of the device, and the ceiling bounds their bytes together. One at a time moved a
  failed device's two thousand groups at one every few round trips; sixteen move them in two and a
  half minutes on the fixture, an admission asked for only where the applied slots leave room.
  ~~Every move takes a slot.~~ Since [Resolved #250](../appendix/resolved/empty-groups-move-without-a-slot.md)
  a group that holds nothing takes none: nothing arrives, so it costs no device's budget, and the
  groups still placing on a failed device are off it once their empty moves run, which wait behind
  no copy.
- **The engine keeps three rules the model did not need**: it lists every old holder it reaches, a
  backfill's done names the drops it read, and an overflowed entry is rebuilt whole. What the engine
  lists and drops is more than the model's two stripes reach.
- **A failed device on a live node retires at once; a down node is held.** A device that failed on
  a node that is up never comes back by itself, so waiting for a grace that never comes leaves its
  stripes short; a node that is down may return holding everything, which is R3.
- **An operator's failure is an order the node carries out.** The control group cannot fail a
  device a node is serving; the node can, as an I/O error would, and its report is what every other
  path already acts on.

## Alternatives rejected

- **A generation and positions on every row**, X1's. Every row of a group moving would be a commit;
  one switch moves the group.
- **Falling back to the base when the sidecar does not match.** A base that may be wrong lets a
  stale claim land; stalling the copy for a snapshot cannot.
- **Judging the claim in the compactor.** It would have to read the group's state as of every frame
  it folds; excluding the refused frame is one set.
- **Staging a rebuild and discarding it when its commit is refused.** A committed row named that
  label, so no fact excludes it, and discarding it broke P16 (`rebuild_drops_its_stage_when_refused`).
- **Counting a moving position on its old holder alone.** A write acknowledged there is missing from
  the arrival after the switch (`both_counts_the_old_holder_alone`, P17).
- **Switching before the copier finishes**, and a whole apply over a newer chunk, each P17 in the
  model.
- **Minting move ids in the control group.** A second counter to keep consistent with the epoch.
- **A planner over placement groups by bytes and free space.** Admission is a count a device; what a
  move costs a device's reserve is filed below.

## Limitations

- **A standalone node recovers nothing.** It has no tablet groups to hold a record, and stays at its
  pin as F86 left it.
- ~~**Nothing a move leaves behind is removed**: the old holders' chunks stay until M20, and so does a
  retired device's record on the map.~~ ✅ Since [F92](reclamation-and-device-lifecycle.md) every slice
  a move leaves is released past its grace, and a retired device is removed by `remove-device`.
- **Admission does not check a device's reserve**, nor plan by the bytes a group holds; a move onto
  a full device is refused at its stage like any write. A host left with one small device after a
  failure fills it, and from then every put marks its position missed and nothing is rebuilt there
  ([item 247](../appendix/known-issues.md#247-a-failed-devices-pool-gathered-stale-positions-after-its-moves-settled)).
- **A coordinator's placement cached before a group moved stages on the slice it left once more**:
  the put's check reads the group again, marks the arrival missed and refreshes the cache, so each
  coordinating shard costs a moved group one rebuilt position, at 2+1 one put refused `NotReady`
  and tried again. Reading a cached view again whenever it names a slice that serves nothing would
  spare it, at a check on every put's placement for as long as any device is removed or retired
  ([Resolved #250](../appendix/resolved/empty-groups-move-without-a-slot.md), Alternatives rejected).
- **A pinned pool is never fitted again.** Its target generation places with the weights and
  exceptions fitted before its pin, so a device that joins is placed on by its capacity weight.
- **A rebuild's and a move's bytes pass through the node that drives them**, which reads the source
  and writes the destination; filed as O110.
- **A slice that is never idle rebuilds nothing.** Stated by S10 and shown by `pools`.
- **A units rebuild does not compare the chunk's checksums with its source's** afterwards; each
  unit's checksum is of the bytes read.
- ~~**An erasure coded pool** has the record and the moves but takes no write before M18, which
  rebuilds from `k` chunks.~~ ✅ [F90](erasure-coded-pools.md#recovery-and-moves) writes one, and
  rebuilds and moves its positions by decode from `k` chunks that verify.

## Invariants to uphold

- **A chunk becomes current only by a commit of its label in its row**, claiming the epoch it was
  staged under; a rebuild, a move and a write alike.
- **The record is derived at apply from committed rows alone**, by every copy.
- **The placement state saved is the checkpoint's own**, and a copy that cannot know it stalls; it
  never falls back to the base.
- **A placement command and a refused stripe frame never reach a compactor.**
- **A moving position counts as holding a label only when both its slices hold it**, for every kind
  of write, confirmation, fold and put.
- **A switch comes after the copier, and a move none of whose other old holders was listed is
  aborted, never switched.**
- **A rebuild is never discarded, and no slice applies a whole chunk older than the one it holds.**
- **A rebuild's units stand over a label at or past where the record's entry began.**
- **A background request that moves bytes waits for its slice's foreground and its device's
  ceiling, at both ends**; a confirmation and a listing alone do not.
- **A move's slot is judged by the control thread's applied state, never by a shard's map**, which
  no slot change moves.
- **A slot bounds bytes, never safety.** What a move copies is what its walk and its listings after
  the begin find, with a slot or without, and a move begun with none takes one before its first byte
  arrives ([Resolved #250](../appendix/resolved/empty-groups-move-without-a-slot.md)).
- **A move begun with no slot is its own driver's alone**: a driver that finds one moving with no
  slot gives it up, and the next poll lists the group again.
- **A down node is not moved off inside its grace; a failed device on a node that is up is.**

## Performance

### On the lab

M16's evidence was taken with `shoaladm bench` from europa (a Ryzen 9 7945HX) on 2026-10-15, at
`1220ff0`, whose engine is `baf6335`'s: the commit after it changed only the bench. The captures
are under the bench worktree's `target/shoaladm-bench/` and are not committed. Every node ran the
one node program `target/deploy` builds for the Zen1 hosts (znver1), the tmdb cluster's, and every
host ran the `performance` governor for its runs.

- **The cluster.** The bench's own copy of `tmdb_cluster.yaml`: three nodes, three copies a host
  apart, each node with two pool devices. europa's are a directory on its Optane (`nvme1n1p1`,
  which its roots share) and a 40 GiB XFS image on the Optane through a loop device (`/optane2`,
  `loop6`); titan's and hyperion's are the `/xfs` volume on their 970 EVO (`dm-1`) and a 40 GiB XFS
  volume beside it (`/xfs2`, `dm-2`), their roots on `dm-0`. `shoal-spike/results/m16-lab.sh` made
  the second devices and records every host change in `m16-host-changes.txt`.
- **The arms.** `rw50` at 1 MiB, two in flight, 90 seconds measured after five of warmup, half of
  64 seeded objects preloaded, every run on a cluster of its own. Four events, three runs each, in
  an order that rotates by round: `none`; `kill`, titan killed a third of the way in and started
  two thirds in, inside its grace; `device_fail`, an operator's `fail-device` of titan's `/xfs` a
  third of the way in; and `device_add`, titan's `/xfs2` set aside before the preload and titan
  started again with it emptied a third of the way in, as a new disk is. A device arm runs on until its
  pool's moves settle, then five seconds more, so its measured window is the whole of its moves.
  No operation failed, and every acknowledged put was read back, none lost or misread.
- **The windows.** An event arm's `before` runs from the start of the measured window to the event,
  `during` from the event to its done mark, and `after` to the end. The steady arm runs at about
  24 operations a second of each kind in its first 30 seconds and 17 to 19 after, on its own, so a
  `before` window is the fastest part of a run and the steady arm's whole run is read beside it.
- **Two in flight.** F86's arms ran eight, at which the load alone has the lab's 970 EVOs at their
  limit, so a failure's cost and its moves' cannot be told apart
  ([below](#eight-in-flight)). Two leaves them room: the steady arm ran titan's and hyperion's
  devices 29 to 62% busy.

Every figure is `median [lowest–highest]` across the runs. The legs again, from
`examples/tmdb_dataset` with a copy of `tmdb_cluster.yaml` under another name, the seeded posters
committed beside the lab script; the rate leg is `--objects get100 --object-sizes 256MiB
--concurrency 1 --events device_fail --duration 60 --runs 2`, and eight in flight
`--concurrency 8 --events device_fail --duration 60 --runs 1`:

```sh
sh shoal-spike/results/m16-lab.sh setup     # the second devices, once
../../target/deploy/release/tmdb-dataset-loader bench run -i <copy> \
    --dataset ../../shoal-spike/results/m16-objects --objects rw50 --object-sizes 1MiB \
    --concurrency 2 --events none,kill,device_fail,device_add --duration 90 --warmup 5 --runs 3 \
    --event-timeout 600 --governor performance --basic --stop-unit shoal-tmdb
# beside a run, and the moves' timings from each node's journal after it
ssh titan sh -s < shoal-spike/results/m16-sample.sh > titan.log &
python3 shoal-spike/results/m16-rate.py titan.log dm-2 enp3s0 <from> <to>
ssh titan journalctl -u shoal-<copy>-bench --since <t> --until <t> | python3 shoal-spike/results/m16-moves.py 6
```

#### The foreground through each event

The measured window of each arm (`f88-events-1220ff0`). A device arm's is the whole of its moves,
242 to 492 seconds; the others' are 90.

| Arm | get ops/s | get p50 ms | get p99 ms | put ops/s | put p50 ms | put p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `none` | 20.6 [18.9–20.8] | 21.7 [3.09–22.0] | 56.6 [53.3–59.7] | 19.3 [19.0–20.3] | 81.6 [80.2–86.1] | 135 [133–138] |
| `kill` | 19.6 [19.1–21.6] | 2.71 [2.62–16.5] | 47.9 [47.5–50.9] | 20.1 [19.3–20.3] | 76.0 [72.1–76.3] | 221 [178–221] |
| `device_fail` | 16.1 [15.9–17.0] | 10.3 [6.07–19.4] | 66.9 [62.0–66.9] | 16.3 [16.3–16.4] | 104 [103–107] | 176 [171–179] |
| `device_add` | 15.5 [15.5–15.8] | 15.5 [7.86–24.9] | 70.5 [69.0–70.9] | 15.5 [15.2–15.6] | 107 [105–109] | 201 [197–211] |

The p99 of each window beside the event, in ms:

| Arm | Kind | `before` | `during` | `after` | `during` over `before` | `during` over the steady arm |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `device_fail` | get | 46.5 [44.7–49.5] | 67.5 [63.3–68.2] | 70.4 [63.3–77.4] | 1.45 | 1.19 |
| `device_fail` | put | 122 [121–133] | 180 [174–182] | 140 [139–142] | 1.48 | 1.34 |
| `device_add` | get | 42.9 [42.9–61.5] | 72.1 [71.7–72.1] | 51.7 [50.4–65.7] | 1.68 | 1.27 |
| `device_add` | put | 124 [124–126] | 204 [199–213] | 150 [147–158] | 1.65 | 1.51 |
| `kill` | get | 40.6 [38.9–43.7] | | 51.3 [50.3–56.7] | | |
| `kill` | put | 128 [121–132] | | 250 [209–258] | | |

The bench's own figure for each device run, the worst kind's `during` p99 over its `before`, was
1.44 to 1.51 for a failure and 1.57 to 1.72 for an addition. A kill's `during` is the kill itself,
and its `after` holds both titan's half minute away and its return.

**Q29's objective for an SSD holds at two in flight, read at the client**, and not at eight
([below](#eight-in-flight)). Through a failure's moves and an addition's, each kind's p99 was 1.45
to 1.68 times its `before`, and the bench's worst kind of any
run 1.72; against the steady arm, 1.19 to 1.51. The objective's other term, its own plus a
millisecond, plays no part at these figures: they are 1 MiB operations over 1 GbE, tens to hundreds
of milliseconds, not the device's own tens to hundreds of microseconds, which the bench does not
see. The rate fell with the latency, two in flight being a closed loop: 15 to 16 operations a
second of each kind through the moves, against the steady arm's 17 to 19 after its first half
minute.

#### The moves

Every placement group of the failed device moved, and every one the added device was given, from
the nodes' own logs (`a placement group began its move`, `switched to its new generation`):

| Run | Groups moved | Event to done, s | Given up | Rebuilt | Stale at done |
| --- | ---: | ---: | ---: | ---: | ---: |
| `device_fail` 1 | 2,732 | 285 | 1 | 1,037 | 0 |
| `device_fail` 2 | 2,732 | 202 | 0 | 726 | 1 |
| `device_fail` 3 | 2,732 | 281 | 24 | 930 | 1 |
| `device_add` 1 | 3,487 | 324 | 1 | 315 | 43 |
| `device_add` 2 | 2,753 | 295 | 0 | 166 | 0 |
| `device_add` 3 | 3,944 | 451 | 1 | 388 | 0 |

**A move's own work is small; its two control commits are what it waits for.** From its begin to
its switch a move took 30 to 150 ms at the median and at most 1.06 s, in every run. The moves
europa's shards led, on the control leader's node, were done in one to two and a half minutes, at
11 to 29 a second. titan's and hyperion's went on at 1.8 to 2.7 a second to the end of every run.
Every proposal slower than 250 ms is logged, and there were thousands a run on each of those two
nodes, admissions asked again for a full device among them: they took 0.76 to 3.5 s at the median
and up to 6.2 s, where europa's own slow ones took 0.27 s. With nothing written beside them, in the
rate leg below, the same members' took 0.36 to 0.47 s and their moves ran at 9 to 11 a second. That
is
[O111](../appendix/optimizations.md#o111-a-placement-groups-move-costs-two-control-commits-beside-its-own-three),
now measured. Why a member's proposal through the leader slows so under a write load is not
established. The followers' control logs share their device with the tables' WAL, and that device
ran 78 to 87% busy through every device arm's moves, against 54 to 62% in the steady arm.

**Moves given up** were almost all one run's: in the third failure, titan, where every group was
arriving, was called silent four seconds after the event and again eleven seconds later, and the
moves onto it then
in flight were given up and begun again
([item 244](../appendix/known-issues.md#244-a-load-that-saturates-a-1-gbe-link-delays-the-control-lanes-reports-and-members-fall-silent)).
None was lost: every group moved.

**Rebuilt** counts the positions marked missed and made current again while the arm ran. Every run
but one ended with no position or one stale; the first addition ended at 43. A run of the failure
at `baf6335`, the same engine, ran on past ten minutes, and its stale count climbed until the
device that took titan's groups filled:
[item 247](../appendix/known-issues.md#247-a-failed-devices-pool-gathered-stale-positions-after-its-moves-settled).

#### A node killed and returned inside its grace

Every kill stopped the whole foreground for seven to nine seconds: no operation completed from the
second after the kill until the eighth to tenth, and the slowest in flight at the kill took 9.7 to
10.2 s. Two in flight soon both wait on groups titan led, through their leases and elections, which
[F42](primary-failover.md) measured. While titan was away nothing moved, as its grace says. After
its return the polls found 374, 321 and 413 positions stale at the most, and each run rebuilt 533
to 583 and held none stale thirty seconds later. The puts replaced 64 objects over and over, so at
most 64 of each run's rebuilds were of an object not replaced since:
[O113](../appendix/optimizations.md#o113-recovery-rebuilds-the-stripes-of-objects-already-replaced).

#### What a failed device's moves copy, and how fast

The arms above measure the moves' count more than their bytes: by its failure titan's `/xfs` had
taken a chunk for each of at most a thousand puts, so most of its 2,732 groups held none. The
rate leg (`f88-rate-1220ff0`, two runs) is the same cluster with 32 objects of 256 MiB preloaded,
eight GiB on each host in 4 MiB chunks, read back one at a time (`get100`, one in flight) so that
nothing but the copy writes a device, and titan's `/xfs` failed 25 seconds in. Every host's
`/proc/diskstats` and `/proc/net/dev` were read each second beside it
(`shoal-spike/results/m16-sample.sh`).

Measured on titan, where every moving group arrived, from its first move's begin to its last
switch:

| Run | Event to done, s | titan's moves, s | Received by titan | Written to `/xfs2` | Received a second, median [peak] | Written a second, median [peak] |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 90 | 84 | 6,636 MiB | 10,728 MiB | 84 [117] MiB/s | 134 [206] MiB/s |
| 2 | 71 | 66 | 6,129 MiB | 10,791 MiB | 101 [117] MiB/s | 171 [235] MiB/s |

**The copy runs at the destination's 1 GbE link.** For its first 25 to 40 seconds titan received
95 to 117 MiB/s, mostly the link's rate, from europa and hyperion together, then less, and its last
seconds moved groups with nothing to copy. Over the whole window that is 79 to 94 MiB/s, against
[X12](../object-storage/recovery-scrub-rates.md)'s 70 to 90 MiB/s for a rebuild into the same 970
EVO. Neither the device nor its 256 MiB/s ceiling bounded it: the device took 1.6 to 1.8 bytes
written for each byte received, at up to 235 MiB/s. The groups moved at 5.5 to 30 a second a node,
and their slow control proposals took 0.31 to 0.47 s at the median, so the bytes, not the commits,
set the pace here. No position was stale at either done, and every get, 28 and 63 of them, came back
whole, its checksums checked as it arrived.

#### Eight in flight

One run of the failure at eight in flight, `f88-failc8d-1220ff0`, with the hosts' counters read
each second beside it. The load alone has the lab's 970 EVOs at their limit: hyperion, which lost
nothing, ran its `/xfs` 98 to 99% busy through the moves and both Zen1 hosts' roots 95 to 99%. From
the failure on, titan's `/xfs2` carried everything titan's two devices had shared, at 97 to 99%.

- **The moves** took 400 seconds for 2,731 groups: europa's in one minute, titan's and hyperion's at
  2.3 and 1.7 a second, whose slow proposals took 2.2 to 3.3 s at the median and up to 6.6 s. Seven
  were given up, and a member was called silent three times in the run. At `402c1bc`, before
  [Resolved #243](../appendix/resolved/move-slots-on-the-map.md), the same arm had switched 675
  groups and given up 48 in six minutes, and never settled.
- **The foreground** missed the objective: the get's p99 was 680 ms through the moves against 99 ms
  before them, 6.9 times, and the put's 574 against 353, 1.6 times; operations fell from 39 a
  second of each kind to 27. The bench cannot say how much of that the moves' work cost and how
  much losing a device at a load its host's other device could not carry did: the gate holds a
  rebuild back for a slice's foreground, but nothing gives back the lost device's capacity.
- **Its last device filled.** The pool's usable space fell from 106 GiB to 10.4 GiB in five minutes
  and titan's `/xfs2`, 40 GiB, stopped writing at 03:18:56. Until then 1,988 positions had been
  rebuilt, and from then none was, while the stale count rose from 1,244 to 2,153 at done. Nothing is reclaimed before
  M20 (✅ [F92](reclamation-and-device-lifecycle.md)), and a 1 MiB put writes a whole 4 MiB chunk
  ([O107](../appendix/optimizations.md#o107-a-stripe-shorter-than-its-chunk-costs-its-device-the-whole-chunk)):
  [item 247](../appendix/known-issues.md#247-a-failed-devices-pool-gathered-stale-positions-after-its-moves-settled).
  The figures at two in flight ended each failure within five minutes, before it filled.

#### What the lab found

The runs before these figures found four defects, each filed and fixed in a change of its own:
an object executor ended by a failed slice's first piece
([Resolved #242](../appendix/resolved/stream-spawned-under-borrow.md)); moves that gave themselves
up and loaded the control group with slots
([Resolved #243](../appendix/resolved/move-slots-on-the-map.md)); the control log's rewrite, which
panicked a control thread once moves took its log to its first purge
([Resolved #245](../appendix/resolved/control-log-rewrite-borrow.md)); and a shard placing by its
pool's pin a group it never read, after the group had moved
([Resolved #246](../appendix/resolved/moved-group-taken-at-base.md)). Until `1220ff0` a device
event was done only once no position was stale, which one run at `baf6335` never reached
([item 247](../appendix/known-issues.md#247-a-failed-devices-pool-gathered-stale-positions-after-its-moves-settled)).
They filed [item 244](../appendix/known-issues.md#244-a-load-that-saturates-a-1-gbe-link-delays-the-control-lanes-reports-and-members-fall-silent)
and [O112](../appendix/optimizations.md#o112-a-put-under-load-marks-its-slow-holder-missed-and-hands-recovery-a-rebuild),
and these figures item 247 and [O113](../appendix/optimizations.md#o113-recovery-rebuilds-the-stripes-of-objects-already-replaced),
and measured [O111](../appendix/optimizations.md#o111-a-placement-groups-move-costs-two-control-commits-beside-its-own-three).
A load of the TMDB dataset into the tmdb cluster, bootstrapped fresh on `b5a29b2` for the formats
F88 changed, stalled after every row was acknowledged once, and finished at once when run again:
[item 225](../appendix/known-issues.md#225-a-load-that-had-every-row-acknowledged-never-finished)'s
fourth sighting.

**The tmdb cluster** runs this build. It was bootstrapped fresh on `b5a29b2` with each host's
second device, loaded, and upgraded to `1220ff0`'s node program one node at a time, in 26 seconds,
once these figures were taken. Every movie, release row and keyword partition then read back at
`Quorum` matched the csv, none missing or different (1,187,691 movies, 1,188,009 release rows and
58,418 keyword partitions, `verify`); `stripes` put nine posters past the inline threshold through
every member, wrote them in place, and read them back whole through every member at `One` and
`Quorum`; and `status` showed 3 of 3, its pool ready on six devices with nothing stale.

### In the fixture

On europa, an operator's failure of one of a node's two devices moved its placement
groups, about two thousand, in about a minute, and in two and a half once an admission was asked
for only where the applied slots left room and no move gave itself up
(`an_operator_fails_a_device_and_its_groups_move_off_it`, [Resolved #243](../appendix/resolved/move-slots-on-the-map.md)).
A stripe row archives in 240 bytes, 48 more than F86's, which S3's published 256 still holds.

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `shoal-model` `every_placement_rule_breaks_and_its_contract_holds`, `a_placement_groups_moves_and_rebuilds_preserve_acknowledged_bytes` | A rule of the group's state, moved, stops breaking its clause, or the contract breaks one |
| `shoal-proto` `the_missed_record_follows_the_model` | The engine's record or claim leaves the model's at some step of a saved or generated schedule |
| `placement_state` `a_missed_write_is_recorded_from_where_its_position_was_current`, `a_holder_below_where_its_entry_began_is_rebuilt_whole`, `a_current_position_has_no_entry_and_a_group_at_its_base_is_forgotten`, `the_record_is_bounded_and_a_backfill_clears_its_overflow_only_over_no_drop`, `a_whole_chunk_or_too_many_writes_make_an_entry_whole` | The record is derived otherwise, unbounded, or rebuilds units it cannot know |
| `placement_state` `a_move_begins_switches_and_aborts_under_its_epoch`, `the_state_round_trips_through_serde`, `a_history_replays_to_any_boundary_above_its_base`, `a_command_round_trips_its_bytes` | A command moves a state it does not name, or the state is not the same at a boundary |
| `pool_placement` `positions_survive_a_set_change` | A staying member loses its position, or an arrival takes another's |
| `poolmap` `a_failed_device_retires_once_and_returns_only_once_its_failure_clears`, `a_target_moves_forward_and_moves_are_admitted_sixteen_a_device` | A device retires twice or comes back failed, a target goes back, or a device takes more moves than admitted |
| `chunk` `a_listing_and_a_background_request_round_trip` | A listing or the background flag does not read back |
| `wal` `an_excluded_frame_is_never_handed_to_a_compactor`, `the_placement_state_rides_the_sidecar_as_of_its_checkpoint` | A refused frame is compacted, or the sidecar's state is not the checkpoint's |
| `snapshot` `a_version_3_file_carries_its_placement_state` | A snapshot loses its placement state |
| `slice` `a_whole_chunk_is_never_older_than_the_one_it_replaces`, `a_placement_group_is_listed_in_pages` | An older whole chunk replaces a newer one, or a listing skips or repeats |
| `devices` `a_ceiling_lets_its_rate_through_and_no_more` | A device's recovery moves past its ceiling |
| `device_store` `rebuild_waits_for_the_slices_idle_time` | A background piece is issued beside a foreground one, or past the ceiling |
| `cluster_fixture` `returning_device_rebuilds_exactly_what_it_missed` | A returning holder rebuilds more or less than it missed |
| `cluster_fixture` `missed_record_survives_restart_snapshot_and_leader_change` | A copy restarted, built from a snapshot or newly leading holds another record |
| `cluster_fixture` `overflowed_record_falls_back_to_backfill` | A holder away past the bound is not brought current |
| `cluster_fixture` `rebuild_never_overwrites_a_newer_write` | A rebuild racing a write lands over it |
| `cluster_fixture` `move_serves_reads_and_writes_throughout` | A write during a move is missing from its new slices, or a read fails for the move |
| `cluster_fixture` `node_down_inside_its_grace_rebuilds_nothing` | A node inside its grace is moved off, or rebuilt around |
| `cluster_fixture` `commit_names_the_generation_it_was_staged_under` | A commit staged under one placement lands under another, through a compaction and a restart |
| `cluster_fixture` `map_change_cannot_make_a_chunk_current` | A map change alone makes a chunk current |
| `cluster_fixture` `an_operator_fails_a_device_and_its_groups_move_off_it` | An operator's failure is not carried out, or its groups do not move off |
| `events` `device_events_read_the_cluster`; `spec` `object_arms_carry_device_and_node_events` | A device's event picks the wrong device or calls moves settled early; an object arm loses its events |

## Related

- [S10](../object-storage/recovery.md), [S5](../object-storage/placement.md),
  [S7](../object-storage/write-path.md), [S13](../object-storage/isolation.md),
  [S18](../object-storage/contract.md#m16-recovery-and-moves-2026-10-14): the design this builds.
- [X1](../object-storage/stripe-model.md#a-placement-groups-state-and-its-moves): the rules it
  builds, held to the model by `the_missed_record_follows_the_model`.
  [X12](../object-storage/recovery-scrub-rates.md): the budgets and the granularity.
- [F86](replicated-pools.md): the writes and reads it makes current. [F87](tablet-walk.md): the walk
  a backfill and a move read. [F85](devices-and-pools.md): the pool map and the store.
  [F44](repair.md): the leader-driven pattern. [F46](capacity-rebalancing.md): the grace and the
  expiry a down node's devices follow.
