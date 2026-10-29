# F92 — M20, reclamation and device lifecycle

## Context

[M20](../object-storage/milestones.md#m20-reclamation-and-device-lifecycle) is the object store's
tenth gate. Until it, nothing removed a placed chunk:
- a replaced, deleted or abandoned object's chunks and rows stayed on their devices;
- the stripes past a truncate stayed too, and so did the floor that hid them, so an object truncated
  sixteen times was refused `TooManyFloors` for good;
- the old holders kept their copies after a move;
- chunks nothing could explain were reported and left.

[Item 247](../appendix/known-issues.md#247-a-failed-devices-pool-gathered-stale-positions-after-its-moves-settled)
filled a lab device in ten minutes under `rw50` for exactly this reason. No device verb existed
beyond F88's `fail-device`, and a failed device came back into service after its node restarted
([item 250](../appendix/resolved/empty-groups-move-without-a-slot.md)'s
second half).

The gate asks for [S10's reclamation](../object-storage/recovery.md#reclamation), each kind on its
committed fact, absence judged behind a barrier and readers given a grace that is not a permission,
and for [S14's device life](../object-storage/operations.md#admin-operations): drained, removed and
tombstoned, reweighted, added and filled, and drained on its own past a checksum threshold.

Four decisions were taken with the user on 2026-10-16, before anything was built:
- **X1's stripe model is extended first**, as F88 did: floors dropped when a later floor subsumes
  them or their cut stripe was rewritten past them, and a moved or aborted group's old holders sealed
  and released.
- **The reader grace is five minutes** (`object_timings.reclaim_grace`); the lab's runs use 30 s.
- **A device is drained on its own at three checksum failures in a day**, beside F89's ten in ten
  minutes that fail it.
- **Seat takeover (`replaces`) is deferred** to the todos; S4, S5 and F85 no longer call it M20's.

The other decisions were mine, and are under **Design choices** with their alternatives.

## What it does

Wire version 14 is the reclamation wire. Below it nothing is reclaimed on a cluster, no slice a move
left is released, and every device command is refused by name. `shoaladm` bootstraps at 14.

```yaml
storage:
  object_timings:
    reclaim: true          # on unless set off
    reclaim_poll: 60s      # how often each shard walks its object entries
    reclaim_grace: 5m      # how long a retired object or a left slice keeps its bytes for readers
  devices:
    - {path: /mnt/nvme1/shoal, class: ssd, checksum_drain: 3, checksum_drain_window: 1d}
```

### What a chunk names

Nothing led from a chunk back to its object's entry: the object id is a hash, and `ObjectMeta` is
keyed by the path's hash. Every chunk now names its **owner**, the key its consumer records it under,
`path_key(path)` for a bucket and zero for none recorded:
- `ChunkHeader` carries it in bytes 112 to 120 of its block, which were reserved, as `stripe_length`
  took 104 to 112;
- a put's `Piece`, a listing's `ListedChunk` and the stripe row (`StripeRow::owner`) carry it too;
- `OBJECT_ROWS_VERSION` is 3.

An entry keeps what reclamation needs:
- `ObjectState::retired` is a list of `Retired { object, extent }`, each object with how far its
  stripes may reach;
- `ObjectEntry::extent` is the most bytes the object ever reached, raised by every put, extend and
  append, never lowered by a truncate.

So the reclaimer names each stripe's key without walking the stripe table.

### A put retires what it replaces, and a given-up try retires itself

A put registers its object in flight before any chunk is written (F86), and now:
- **a put that commits retires the object it replaces**, with its extent, and any other put in flight
  at the path;
- **a delete retires the object it deletes**;
- **a try given up after a chunk may have been written retires its own id**. One given up before any
  was, because its placement could not be read, only stops naming it in flight;
- **registration refuses an id its entry names retired**, by name (`Gone`): reclamation may be
  removing what it wrote.

A try given up after writing is answered `Abandoned` (outcome 12) where its own outcome would be
retried, since a retry under the same identity would only meet its retired id. A client that minted
the identity puts the same bytes again under a new one; one whose caller named the identity hands
`Abandoned` back (`client/objects.rs`, `put_with`).

### Seals

A reclaimer frees what a slice cannot judge alone, so a slice removes nothing under reclamation but
under a seal:
1. the driver seals a scope on each holder, `Object { consumer, pg, object }` or
   `Group { consumer, pg }` (`ChunkRequest::Seal`, kind 13), and the holder acknowledges;
2. a sealed holder defers every request for what is sealed - a stage, a write, an apply, a rebuild's
   or a move's piece - until the seal ends (`Slice::until_unsealed`, one load while no seal is held);
3. the driver strong-reads the fact: the entry at `Quorum`, or the group's placement state through
   its leader's barrier;
4. only then does it send `Reclaim { scope, seal }` (kind 15), refused `NotSealed` unless its seal is
   the live one, or `Unseal` (kind 14) when the fact says the bytes are still named;
5. a reclamation ends the seal, and what waited runs and finds what is left.

A seal lapses on the slice's own clock after 30 s (`SEAL_LAPSE`). A timer only ever ends a refusal:
past it the deferred requests run, and a reclamation under the seal is refused.

`Slice::reclaim` removes, under each chunk's write lock as a quarantine does:
- the chunk's previous state and staged records, the journal's space released;
- its file, renamed into its pool's `free/<length>/` while the pool is short of its target and
  unlinked past it (`FreePool::recycle`), so no file is ever punched and the shared flush's rule
  holds;
- its count and bytes, then the empty object and group directories, synced.

A `Group` scope removes the group's whole directory on the slice. A slice remembers each removal for
a seal's lapse and refuses a write of what it removed in that window, `Removed`, so a piece on its way
when the removal ran, or one its seal held back, never brings it back
([Resolved #253](../appendix/resolved/put-left-on-moved-holder.md)).

### A retired object

Each shard runs a reclaimer (`shard/objects/reclaim.rs`) over the bucket `ObjectMeta` groups it
leads, or on a standalone node the tablets it hosts, every `reclaim_poll`. It walks each tablet's
entries with F87's walk, and for each retired object it has seen for `reclaim_grace` on its own
clock:
1. names each stripe `0..ceil(extent / stripe_bytes)`, its placement group and its holders, from the
   group's state read linearizably: every position's holder, every moving position's arrival, and
   every slice the group left and has not released;
2. seals `Object` on every serving holder, 32 groups at a time;
3. reads the entry at `Quorum`, and goes on only if the id is not live and current at its path and
   not in flight there;
4. sends `Reclaim` to every holder it sealed;
5. tombstones each stripe row a sequence past what it read, so the missed record forgets the stripe,
   then deletes it;
6. takes the id out of the entry, and a deleted entry that names nothing else goes with its row.

A holder that is down is not waited for. A holder that refuses, or whose seal lapsed, leaves the id
retired for the next pass, counted `deferred`.

### Floors

A truncate prunes every older floor whose cut is at or past its own, which hides nothing the new one
does not (`floors_after`, X1's `FloorPrune::Subsumed`). The reclaimer takes a live object's oldest
floor and runs X1's `Reclaim`:
1. every stripe wholly past the cut and stamped below the floor's epoch is tombstoned
   (`StripeRow::reclaimed`, any epoch, a sequence past, every label marked, stamped at the floor);
2. each holder is told `DiscardThrough { chunk, through }` (kind 16), the sequence the tombstone
   moved past. No seal is needed: no write staged before the tombstone can name a label at or below
   it again;
3. the floor is dropped once its cut is a stripe's edge, or the stripe it falls inside has been
   written since and stamped at or past the floor (X1's `FloorDrop::CutRewritten`).

A floor's tombstones are kept, as S3 has it, not deleted.

### What a move leaves

`MoveSwitch` names the old slices that hold no position in the new generation, and `MoveAbort` the
arrivals that may hold copies. The group's state keeps them in `PgState::left` (sidecar, snapshot and
placement answer alike). The group leader's move driver releases each one past `reclaim_grace` on its
own clock (`Work::Release`, `moves.rs`):
1. seals `Group` on the slice;
2. reads the group's state through its leader's barrier;
3. if the group no longer names the slice left, or places on it again, unseals and stops, committing
   `ReleaseDone` in the second case;
4. else `Reclaim { Group }`, then `ReleaseDone`.

A slice whose device left the map or failed is released with no request: nothing on it is ever read,
and a light scrub finds it if it serves again. A begin whose arrival the group left waits for that
release first, so a copy never lands beside what the group left there.

### What a light scrub cannot explain

A light pass (F89) asks the consumer about a stripe with no row whose chunks name an owner:
- the owner's entry is read at `Quorum`, once an object a placement group;
- **named nowhere**: every holder that listed it is sealed, the entry read again, and the chunks
  removed under the seal, counted `unexplained`;
- **retired**: the reclaimer's after its grace, neither unexplained nor marked short;
- **current or in flight**: judged as F89 did.

A reclaimed stripe's tombstone the holder kept chunks under is discarded through the tombstone's
sequence.

### A put's copy left by a stale placement

A put places by its shard's cached state of each group, and reads the state again once its pieces
are durable; a group that moved since is found moved then, and its new holders the put did not reach
are marked missed. Every holder a piece reached that the state now does not name is sealed, the
group's state read again while sealed, and the object's chunks there removed unless the group places
there again, whether the put commits or is refused: a group that left two of its slices at once
leaves a stale put holding fewer positions than it needs. Without it a put through a stale cache
left its copy on a slice whose release had run, and a drained device never emptied
([Resolved #253](../appendix/resolved/put-left-on-moved-holder.md), found by the lab's drain the day
F92 was committed).

### A group found where it does not place

A late writer can still land on a slice its group has left and released: a rebuild's or a move's
background copy, which reads the group's state, waits on its device's ceiling, and writes after a
switch, the release and the 30 s the slice refuses it for. Nothing would ever ask that slice about
the group again, so one shard of every node sweeps the node's serving slices at every reclamation
pass (`Reclaimer::sweep`):
1. each slice lists its group directories (`Slice::groups`, one directory read a consumer);
2. a group the pool's target places on the slice is left without a read;
3. one found otherwise for `reclaim_grace` has its state read at its leader, at most 256 a slice a
   pass;
4. one that is not moving, does not name the slice left, and places nothing on it is sealed
   `Group`, read again while sealed, and removed under the seal with its directory, counted
   `released`; one placed there again is unsealed and left.

The lab's drain taken at `5383d7a` emptied the second and third of its three drained devices and
left the first holding chunks past the event's 900 s, which this closes
([Resolved #253](../appendix/resolved/put-left-on-moved-holder.md)).

### Readers fail by name

A read whose chunks are all missing reads its entry again at `Quorum`. If the object is no longer
current at its path, it is answered `Gone` ("replaced or deleted while it was read, and its bytes are
reclaimed"), never other bytes.

### A device's life

The pool map's `DeviceRecord` gains a drain, a retirement, a tombstone, an operator's weight, and
whether its node reported it empty:

| Verb | Admitted when | What it commits |
| --- | --- | --- |
| `drain-device <id>` | Placed, not retired or removed; asked again it is the drain begun | `drain`, and its era ends: the generation moves, a pinned pool is fitted again, its target follows, and the move driver copies the device's groups off it while it serves |
| `remove-device <id>` | Retired; or drained, nothing arriving on it, every pool of its class targeted past its era, and its node reported every slice empty; refused by name otherwise | The tombstone, and the era ended if it had not |
| `remove-device <id> --lost` | Any device on the map | The same, on the operator's word that the device is gone |
| `reweight-device <id> <w>` | Placed, the weight above zero | `weight`, and an era at it; every later observation keeps it over the configuration's |

Each carries an operation id and the version it was written against, and is in `is_mutation()`, as
`FailDevice` and `Scrub` now are.

**Retired is permanent.** `retire_device` records why, and a failure its node stops reporting never
begins another era. That fixes item 250's second half: a restart that finds a failed disk whole no
longer brings back a device whose chunks were rebuilt elsewhere. A fault cleared on a node that was
up does not either: F86's `ack_requires_k_plus_f_current` lost a device on a live node and restarted
it to bring the device back, and now brings back its stopped node instead.

**Every door refuses a removed device**, and every observation skips one drained, retired or removed:
- `observe()` begins no era for it, and the node's `held` check counts it held;
- `set_failed`, `order_failed`, `drain_device` and `reweight_device` refuse or ignore it;
- its node fails it in memory by name once its control state names it removed ("removed by ...:
  wipe it and add it as a new device") or retired, so it serves nothing;
- a member's tombstone tombstones its devices.

**Empty is a committed fact.** The leader's device check commits, beside the failed set, which of a
node's devices whose era ended hold nothing on any slice (`ReportDevices::empty`), so the removal's
admission reads committed state on every replica.

**Auto-drain.** A device counts its checksum failures inside `checksum_drain_window` (a day) as it
counts them inside F89's window. At `checksum_drain` (three) its report says `drain_wanted`, and the
control leader proposes `DrainDevice` once, under an operation minted from the device and its node's
incarnation.

**A pinned pool is fitted again.** Since its groups move to a target (F88) and what a move leaves is
released, the leader fits a pinned pool's weights and exceptions again at every generation the map
moves to once the cluster speaks wire 14. A reweight or a new device then places by a fit, and an
exception naming a device that left ends. The fit in force before stays with the generations it held
for, which is where the pin's stripes are read.

### The operator's surface

- `shoaladm` and the cluster tab: `drain-device`, `remove-device [--lost]` and `reweight-device`,
  each previewed with what moves and its boundary.
- `devices` leads with each device's id, which every verb names, and says the bytes its chunks hold
  and a drain its node asks for.
- `pools` gains a `reclaim:` line: objects, stripes, removals, floors, released, unexplained, pending,
  deferred, and how many devices are draining.
- The bench's `device_drain` event drains one of the victim's placing devices and is done once the
  moves settled and the device holds nothing.
- `tmdb-dataset-loader reclaim fill|delete|wait|churn` puts, deletes and churns posters and prints
  every device's chunk bytes. Since [Q31](../object-storage/contract.md#q31-a-schema-change-a-backup-a-restore-and-force_recover-2026-10-17)'s
  lab run, `reclaim read` reads every poster back through the members in turn and counts what each
  was answered, and `--first <index>` sets one run's posters beside another's.
- An inventory's `object_timings` is rendered into every node's `storage.object_timings`, each key as
  `shoal.yml` writes it, so a deployment can shorten the grace (`reclaim_grace: 30s`); a key the
  engine does not read refuses the node's start by name.
- The fixture: `.reclaim(grace, poll)`, `RECLAIM on|off|now`, `HOLD_RECLAIM <ms>` between a seal and
  its read, `HOLD_READ <ms>` between a read's entry and its stripes, and `HOLD_STAGER` now holds a put
  before its commit too.

### Found on the way

Three defects in this change's own code, each found by its acceptance rows or its search and fixed
before it was committed:
- **`PgView::left` was skipped when empty**, which a placement answer's postcard read as the next
  field's bytes: every reclamation that read a group's state from another node failed with "Hit the
  end of buffer". Both `left` fields are always written now.
- **A put's retry met its own retired id.** A try given up for a placement that could not yet be read
  retired its id, and the client's retry under the same identity was refused `Gone`; `put_when_ready`
  in the fixture never put its first object. A try that wrote nothing no longer retires, and one that
  did answers `Abandoned`.
- **The model's new count of dropped floors changed every old block's digest**, since the old
  `Reclaim`'s drops were counted too; it is taken only under M20's rules
  ([X1's page](../object-storage/stripe-model.md#what-it-does-not-settle-1)).

## Design choices

**A chunk names its owner.** A slice and a scrub can ask a consumer about a chunk only if the chunk
says whose it is. A put already writes its header, so the key costs nothing at the put.

**A holder discards only under a seal.** Labels cannot fence a retired object - a put registered again
writes at the put's label - and a group's chunks on a slice a move returns to are at the labels they
had. A seal set before the strong read means a late message or a stale driver removes nothing written
after its read, with no persistent state and no clock in the safety argument.

**Registration refuses a retired id.** Otherwise a retry would bring a retired id back as in flight,
which reclamation could then be removing. The client's new identity for an `Abandoned` try keeps a
retry of an unacknowledged put working.

**The entry keeps the extent.** A retired object's stripes are named from its entry alone, never by a
walk of the stripe table, and a stripe past the extent (an extension whose size commit was lost) is the
light scrub's.

**A retired object's rows are deleted, a floor's kept.** A retired id is never current again, so its
rows have no reader; a floor's stripes belong to a live object whose later writes commit over the
tombstone.

**Release is the move driver's.** The leader that switched or aborted the move knows its slices, and a
release and a begin of one group never run together.

**Empty is committed beside the failed set**, so a removal is judged by the state machine on every
replica rather than on the reports one leader heard.

## Alternatives rejected

**A third generated table from object id to path.** A commit on every put; rejected for the owner key.

**Minting the path's key into the object id.** It breaks the 80 bits that keep ids unique and a
retry's id the same.

**Removing on the grace's timer.** The grace only puts a removal off for readers; P16 is that a
committed fact allows it.

**Re-registering a retired id under a seal.** Safe for its chunks, but a stale reclaimer's row
deletion could still meet the new object's rows; refusing is the simpler fact.

**A removal admitted by the leader's reports.** Not deterministic across replicas, and refused for the
committed `empty`.

**Punching a removed chunk's file.** The shared flush relies on chunk files whose extents are all
written; a freed file is renamed whole or unlinked.

**Leaving a pinned pool unfitted**, as F86 had it. A reweight would change nothing, and an exception
naming a failed device's slice would stand at every later generation.

## Limitations

- **Two puts to one path in flight at once: the earlier is answered `Gone`.** The later registration
  retires the earlier, whose commit then loses and whose retry finds its id retired. It is never
  acknowledged, since a read after that acknowledgement could still see the object before it; a
  client that wants the last word puts again. The lab's churn met it about once in four hundred
  operations, two workers drawing the same poster.
- ~~**A put's stale copy is removed by the put alone.** A coordinator that dies between its pieces and
  its removal leaves it, and a drained device holding one is removed only with `--lost`
  ([Resolved #253](../appendix/resolved/put-left-on-moved-holder.md#still-open)).~~ A node's sweep
  removes it, as it removes any late piece on a slice its group left.
- **A stray group's chunks wait a grace and a pass** before a sweep removes them, and a drained
  device's removal waits with them.
- **A sweep reads at most 256 groups of a slice a pass**, the still-moving groups of a drained device
  among them, so a stray group on a device draining thousands is read once they have moved.
- **Recovery and moves still read a retired object's stripes** until its rows are deleted
  ([O113](../appendix/optimizations.md)).
- **A light scrub's question is asked once a placement group a pass**, not cached across the pass.
- **A slice that lists a chunk with owner zero** (written before M20) is judged as F89 judged it.
- **`TooManyFloors` is still met** by an entry whose sixteen floors each hide a cut stripe nothing has
  rewritten since.
- **A removal is admitted once the pools' targets passed the device's era**, not once every group has
  moved: a group never written that still places at an older generation sends a put's chunk there,
  which the removed device's node refuses, so the position is missed and rebuilt. ~~Item 250's first
  half, on an erasure coded pool, is not reproduced on a replicated one by
  `removed_device_id_is_never_accepted_again` and stays open.~~ ✅ Since
  [Resolved #250, the rest](../appendix/resolved/empty-groups-move-without-a-slot.md) such a group
  moves off with no slot ahead of every other move, and since
  [Resolved #333](../appendix/resolved/removed-device-serves-nothing.md) a removed device serves
  nothing to a rebuild, a copy or a release: its slices are released without a word.
- **A device removed as lost keeps its files**: nothing serves or reclaims a removed device, and its
  directory is the operator's to wipe.
- **`status` shows a device's report, not its phase**: draining, retired and removed are in `pools` and
  `admin devices`.
- **Seat takeover (`replaces`)**, a new device drawn for exactly its predecessor's groups, is deferred
  ([todo](../appendix/todos.md#seat-takeover)).
- **`pools` judges a pool's devices at its pinned generation**, so a device removed since stays
  counted, failed: the tmdb cluster's `fast` read `devices 5/6` after its removal
  ([todo](../appendix/todos.md#what-f92-left-for-later)).
- **A reweight refits every pinned pool at the new generation**, which moves more groups than the
  reweighted device's alone.

## Invariants to uphold

- **A holder removes nothing a committed fact does not free** (P16): a retired object's or a released
  group's chunks only under a live seal set before the strong read that found them free, a floor's
  stripes only through the sequence a committed tombstone moved past.
- **A sealed request waits; it is never refused for the seal.** A write of what the seal's removal
  then removed is refused for the seal's lapse after it.
- **A seal's timer only ends a refusal.**
- **A registration never brings a retired id back.**
- **A stray group is swept only under a seal set before the read that found it stray, and never
  while it moves**; a group the pool's target places on the slice is never read, which is what keeps
  a settled pool's pass to one directory listing a slice.
- **The grace is the driver's own clock, never a permission**, and a new leader starts it again.
- **The placement answer and the sidecar are postcard**, so `PgState::left` and `PgView::left` are
  always written; a field skipped when empty is read as the next one's bytes.
- **A removed chunk's file is renamed whole or unlinked, never punched.**
- **A drained, retired or removed device never begins an era**, and its id is refused at every door.
- **Empty and removal admission read committed state alone.**
- **A dropped-floor count in the model is taken only under M20's rules**, so every saved schedule and
  every block digest of the configurations before them stays as it was.

## Performance

### In the fixture

On europa, at three test threads: `retired_object_is_reclaimed` and
`discard_requires_a_committed_fact` together in 24 s, and the drain, removal and unexplained rows in
140 s, about two thousand empty placement groups moved and released each.

### On the lab

Taken at `e4808f2` on the lab's three hosts: europa (Ryzen 9 7945HX, Zen4) and titan and hyperion
(Ryzen Embedded V1756B, Zen1), every host at the performance governor, 1 GbE. The tmdb cluster was
bootstrapped fresh at the commit from `tmdb_cluster.yaml` with `object_timings: {reclaim_grace: 30s,
reclaim_poll: 10s}` (`target/lab/m20/tmdb-m20.yml`), its `fast` pool three copies by host over two
SSD devices a host: europa's Optane and an XFS image on it, titan's and hyperion's two XFS volumes on
their 970 EVO. The node and the loader are the `znver1` build every node runs; the scripts are under
`target/lab/m20/`, which is not committed.

**Space comes back** (`space.sh`). 2,048 posters of 4 MiB were put through the three members in turn,
four in flight, in 123 s: 6,144 chunks, 25.8 GB of chunk files. All were then deleted in 4 s, and
`reclaim wait` read every device's chunk bytes every two seconds:

| | europa `/optane` | `/optane2` | titan `/xfs` | `/xfs2` | hyperion `/xfs` | `/xfs2` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Filesystem used before, bytes | 57,578,307,584 | 1,157,115,904 | 10,857,594,880 | 1,157,541,888 | 10,857,598,976 | 1,157,541,888 |
| Full | +7,567,581,184 | +1,033,080,832 | +5,808,242,688 | +2,792,820,736 | +5,581,496,320 | +3,019,567,104 |
| After the reclamation | **+32,768** | **+32,768** | **+32,768** | **+32,768** | **+32,768** | **+32,768** |

- **Nothing went inside the grace, and everything went past it.** The chunk bytes stood at 25.8 GB
  for 26 s after the deletes, fell from the twenty-eighth second, and reached zero at the forty-sixth:
  50 s from the first delete to an empty pool, at a grace of 30 s and a walk every 10 s.
- **The margin is 32 KiB a device**, the directories' blocks a fill left; the free pool's files were
  at their target before and after, since a freed file past it is unlinked.
- `pools` counted 2,048 objects reclaimed, 6,144 removals, and nothing pending or deferred.

**A steady footprint** (`reclaim churn`). Thirty minutes of puts, replaces and deletes over 512
posters of 4 MiB, two in flight, three operations in four a put, every device read every ten seconds:

| Window | Footprint, median [least–most] | Chunks, median | Operations by its end |
| --- | --- | ---: | ---: |
| 0 to 5 min | 9.95 GB [1.79–10.34] | 2,369 | 4,432 |
| 5 to 10 min | 10.25 GB [9.81–10.49] | 2,442 | 8,792 |
| 10 to 15 min | 10.15 GB [9.90–10.41] | 2,418 | 13,269 |
| 15 to 20 min | 10.17 GB [9.77–10.47] | 2,422 | 17,643 |
| 20 to 25 min | 10.05 GB [9.82–10.32] | 2,394 | 22,093 |
| 25 to 30 min | 10.14 GB [9.86–10.50] | 2,415 | 26,499 |

- **It held flat from its first minutes** while 20,045 puts wrote about 240 GB of chunks. The live
  posters, about 384 of 512 at the churn's equilibrium, are 4.8 GB in three copies; the rest is what
  was retired inside the grace and the walk, about 40 s of puts at 11 a second.
- **41 operations failed, every one a put answered `Gone`**: two workers drew the same poster, and
  the earlier put was retired by the later's registration (**Limitations**).
- `pools` counted 21,332 objects reclaimed over both runs, 63,996 removals, 345 retired objects
  pending inside their grace at the end, and nothing deferred.

**A drain under load** (`drain-bench.sh`, capture `m20-drain-a561d0d`). `shoaladm bench run` on the
bench's own copy of the cluster, `rw50` at 1 MiB, two in flight, 90 s measured after 5 s of warmup,
three runs of no event against three of `device_drain`, which drains the victim's first placing SSD
(titan's `/xfs`) 35 s in and is done once the moves settle and every slice of it reports nothing
held. Every node ran `a561d0d`'s program (sha256 `27856547…`, all three hosts):

| Run | Done after the request | get p99 before → during | put p99 before → during | Errors | Acknowledged writes lost |
| --- | ---: | --- | --- | ---: | ---: |
| 0 | 327 s | 42.27 → 71.17 ms (1.68×) | 153.47 → 218.75 ms (1.43×) | 0 | 0 of 64 |
| 1 | 295 s | 44.19 → 65.60 ms (1.48×) | 149.76 → 230.01 ms (1.54×) | 0 | 0 of 64 |
| 2 | 367 s | 42.37 → 59.39 ms (1.40×) | 153.22 → 233.34 ms (1.52×) | 0 | 0 of 64 |

- **Every kind's p99 during the drain stayed under twice its own before it**, the line M20 is judged
  by, the worst 1.68× for gets; the runs without an event put the arm's p99s at 53–69 ms for gets and
  182–190 ms for puts over their whole 90 s.
- **Every drain emptied its device**, five to six minutes after its request: about 2,740 placement
  groups moved and released a drain. A probe of run 0 saw its moves settle 5 min 20 s after the
  request and the device hold nothing seconds later. Three earlier takings did not ([Resolved #253](../appendix/resolved/put-left-on-moved-holder.md)):
  `e4808f2`'s left 306 chunks of puts through stale placements, `76078e1`'s six late pieces, and
  `5383d7a`'s first drain never emptied, which a probe of a fourth taking traced to a put refused
  under its group's moved state. That fourth capture is labelled `db1ae04` and ran `5383d7a`'s
  program (item 254), so it is not quoted.
- `ack 64 lost 0` is the bench's ledger: every write it was acknowledged for read back afterwards.

**The device verbs** (`device-life.sh`, on the tmdb cluster at `a561d0d`, 256 posters of 4 MiB in
`fast`, idle otherwise):
- `remove-device` of titan's second SSD before a drain is refused by name, naming `drain-device`;
- `drain-device` is applied, and `remove-device` at once is refused: "device … still holds chunks, as
  node … last reported: its placement groups have not all moved off it and released it";
- its 1,375 groups switched in 47 s, the device held nothing at 93 s, and the removal was applied
  then;
- titan started again with the device's directory intact serves nothing of it, reporting it
  "failed: device … was removed by admin (op …)";
- `reweight-device` of hyperion's second SSD to 160 GB moved the pool's target from 9 to 11 and
  switched 1,279 more groups, settled within 40 s.

## Tests

| Test | What breaks if it is reverted |
| --- | --- |
| `cluster_fixture` `retired_object_is_reclaimed` | A replaced or deleted object's chunks or rows stay past the grace, the replacement loses a chunk, or a read held across the reclamation is answered other bytes or anything but `Gone` |
| `cluster_fixture` `discard_requires_a_committed_fact` | A chunk goes inside the grace, a current object's or a put in flight's goes after it, a retired id is registered again, or a reclaimed id cannot be put again |
| `cluster_fixture` `unexplained_chunk_is_found_and_reclaimed` | A reclaimed object's chunk a down holder kept stays after a light scrub, goes without its consumer's answer, or a live object's chunk goes |
| `cluster_fixture` `drained_device_leaves_only_when_its_chunks_are_elsewhere` | A drain loses a write, leaves chunks on the device, is not released, or a placed or unreleased device is removed |
| `cluster_fixture` `removed_device_id_is_never_accepted_again` | A removed device is observed again, served, placed on, or accepted by a verb |
| `cluster_fixture` `a_put_through_a_stale_placement_leaves_nothing_on_a_drained_device` | A put through a stale cache leaves its chunk on a drained device, which never empties (Resolved #253) |
| `cluster_fixture` `a_device_past_its_checksum_drain_threshold_is_drained` | Three checksum failures do not drain a device, or drain it by failing it |
| `objects` `a_replaced_and_a_deleted_object_are_reclaimed_on_one_node` | A standalone node keeps a retired object's chunks |
| `objects` `a_floors_stripes_are_reclaimed_and_the_floor_goes` | A floor's stripes stay, or the floor stays once its cut stripe is rewritten |
| `device_store` `a_seal_defers_what_it_covers_and_a_reclamation_needs_it_live` | A sealed request runs, a reclamation under another or a lapsed seal removes anything, or a freed file grows the pool past its target |
| `device_store` `a_late_write_of_what_was_removed_is_refused` | A late piece of what a reclamation removed lands on the slice (Resolved #253) |
| `device_store` `a_released_group_leaves_nothing_of_itself_on_its_slice` | A group's release leaves a chunk or its directory, or takes another group's |
| `cluster_fixture` `a_put_refused_by_a_moved_group_leaves_nothing_where_it_moved_from` | A put refused because its group left two of its slices at once leaves its copies there (Resolved #253) |
| `cluster_fixture` `a_chunk_left_where_its_group_does_not_place_is_swept` | A late piece on a slice its group left and released stays for good, and a drained device holding one is never removed (Resolved #253) |
| `device_store` `a_slice_lists_its_groups_and_a_released_one_is_no_longer_listed` | A sweep cannot list a slice's groups, or lists a released group's emptied directory at every pass |
| `stripe_model_conformance` `floors_and_released_slices_follow_the_model` | The product's floor pruning or slices left differ from X1's |
| `stripe_model` `reclamation_and_releases_preserve_acknowledged_bytes`, `every_reclamation_rule_breaks_and_its_contract_holds` | The model's M20 rules break the contract, or an unsafe one does not |
| `poolmap` `a_failed_device_retires_once_and_stays_retired` | A failed device comes back once its failure clears (item 250) |
| `poolmap` `a_drained_device_is_removed_and_its_id_is_never_accepted_again`, `a_reweight_begins_an_era_the_configuration_never_undoes`, `a_tombstoned_member_tombstones_its_devices` | A drain, a removal, a reweight or a member's tombstone records otherwise |
| `control::types` `a_device_is_drained_removed_and_reweighted_only_as_its_facts_allow` | A device verb is taken below wire 14, a removal is admitted early, or a removed device is observed back |
| `devices` `a_device_asks_for_its_drain_before_its_failures_fail_it` | The drain threshold does not ask, asks early, or fails the device |
| `admin` `admin_bodies_round_trip` | A device verb is not a mutation, or does not round trip |
| `actions` `an_action_previews_its_boundary_and_follows_its_record` | A device verb does not parse, or `--lost` is taken from anything else |
| `model` `devices_are_drawn_a_line_each` | `devices` stops leading with a device's id |
| `spec` `object_arms_carry_device_and_node_events` | `device_drain` is not an object arm's event, or does not parse |
| `render` `a_node_file_carries_the_inventorys_object_timings` | An inventory's object timings do not reach a node's file, or one is written when none is named |
| The six fixture rows above, and `device_store` and `objects` whole, under `SHOAL_TEST_ROTATIONAL=1` | Reclamation, a seal or a device's life fails on a disk: every device rotational, its journal beside it |

## Related

[M20](../object-storage/milestones.md#m20-reclamation-and-device-lifecycle) for the gate;
[S10](../object-storage/recovery.md#reclamation), [S11](../object-storage/scrub.md),
[S14](../object-storage/operations.md#admin-operations) and [S3](../object-storage/objects.md#size-holes-and-truncate)
for the design; [X1](../object-storage/stripe-model.md#reclamation) for the model's rules;
[S18's record](../object-storage/contract.md#m20-reclamation-and-device-lifecycle-2026-10-16) for the
decisions; [F88](recovery-and-moves.md) for the moves whose slices are released and the retirement made
permanent; [F89](scrub-and-repair.md) for the scrub whose unexplained chunks are now asked about;
[F86](replicated-pools.md) for the put whose retirements are reclaimed.
