# F89 — M17, scrub and repair

## Context

[M17](../object-storage/milestones.md#m17-scrub-and-repair) is the object store's seventh gate. After
[F88](recovery-and-moves.md) a stale chunk was made current again, but a damaged one was only ever
refused: a unit that failed its CRC-64/NVME answered `Corrupt` to the read that met it, and nothing
counted it, set it aside or rebuilt it. A rebuild's confirmation read only a chunk's header, so a
damaged chunk at its row's label was "current" for good, and bytes nobody read were never checked
at all. R18 asks that objects be scrubbed: read on purpose, on a schedule, so damage is found while
there is still redundancy to mend it. This gate builds [S11](../object-storage/scrub.md) for
replicated pools: a read's checksum failure reported, a light scrub and a deep scrub driven by each
stripe group's leader with a committed cursor, quarantine and a rebuild on a checksum's own evidence,
everything else stopped with its evidence, and a schedule paced by each slice's idle time under the
ceiling recovery already draws on. On by default.

Every row of [S1](../object-storage/prerequisites.md#required) was done before it, and M16 with it.
What stood open was S18's Q28: what a disk's and an SSD's foreground is held to while a deep scrub
runs, and how large the parity check's sample is. Four decisions were taken with the user on
2026-10-15, before anything was built:

- **A deep scrub's objective**: a disk's foreground p99 within **1.5 times** its own while a deep
  scrub runs at the default budget, and an SSD's within **`max(1.25 × its own, its own + 1 ms)`**,
  the form M16's rebuild objective took. [X12](../object-storage/recovery-scrub-rates.md) found idle
  pacing cost a disk's foreground 1.10 to 1.34 times at the median, and nothing kept the Optane's
  86 µs foreground under 1.25 times.
- **The parity check's sample**: **a quarter of a pool's placement groups a deep pass, rotating**,
  so every group's parity is checked every fourth pass. The fold costs 0.49 to 0.71 of the
  checksum's own (X12), so a quarter of it is 12 to 18%. A replicated pool has no parity: the
  sample is recorded now (`PARITY_CHECK_EVERY`) and the check is M18's to build. ✅ Built by
  [F90](erasure-coded-pools.md#the-scrubs-and-the-parity-check).
- **A device past ten checksum failures in ten minutes is failed by its node**, as an operator's
  order is carried out, and F88's retirement moves its groups off. Ten in 600 seconds is ZFS's
  `checksum_n` and `checksum_t`, and is set per device.
- **Engine only.** The stripe model is not extended: P15 is kept at the slice by checksums, and the
  one thing M17 commits about a stripe is a position marked missed, which the model already checks
  as a rebuild that finds nothing. The seven acceptance rows hold the rest.

The other decisions were mine, and are under **Design choices** with their reasons.

## What it does

### Configuration

Nothing new is required. Wire version 11 is the scrub wire: until a cluster activates it nothing is
scrubbed, verified or asked for, and `shoaladm` bootstraps at the version its build speaks, which is
11 now. Scrubs are on by default, scheduled by each pool's policy:

```yaml
storage:
  devices:
    - {path: /xfs/shoal-devices/d0, class: ssd, checksum_failures: 10, checksum_window: 10m}
  pools:
    fast:
      class: ssd
      redundancy: {replicas: 3}
      scrub: {light: 1d, deep: 7d, window: "01:00-06:00"}   # every field optional
  object_timings:
    scrub: true            # off only for a node that should scrub nothing
    scrub_poll: 1s         # how often a shard reads its led groups' cursors
    scrub_inflight: 2      # tablet groups' passes a shard drives at once
```

`light` is how often a light pass runs, daily unless set. `deep` is the least time between the
starts of two deep passes, a week unless set: a pass slower than that is followed by the next as it
ends, so on a disk the interval is how long a pass takes (X12). A `window` of hours in UTC holds a
scheduled pass between its placement groups while it is shut; an operator's scrub runs at once
whatever the window. An interval of `0s` schedules nothing of that kind. Spans are written `90s`,
`30m`, `12h` or `7d`, and a bare number is refused.

### Damage found on a slice

A stripe chunk's units each carry a checksum bound to their place in its header, and a read verifies
every unit it returns, as F85 built it. What changed is what happens when one fails and no staged
record can write it again (`Slice::condemn`):

- the chunk's own file is verified again under its write lock, at the label it was found at; a unit
  that came from a record or a held previous state, or a chunk an apply moved on since, is not the
  chunk's fault;
- otherwise the chunk is **set aside**: renamed into the slice's `quarantine/`, under its own path
  and a count, both directories synced. From then on it is missing to every path, so no read, no
  confirmation and no rebuild is offered it, and a listing names it as quarantined;
- the failure is **counted against its device**, in its slice's report, and past the device's limit
  inside its window the device is failed as an operator's order is carried out;
- a header that fails its own checksum under the chunk's lock is set aside the same way, found by a
  listing or a verification.

A chunk set aside is **removed only once its replacement verifies**: when a whole chunk is placed at
its id, by a rebuild or a move, the slice reads every unit of the new file back and verifies it
before it deletes the evidence. A replacement that fails is set aside beside it. A record's unit
that fails as it is applied is counted the same way.

### A read's failure is reported

A coordinator that is answered `Corrupt` by a holder goes on to the next holder the row calls
current, as before, and now also **marks that position missed** in the stripe's row, behind the
read, so recovery rebuilds it from a copy that verifies. The read is counted as a corrupt find under
its pool. A stripe its put wrote whole gets a row for the mark, every other position at the put's
label, as F88's put marks a straggler.

### A rebuild never uses a corrupt source

A rebuild reads its bytes from a holder the row calls current; a source that answers `Corrupt` has
set its chunk aside, and the rebuild **marks it missed in the same commit** that makes its target
current, then tries the next. A stripe with no current copy that verifies is **lost**: reported by
name with the positions that failed, counted, and nothing written. A lost stripe is not tried again
until its row moves, or ten minutes later. Every rebuild commit is made over the row as it is now:
the row is held by the shard from the rebuild's read to its commit and read again before it, so no
mark a scrub or a read of the same shard made meanwhile is committed over.

### The scrubs

Every shard runs one scrub task over the bucket stripe groups it leads
(`shard/objects/scrub.rs`). Each group keeps two cursors, one a kind, inside its placement state
(`ScrubState`, `shoal-proto/src/shared/scrub.rs`), so they are persisted as of the checkpoint and
carried by a snapshot as the rest of that state is. A pass:

1. **begins** with a committed `ScrubBegin`, one past the cursor, when its pool's schedule says so
   or an operator asked; a group never scrubbed waits a stagger fixed by its identity after its
   leader first sees it, a share of the interval and never none, so a pool's groups do not begin
   together;
2. takes each placement group of the tablet group **in key order from the cursor**, one at a time,
   skipping none: a moving group holds the pass until its move switches or aborts;
3. for each: the group's rows are **walked** (F87), and each position's holder **lists** what it
   holds of the group, a scrub's request, a page of 256 chunk headers at a time. A light pass
   compares the two:
   - the row calls a position current and its holder lists the label it names: present;
   - the holder lists an older label, or nothing: **stale**, judged again on the row read at
     `Quorum`, and for a stripe its put wrote whole only once a later pass finds it short past the
     grace a stager is given, since a put may still be landing;
   - the holder lists the chunk as quarantined: **corrupt**;
   - a placed chunk ahead of its row, or at a label other than a put's with no row: **unexplained**,
     reported and left for M20 (✅ since [F92](reclamation-and-device-lifecycle.md#what-a-light-scrub-cannot-explain)
     a row-less chunk is asked of its owner's entry first, and removed under a seal when it names it
     nowhere);
   - a chunk of a stripe its row reclaimed: **reclaimable**, counted and left for M20 (✅ since F92
     discarded through the tombstone's sequence);
4. a **deep** pass then verifies every chunk judged present where it lies, a piece of 1 MiB at a
   time (`ChunkRequest::Verify`): the holder reads the piece's units, verifies each against its
   header and answers `Verified` with each unit's checksum unbound, or `Corrupt` once it set the
   chunk aside. The copies' checksum tables at one label are compared: **copies that each verify
   and disagree are stopped with their evidence**, logged by name with the units that differ, and
   nothing rebuilds them by a rule;
5. what is stale or corrupt is **marked missed** in its row, claiming the epoch the group was read
   at, for recovery to rebuild; the scrub never mends a chunk itself;
6. the cursor moves past the placement group with a committed `ScrubStep`, and after the last a
   `ScrubEnd` records when the pass finished. Since [Resolved #330](../appendix/resolved/scrub-refused-for-now.md)
   a group with a stripe whose query was refused for now is not stepped: the pass stops there, and
   the next poll scrubs the group again from the cursor.

A step is refused unless it follows the cursor from where its driver read it, forward, so a new
leader resumes where the last one stopped and no placement group is skipped or counted twice. None
of the three commands moves the group's epoch, so no write in flight is refused for a scrub.

### Pacing

A scrub's request is a third class beside foreground and background (`FLAG_SCRUB`): it waits until
its slice has no foreground request **and no rebuild's or move's** in flight, takes the slice's one
scrub permit, so a slice serves **one scrub piece at a time**, and draws its bytes from the device's
`recovery_ceiling`, the budget rebuilds and moves draw on. A scrub's listing draws a header block a
chunk it may list. It did from neither dispatch until
[item 299](../appendix/resolved/a-gate-keeps-a-requests-class.md): the gate handed `serve` the listing unwrapped,
and it drew nothing. Scrub requests run on the object executor's background queue, a lane's included,
and a rebuild's or a move's request counts as recovery from before its own wait, so a scrub never
starts beside one.

### Operations

- **`scrub <pool> [light|deep]`** in the cluster tab and `shoaladm admin`, admins only, deep unless
  light is asked for: committed as a count of requests on the pool map, which every leader of the
  pool's groups begins a pass for at once, and followed until no group of the pool is pending.
  Since [F93](operations-and-the-real-cluster.md#the-operators-surface) a scrub is **narrowed** by
  `scrub <pool> [light|deep] [bucket <name> | device <id> | slice <id> <index>]`: the request still
  counts on its pool, so every group of the pool serves it and its pending figures stay right, the
  newest request's target is kept with the count, and a pass scrubs and counts only the placement
  groups the target names; a target the pool does not hold is refused. An operator's scrub carries
  an operation id, a principal and an expected version, so it is refused against a stale version and
  answered `Repeated` for its id; the cluster's own, a lost journal's, carries the nil id.
- **`pools`** gains a scrub line a pool: when the oldest light and deep pass finished, how many
  groups are pending, the chunks and bytes verified, and what was found stale, corrupt, unexplained,
  disagreeing and lost, and since F93 `behind`: a chunk ahead of its stripe's row, which a pass keeps
  and never marks. The figures are counted on the shards that lead each group, summed in
  `NodeStats.recovery` and again by the leader, beside F88's.
- **`status`** shows a device's checksum failures and the chunks it holds set aside, once either is
  more than none.
- **The bench** has a new object arm event, `deep_scrub`: every pool asked for a deep scrub at
  `--event-at`, done once no group of any is pending. It takes no node down, so an attached cluster
  may run it with `--yes-events`.

### The fixture

`SCRUBS on|off` pauses every scrub driver on a node (`SCRUB` is F44's), `HOLD_SCRUB <ms>` holds a
pass after each placement group it finishes, `OBJECTS` reports each node's checksum failures,
chunks set aside, bytes scrubbed and the most scrub requests a slice served at once, and
`PLACEMENT` each group's cursors, its tablets and whether the node leads it.

## Design choices

- **Quarantine is a rename into the slice.** The evidence stays on the device it was found on, under
  its own path and a count, and no index records it: a slice reads `quarantine/` at its open. A
  quarantined chunk is missing to every path, which is what keeps it out of every read, confirmation
  and rebuild without a new check on any of them.
- **The slice verifies a replacement before it removes the evidence.** S11 has the rebuilt chunk
  verified and only then the damaged copy removed; the slice that holds both is the one place that
  can do it atomically with the placement, under the chunk's write lock, and only when a quarantine
  holds the id, so a rebuild into a healthy slice pays nothing.
- **A scrub marks and recovery mends.** One path makes a position current - F88's rebuild, from
  copies that verify under the row's labels - and a scrub, a read and a rebuild that met a corrupt
  source only mark. A mark is conservative: a position marked wrongly is confirmed current by the
  next rebuild and recommitted.
- **The cursor lives in the placement state.** It needs the checkpoint-exact persistence, the
  snapshot carriage and the leader-change behaviour F88 built for the record, and gets them by being
  a field of the same state. Its commands ride `PLACEMENT_TABLET` and move no epoch.
- **One cursor a tablet group a kind, stepped a placement group at a time.** The step is the unit of
  resumption and of proof: a step from where the cursor was read, forward, is what makes "no group
  skipped" a property of the apply rather than of the driver. A cursor a placement group would keep
  four thousand more states a pool for nothing a new leader needs.
- **A deep scrub answers each unit's checksum unbound.** Bound checksums differ by position, so the
  copies of a replicated stripe are compared on the checksum of their bytes alone, which the holder
  computes in the same pass that verifies them; only verdicts cross the network.
- **A scrub's request is a class of its own, one at a time, behind recovery.** X12 measured a deep
  scrub paced one piece at a time by idle time, and isolation's order is scrub behind rebuild behind
  apply behind foreground. The device's ceiling is the one budget both draw on, as S10 asks.
- **Cadence is pool policy; the switch and the pacing of the driver are the node's.** Every leader of
  a pool's groups schedules alike, wherever it moves; whether a node scrubs at all, and how often it
  looks, are its own business, as F88's recovery settings are.
- **A deep pass begins an interval after the last began, or as it finished.** That is X12's interval
  that is how long a pass takes, with a floor so an SSD's three-hour pass does not read its device
  end to end all day.
- **A put's stripe short of a copy waits a grace before it is marked.** A put writes whole chunks
  that no row names until its entry commits, and its pieces land at different times; the stager's
  own grace (`decide_after`) is the time after which a stage nobody decided is judged too. A
  quarantined copy is marked at once: a verified failure is never a put still landing.
- **A stripe whose copies disagree stops.** Each copy verified against its own header, so no copy is
  wrong by its own account and nothing chooses between them; S11 leaves that to an operator's
  `ResolveStripe`, ~~which is M21's~~ which [F93](operations-and-the-real-cluster.md#a-stripe-ahead-of-its-row)
  built at M21: `lost` declares a stripe lost, and `accept` moves a stripe whose holders are ahead
  of its row to the label `k + f` of them hold. Neither names one of two copies at one label to
  trust.
- **No light-scrub index.** X6 put a cold walk of a million chunks' headers at 37.5 s on the 970 EVO
  and 15.8 s on the Optane, and X7 at 217 s on a disk under XFS, which Q23 makes the only filesystem
  for a disk: minutes for a whole device once a day, against an index that is another format to keep
  consistent.

## Alternatives rejected

- **Repairing a chunk in place from a copy as the scrub finds it.** A second path that makes bytes
  current, beside the rebuild's, with its own races against writes. Marking hands it to the one
  that is already held to P8 and P17.
- **Comparing whole chunks across holders.** Moves every byte across the network to learn what two
  checksum tables say.
- **A scrub as an entry every replica applies**, F44's shape. A chunk's label already says which
  write it holds; there is nothing to freeze at an index.
- **Deleting a damaged chunk at once.** Until a replacement verifies the damaged copy is the
  evidence, and on a stripe that turns out lost it is all there is.
- **Counting each failing unit against its device.** A region of bad sectors is one event; counted
  by the unit, ten units of one chunk would fail a device on a single find.
- **Holding the chunk's lock across a whole verification.** A 4 MiB chunk read at idle pace under a
  read lock holds a foreground apply back for as long; the header is read again with each piece
  instead, so an apply between pieces is seen as the chunk moving on, never as damage.

## Limitations

- **A mark can still be lost to a commit from another shard.** A rebuild holds the row on its own
  shard and reads it again before it commits, but a read's mark from another node's coordinator can
  land between that read and the commit. The chunk is set aside and never served, so the cost is
  time: the next light pass marks it again.
- **Copies that disagree stop with their evidence and nothing else.** `ResolveStripe`, an operator's
  word on such a stripe, ~~is M21's~~ was built by [F93](operations-and-the-real-cluster.md) for a
  stripe ahead of its row; on copies that disagree at one label it can only declare the stripe
  `lost` ([todos](../appendix/todos.md#what-f89-left-for-later)).
- ~~**A put's chunk with no row is not judged against its object's entry.** A row-less chunk at a
  put's label is presumed a live put's; one of an object replaced or deleted is found only by M20's
  reclamation, which needs the same answer from an object id to its path.~~ ✅ Since
  [F92](reclamation-and-device-lifecycle.md#what-a-light-scrub-cannot-explain) a chunk names its owner,
  and a light pass asks the owner's entry at `Quorum`.
- **A placement group that cannot be scrubbed holds its pass.** One that keeps failing - its
  placement cannot be computed, its walk is refused - stops the pass there each poll, logged at debug.
- ~~**The parity check is not built.** Its sample is recorded; M18 builds the check.~~ ✅ Built by
  [F90](erasure-coded-pools.md#the-scrubs-and-the-parity-check).
- **A disk's scrub objective is stated and not measured through a cluster.** The lab's pools are on
  SSDs until M19 runs a rotational pool; X12's figures are what the 1.5 times was chosen from.
  ✅ [F91](rotational-devices.md) runs one on the lab's disks, and its lab figures take a deep scrub
  through a cluster beside a foreground.
- **A standalone node scrubs nothing.** It has no tablet groups to keep a cursor, as F88's recovery
  has no record there. A read still sets a damaged chunk aside and counts it.

## Invariants to uphold

- **A unit that fails its checksum is never returned, never a source, never merged into and never
  checksummed afresh.** A partial apply keeps every other unit's checksum from the header; a
  sub-unit write merges at the coordinator from verified reads.
- **A chunk found corrupt is set aside before anything else, and removed only after a replacement
  at its id verifies**, under the chunk's write lock.
- **A stale chunk is never counted corrupt.** Only a unit or a header failing its own checksum, in
  the chunk's own file, is damage.
- **A scrub never makes a position current.** It marks; F88's rebuild mends, from copies that
  verify under the row's labels.
- **A rebuild never uses a source that answered corrupt, and marks it in the commit that makes its
  target current**, over the row as it is now.
- **A scrub's cursor moves only by a committed step from where it was read, forward**, and no scrub
  command moves an epoch. **A step is committed only once every stripe of its placement group was
  judged or failed for good** (item 330): a refusal for now holds the cursor.
- **A scrub's request waits for its slice's foreground and recovery, holds the slice's one permit,
  and draws its bytes from the device's ceiling.**
- **Anything a checksum does not explain stops with its evidence**: copies that verify and
  disagree, a stripe with no copy that verifies.

## Performance

### In the fixture

On europa, a deep pass over a pool's 4,096 placement groups, three tablet groups of a three-node
cluster, took about seventy seconds with two objects stored, most of it a step's commit and three
listings for groups that hold nothing (`flipped_bit_is_found_by_a_read_and_by_a_deep_scrub`). A
read that met a damaged unit was answered from another copy, and the copy was set aside, marked and
rebuilt within three seconds.

### On the lab

M17's evidence was taken with `shoaladm bench` from europa (a Ryzen 9 7945HX) on 2026-10-15, at
`c700546`, from a worktree at the commit. Every node ran the one node program `target/deploy` builds
for the Zen1 hosts (znver1), and every host ran the `performance` governor for its runs.

- **The cluster.** The bench's own copy of `tmdb_cluster.yaml`, F88's: three nodes, three copies a
  host apart, each node with two pool devices on one SSD. europa's are a directory on its Optane and
  a 40 GiB XFS image on the Optane through a loop device; titan's and hyperion's are the `/xfs`
  volume on their 970 EVO and a 40 GiB XFS volume beside it. Every pool and device at its defaults:
  a ceiling of 256 MiB/s a device, and no scheduled pass in any arm, since a group's first waits at
  least a 997th of a day after its leader first sees it.
- **The arms.** `rw50` at 1 MiB, two in flight, 90 seconds measured after five of warmup, half of 64
  seeded posters preloaded, every run on a cluster of its own; `none`, and `deep_scrub`, every pool
  asked for a deep scrub a third of the way in and done once no tablet group is pending. Three runs
  each. No operation failed, and every acknowledged put was read back, none lost or misread.
- **The rate leg** is the same cluster with 32 posters of 256 MiB put, 24 GiB a pool and 8 GiB a
  host, read back one at a time (`get100`, one in flight) beside a deep scrub, twice.

Every figure is `median [lowest–highest]` across the runs. The legs are
`target/lab/f89/bench.sh`'s, as F88's were:

```sh
cd examples/tmdb_dataset && ../../target/deploy/release/tmdb-dataset-loader bench run -i <copy> \
    --dataset ../../shoal-spike/results/m16-objects --objects rw50 --object-sizes 1MiB \
    --concurrency 2 --events none,deep_scrub --duration 90 --warmup 5 --runs 3 \
    --event-timeout 900 --governor performance --basic --stop-unit shoal-tmdb
# the rate leg
... --objects get100 --object-sizes 256MiB --concurrency 1 --events deep_scrub --duration 60 --runs 2
```

#### The foreground through a deep scrub

The measured window of each arm (`f89-events-c700546`):

| Arm | get ops/s | get p50 ms | get p99 ms | put ops/s | put p50 ms | put p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `none` | 20 [20–23] | 3.00 [2.60–21.7] | 53.1 [43.3–54.6] | 21 [19–21] | 86.0 [82.4–86.7] | 132 [130–133] |
| `deep_scrub` | 18 [18–19] | 16.9 [15.9–19.1] | 55.6 [54.3–57.3] | 19 [18–19] | 89.4 [89.3–89.9] | 152 [151–154] |

The p99 of each window beside the event, in ms:

| Kind | `before` | `during` | `after` | `during` over `before` | `during` over the steady arm |
| --- | ---: | ---: | ---: | ---: | ---: |
| get | 45.6 [45.3–47.5] | 60.6 [55.7–61.5] | 55.0 [52.5–55.3] | 1.28 [1.23–1.35] | 1.14 |
| put | 127 [126–147] | 161 [157–162] | 138 [136–138] | 1.24 [1.10–1.28] | 1.22 |

**A deep pass took 40 to 43 seconds** from the request to the last group's end, every time, and
verified 3,141 to 3,222 chunks, nothing stale and nothing corrupt. **The objective holds against
the steady arm and not wholly against the window before the event**: through the pass each kind's
p99 was 1.14 and 1.22 times the steady arm's, and 1.23 to 1.35 times its own `before`, which the
bench's worst kind put at 1.27, 1.28 and 1.35; an SSD's line is `max(1.25 × its own, its own +
1 ms)`, whose millisecond plays no part at a client's tens of milliseconds over 1 GbE. As F88 found,
a `before` window is the fastest part of an arm: the steady arm runs about 24 operations a second
of each kind in its first half minute and 17 to 19 after. Through the pass both kinds fell to 14 or
15 a second, two in flight being a closed loop, and the put's p50 rose from 68 ms to 110.

**What the pass cost is mostly its steps, not its reads.** It read about a gigabyte a host, the
chunks' first mebibyte each, while it committed a step for each of the pool's 4,096 placement groups
and listed three holders of each, nearly all of them holding nothing: about a hundred commits a
second on the stripe groups' logs beside the arm's own. That is
[O114](../appendix/optimizations.md#o114-a-scrub-pass-commits-a-step-for-every-placement-group-empty-ones-included),
measured here.

#### How fast a deep scrub reads

The rate leg (`f89-rate-c700546`): a pass verified all 6,144 chunks of 4 MiB, 8 GiB on each host, in
**39.1 and 37.3 seconds** from the request, about **210 and 220 MiB/s a host**, each host's two
devices sharing one SSD. X12 read the 970 EVO at 385 MiB/s paced by idle time on one device with
nothing else running; here a 1 GbE reader of 256 MiB objects ran beside it, its p99 1.07 and 1.21
times its own before, and the pass's steps and listings shared the time. At these figures a 4 TiB
SSD is read in about five and a half hours, against X12's three.

#### The tmdb cluster

The tmdb cluster runs this build. Its placement state's format changed, so it was destroyed and
bootstrapped fresh at wire 11 and loaded; every movie, release row and keyword partition then read
back matched the csv (1,187,691 movies, 1,188,009 release rows and 58,418 keyword partitions,
`verify`), and `stripes` put nine posters through every member, wrote them in place and read them
back whole through every member.

- **A flipped bit, found by a deep scrub.** One bit of unit 5 of titan's copy of a 9 MiB poster's
  second stripe was flipped on `/xfs2` and synced, its header untouched. An operator's
  `scrub fast deep` verified the 44 other copies of the nine posters and counted that one corrupt:
  titan's slice set the chunk aside at 06:24:40.025, the scrub marked its position 21 ms later, and
  recovery rebuilt it from a copy that verifies, the evidence removed once the replacement verified,
  by 06:24:40.48. `status` then showed titan's `/xfs2` with one checksum failure and nothing set
  aside, and `pools` the pass, the find and the rebuild.
- **A lost copy, found by a light scrub.** hyperion's copy of another poster's chunk was deleted
  from its device, a holder the record had lost track of. A `scrub fast light` counted it stale and
  not corrupt, and recovery rebuilt it within seconds. On the lab a light pass over the pool's 4,096
  placement groups, eighteen tablet groups led over three nodes, took about two seconds.

`status` showed 3 of 3 afterwards, and every host was put back on its resting governor. The bench's
rate leg left an empty device directory it had made on titan, removed by hand
([item 248](../appendix/known-issues.md#248-shoaladm-bench-left-an-empty-device-directory-it-had-made)).

## Tests

| Test | What breaks if the feature is reverted |
| --- | --- |
| `device_store` `scrub_waits_for_the_slices_idle_time` | A scrub piece is served beside a foreground request or a rebuild's, or two at once on a slice |
| `device_store` `scrub_stays_inside_its_byte_budget` | A scrub reads past its device's ceiling |
| `device_store` `scrub_never_launders` | A unit's checksum is computed again over damaged bytes, a damaged chunk is left where a read finds it, or its evidence is removed before a replacement verifies |
| `device_store` `unit_checksum_binds_bytes_to_their_place` | A chunk found failing a read is kept in place, or not counted |
| `cluster_fixture` `flipped_bit_is_found_by_a_read_and_by_a_deep_scrub` | A read's failure is refused and not reported, or damage nobody reads is never found and mended |
| `cluster_fixture` `corrupt_chunk_is_never_a_source` | A rebuild copies a damaged source, or a stripe no copy verifies is invented rather than reported lost |
| `cluster_fixture` `stale_is_not_reported_as_corrupt` | A stale copy is counted as damage against its device, or never marked |
| `cluster_fixture` `scrub_resumes_from_its_cursor` | A new leader begins a pass again, skips a placement group, or finishes one short |
| `placement_state` `a_scrub_cursor_moves_only_forward_and_from_where_it_was_read`, `a_command_round_trips_its_bytes` | A step skips or repeats a placement group, a scrub moves an epoch, or a command does not read back |
| `scrub` `a_span_reads_as_it_is_written`, `a_window_holds_its_hours`, `a_policy_reads_from_a_pool`, `a_pass_is_due_by_its_schedule`, `an_operators_request_runs_at_once`, `a_stagger_spreads_groups_over_the_interval`, `figures_count_pending_groups_and_the_oldest_finish`, `a_kind_reads_and_crosses` | The schedule begins a pass early, late, outside its window or never, or the figures say a pool is scrubbed when a group is pending |
| `chunk` `a_verification_and_its_verdicts_round_trip`, `a_listing_and_a_background_request_round_trip` | A verification, its verdicts, the scrub flag or a listing's quarantined and staged chunks do not read back |
| `slice` `a_placement_group_is_listed_in_pages` | A staged chunk is listed as one in place |
| `admin` `admin_bodies_round_trip` | A scrub is not a mutation, so a principal who is not an admin may send one |
| `actions` `a_scrub_parses_and_is_followed_until_no_group_is_pending`, `pools_are_read_and_drawn` | `scrub` sends another request, is called done while a group is pending, or `pools` loses its scrub line |
| `spec` `object_arms_carry_device_and_node_events` | An object arm loses its deep scrub event, or an attached cluster is refused one |

## Related

- [S11](../object-storage/scrub.md), [S10](../object-storage/recovery.md),
  [S13](../object-storage/isolation.md), [S14](../object-storage/operations.md),
  [S18](../object-storage/contract.md#m17-scrub-and-repair-2026-10-15): the design this builds.
- [X12](../object-storage/recovery-scrub-rates.md): the pacing, the piece and the objectives' figures.
  [X6](../object-storage/device-store-ssd.md#5-listing-a-placement-group) and
  [X7](../object-storage/device-store-hdd.md): the light scrub's walk.
- [F88](recovery-and-moves.md): the rebuild that mends what a scrub marks, the ceiling it shares and
  the placement state its cursor lives in. [F87](tablet-walk.md): the walk a pass reads rows by.
  [F85](devices-and-pools.md): the store and its checksums. [F44](repair.md): how tables are
  scrubbed, and why objects cannot be the same way.
