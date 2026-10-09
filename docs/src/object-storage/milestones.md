# Milestones

~~**Provisional. Nothing below is delivered, and no gate has been set.**~~ **Nothing below is
delivered. The gate before M11 is passed**, on 2026-10-09: the contract is agreed
([the record](contract.md#before-m11-the-contract-agreed-and-q27s-path-modelled-2026-10-09)). This page was a draft
order, drawn before the [spikes](spikes.md) had reported, so that the dependencies could be
argued with. It becomes a plan when [S18's decision record](contract.md#decision-record)
holds the decisions on
[the spikes page's list](spikes.md#what-has-to-be-on-the-record-before-the-milestones-are-real),
and each gate is then set the way the Distributed chapter's were: its acceptance tests named
on their owning pages, and its evidence stated before the work starts. ✅ The record holds every
one since the contract was agreed, so the order is a plan; each gate after the first is set as its
work starts.

M11–M21 continue [Distributed Shoal's](../distributed/milestones.md) M0–M10c and are stable
identifiers. Acceptance tests live on their owning S pages, indexed by
[S16](testing.md#the-acceptance-index), and each names one gate below.

**Every gate keeps the cluster's own rules.** Metadata is rows in tablet groups, held to
P1–P6. No gate adds a second consensus protocol, an external service or a clock that decides
anything ([R7](../distributed/overview.md#what-is-asked-of-it)), and no gate is passed by
weakening a clause of [the contract](contract.md#the-contract).

## How a gate is written

| Line | Holds |
| --- | --- |
| **Closed before it** | The questions of [S18](contract.md#questions-to-answer) that are decided, with their evidence on the record, before the gate's work starts |
| **Lands first** | The required rows of [S1](prerequisites.md#required) placed at its start. Each is a change of its own, with its own page and tests, and nothing stops it landing sooner |
| **Delivers** | What exists afterwards |
| **Acceptance** | The owning pages' rows that name this gate |
| **Evidence/exit** | What is measured or demonstrated beyond tests passing. Every number is labelled by host, CPU and governor |
| *Not at this gate* | What a reader might expect here and will not find |

## The gates at a glance

| Gate | In a line | Closed before it | Lands first |
| --- | --- | --- | --- |
| ✅ Before M11 | The contract agreed, ✅ on 2026-10-09 ([the record](contract.md#before-m11-the-contract-agreed-and-q27s-path-modelled-2026-10-09)) | Q14 (safety ✅ [X1](stripe-model.md)), Q15 (✅ X1), Q16 (✅ X1), Q18 (✅ X1), Q19 (✅ [X2](placement-simulation.md), the commit ✅ X1); Q27's path in its commit held safe (✅ [X1](stripe-model.md#a-small-write-in-its-commit)) | — |
| M11 | The model, the fixture's faults, the driver's kinds | Q30, ~~in part~~ ([recorded](contract.md#decision-record), ✅ with X13) | ~~Device faults in the fixture~~ (✅ [F70](../features/storage-faults.md)); ~~operation kinds and byte counters in the driver~~ (✅ [F69](../features/driver-operation-kinds.md)) |
| M12 | Buckets in the schema and the tables they generate | Q25 (in part ✅ [X10](stripe-row-costs.md)) | ~~The conditional write~~ (✅ [F68](../features/conditional-writes.md)); ~~items 198 and 202~~ (✅ [Resolved #92, #198](../appendix/resolved/composite-partition-key.md), [Resolved #202](../appendix/resolved/append-batch-bytes.md)) |
| M13 | The wire, pool policy, inline objects, the baseline | Q21 (the checksum ✅ [X5](checksums.md)), Q26 (in part ✅ [X11](streamed-bodies.md)), the rest of Q30 (✅ [X13](benchmark-shape.md)) | ~~More than one frame for one query~~ (✅ [F73](../features/bodies-across-frames.md)) |
| M14 | Devices and their slices, the pool map, placement and the device store, on one node | Q22 (on SSDs ✅ [X6](device-store-ssd.md)), Q24 (in part ✅ [X9](table-latency.md)) | ~~Item 46~~ (✅ [Resolved #46](../appendix/resolved/unmarked-directory-refused.md)); a failure domain on a member; free bytes for every root |
| M15 | Replicated pools: stage, commit, apply and read | Q27 (the device's half ✅ [X6](device-store-ssd.md#3-a-partial-write)) | — |
| M16 | Recovery and moves | Q17 (its granularity in part ✅ [X12](recovery-scrub-rates.md)), Q29 (✅ X12) | The walk of a tablet's rows |
| M17 | Scrub and repair | Q28 (✅ X12, ✅ X14) | — |
| M18 | Erasure coding | Q20 | — |
| M19 | Rotational devices | Q23 (✅ [X7](device-store-hdd.md)) | ~~Disks, fitted to the lab~~ (✅ fitted 2026-10-06) |
| M20 | Reclamation and the life of a device | — | — |
| M21 | Operations and the real cluster | Q31 | — |

## Group 0: the contract and what it stands on

### Before M11: the object contract

~~**Not settled.**~~ **Settled 2026-10-09** ([the record](contract.md#before-m11-the-contract-agreed-and-q27s-path-modelled-2026-10-09)). It is settled when P7–P19
on [S18](contract.md#the-contract) are agreed, one
numbered property each, beside the schedule that violates it and the test that owns it, and
when five questions are decided with evidence:

- [Q14](contract.md#questions-to-answer), what orders a stripe's writes: ✅ for safety by
  [X1](stripe-model.md) ([recorded](contract.md#q16-and-q18-and-q14-q15-q19-in-part-the-stripe-protocol-modelled-2026-10-06)), for cost ✅ X3's for stripes as rows ([recorded](contract.md#q14-in-part-the-cost-of-stripes-as-rows-2026-10-07)) and ✅ X8's for the small write ([recorded](contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08));
- Q15, who stages: ✅ X1;
- Q16, the acknowledgement rule: ✅ X1;
- Q18, size and truncate across tablets: ✅ X1;
- Q19, placement: ✅ in part by [X2](placement-simulation.md)
  ([recorded](contract.md#q19-in-part-placement-2026-10-03)), and ~~what is left, how a commit
  checks a generation and the positions beside it, is X1's~~ the rest, how a commit checks a
  generation and the positions beside it, ✅ by X1.

The evidence is ✅ [X1](stripe-model.md) for safety,
✅ [X2](placement-simulation.md) for placement, and
✅ [X3](bytes-through-groups.md),
✅ [X8](small-writes.md) and
✅ [X9](table-latency.md) for cost. A clause the model contradicts
is changed with a recorded cause, and the page that leaned on it is changed with it.

No type, wire format, file format or dependency of the object store is added at this gate.
The model is the one part of it written beforehand. A prerequisite of
[S1](prerequisites.md#the-order) is another matter: each is worth having with no object
store at all, and several can land while the spikes run. A design cannot pass by calling the
appointment of a primary an edit to the pool map, and it cannot pass on a latency number
alone: a faster direction that X1 has not checked is not a candidate.

If X1 finds a violation that the safe policy cannot be repaired for, the gate does not pass,
[S7](write-path.md)'s preferred direction changes, and everything below is redrawn. ✅ It found
none: every violation it found was repaired by a local rule, recorded with the rule it replaced,
and the direction stands ([the record](stripe-model.md#recommendation)).

**One direction came after the model**: [X8](small-writes.md)'s small write that rides inside its
commit ([Q27](contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08)), faster on a
device that flushes and unchecked, so by the rule above not a candidate. ✅ The model was extended
to it before the contract was agreed, run both ways at r3 with a setting for each rule the path
depends on, and searched on the lab's three hosts. Three rules no page had stated were found and
repaired by local rules, none needing a primary, a vote among holders or undo, and P10, P11, P16
and P18 changed with it ([the small write](stripe-model.md#a-small-write-in-its-commit)). Every clause was then agreed, each beside the schedules that
violate it and the tests that own it ([the record](contract.md#before-m11-the-contract-agreed-and-q27s-path-modelled-2026-10-09)).

### M11. Step 0: the harness and the facts

**Closed before it.** Q30, how the driver gains object operations
([X13](spikes.md#x13-the-benchmarks-shape)), ~~whole~~ in part: the one driver is generalized,
recorded on 2026-10-03 with [F69](../features/driver-operation-kinds.md)
([S18](contract.md#decision-record)). Its rest, the object dataset and the rate of seeded bytes,
is needed only once buckets exist and closes before M13. ✅ It closed on 2026-10-05
([X13](benchmark-shape.md), [S18](contract.md#q30-the-object-dataset-and-seeded-bytes-2026-10-05)).

**Lands first.** ~~The torn-write, full-disk and device-loss faults in the fixture.~~ ✅ landed
as [F70](../features/storage-faults.md). ~~Operation
kinds beyond read and insert, and byte counters, in `shoal-loadgen`.~~ ✅ landed as
[F69](../features/driver-operation-kinds.md).

**Delivers.** The stripe model in `shoal-model` as a test, with every schedule of
[S7](write-path.md#the-schedules-that-shaped-it) saved and an unsafe setting for each rule
the design depends on: ✅ built ahead of the gate as [X1](stripe-model.md), with
`object_model_preserves_acknowledged_bytes` and `every_unsafe_policy_has_a_saved_schedule`, and since
2026-10-09 the small write in its commit run both ways at r3, with seven settings of its own
([the small write](stripe-model.md#a-small-write-in-its-commit)). Faults for a directory a test names, each tested against itself. A
byte ledger that judges ranges and shares no code with what it judges. A driver whose
operation kinds come from the schema, and whose windows count bytes both ways.
`acceptance_tables.rs` extended to read this part and to know M11 to M21.

**Acceptance.** S18's `object_model_preserves_acknowledged_bytes`. S16's schedule, fault,
ledger and table-structure rows. S15's three driver rows.

**Evidence/exit.** Every saved schedule replays to the violation, or for a progress schedule
the bound, it records, and a generated run of the safe policy finds none and finishes within
every bound. Every fault's self-test shows it does what its name says.
A table capture taken before the driver's change and one taken after are accepted by
`compare` as the same benchmark, and no arm id has moved. No durability claim is inferred
from a kill.

*Not at this gate:* no bucket, no object and no object arm. The driver's new kinds are
exercised by a kind a test supplies.

### M12. Tables: what the metadata needs

**Closed before it.** Q25, the metadata rows: their layout, the inline threshold, and what a
commit to a cold row costs ([X10](spikes.md#x10-what-a-stripe-row-costs)). A generated row
is persisted from the first object on, so its layout is fixed here. ✅ Recorded in part by X10 on
2026-10-04 ([Q25, in part](contract.md#q25-in-part-the-metadata-rows-2026-10-04)): stripe rows
are not kept resident, the inline threshold defaults to 16 KiB, and the index sets no floor under
the stripe above X6's. Whether buckets share one stripe table is left to this gate. And since X8
on 2026-10-08 the stripe row carries a field of pending bytes, empty unless a small write rode
its commit ([Q27](contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08)).

**Lands first.** ~~A conditional write on unsorted tables, with a typed refusal~~ ✅ landed
as [F68](../features/conditional-writes.md), for sorted tables too. ~~Known issue
198, the partition key of two fields, which is also item 92.~~ ✅ landed as
[Resolved #92, #198](../appendix/resolved/composite-partition-key.md). ~~Known issue 202, a byte
bound on an append batch.~~ ✅ landed as
[Resolved #202](../appendix/resolved/append-batch-bytes.md).

**Delivers.** `Bucket<Marker>` as a third kind of field in `#[shoal::db]`
([S2](buckets.md)): the two generated tables with their ids and tablet groups, the bucket
enum and its ids, the generated rows' layout folded into the fingerprint, the client half
with no engine in its graph, and a client's query of a generated table refused at the front
door.

**Acceptance.** S2's four rows.

**Evidence/exit.** Each prerequisite has its own page, with its reproduction, by the
repository's rules for a fix or a feature. `shoal-client-check` compiles a schema with a
bucket and no engine. A schema with one bucket has the groups the map derives for two more
tables and no others. X10's figures are taken again on the generated rows as built, and
read against the record's ([X10](stripe-row-costs.md#what-x10-does-not-settle)): 224 bytes a
stripe row and 336 an object row archived, 39 bytes of index a cold row, about 4,900 commits a
second a group on the lab, and a cold commit 1.00× a warm one at depth one and 0.62× at depth 32.

*Not at this gate:* nothing can be put in a bucket. No operation reaches one.

### M13. The wire and the baseline

**Closed before it.** Q26, streamed bodies ([X11](spikes.md#x11-streamed-bodies)). ✅ Recorded in
part by X11 on 2026-10-05 ([Q26, in part](contract.md#q26-in-part-streamed-bodies-2026-10-05)):
object bytes travel on connections a client keeps apart for long streams, in data frames of
1 MiB; the window and the memory budget as settings are M15's. Q21, the
checksum ([X5](spikes.md#x5-checksums)): a frame that carries a unit's checksum fixes it on
the wire before any slice stores one, so the dependency is chosen ~~here~~ before this gate.
✅ It is: CRC-64/NVME through `crc-fast`, with a combine of Shoal's own
([X5's record](checksums.md), [S18](contract.md#q21-in-part-the-checksum-2026-10-03)). The rest
of Q21, the granule ~~and the chunk digest~~, goes with Q20's geometry ~~and X1~~; the chunk digest
✅ is not kept ([X1](stripe-model.md#the-chunk-digest)). The rest of Q30, the
object dataset and how fast one core makes seeded bytes, since this gate's object arms need both
([X13](spikes.md#x13-the-benchmarks-shape)). ✅ Recorded by X13 on 2026-10-05
([Q30](contract.md#q30-the-object-dataset-and-seeded-bytes-2026-10-05)): a stream makes its own
bytes inline, from a description of integers whose bytes are SplitMix64 in counter mode, and
read-back makes them again.

**Lands first.** ~~More than one frame for one query on the client wire.~~ ✅ landed as
[F73](../features/bodies-across-frames.md): openers, `Data` frames with `LAST` on the final one, a
receiver that judges each frame against its stream, and connections set apart for long streams.

**Delivers.**

- The three object message types behind a capability bit, ranged frames in both directions
  (`ObjectData` is F73's `Data`, so two of the three are new),
  and the client's bucket handle and seekable file ([S12](wire-and-client.md)).
- Storage pools and bucket bindings as committed policy, with no device behind them yet: the
  policy half of [S4](pools-and-devices.md#pools-and-bindings-are-policy), and `pools` and
  `buckets` in an inventory. A bucket has a pool to be bound to and an inline threshold to
  obey.
- An object's metadata operations as conditional commits of its path's entry: create,
  replace, stat and delete, with the whole path compared ([S3](objects.md)).
- Objects at or under the inline threshold, end to end. Anything larger is refused by name,
  since no pool has a device.
- The driver's object arms and a described dataset in `shoaladm bench`, with every
  acknowledged byte read back ([S15](performance.md)): a description's integers and its
  generator, `splitmix64-ctr/1`, as [X13](benchmark-shape.md#recommendation) recorded them.
- A unit's checksum, from the client's side of the wire, in its final form:
  - CRC-64/NVME through `crc-fast` `=1.10.0`, with default features off and its `unsafe`
    read first;
  - Shoal's own combine, with one multiplier for each unit length;
  - X5's digests frozen as literals by `checksum_is_stable_across_builds`
    ([X5's record](checksums.md#recommendation)).

**Acceptance.** S12's capability, malformed-frame and retried-write rows. S3's path identity
and inline rows. S4's two policy rows. S15's `object_arm_round_trips_against_one_node` and
`every_acknowledged_byte_is_read_back`.

**Evidence/exit.** **The baseline**: candidate A of [S7](write-path.md#the-candidates)
measured through the product's own client and driver. Objects as rows, from 4 KiB to what a
row carries, on one node and on the lab's three: bytes a second, the tails, and what the
device was asked to write for each byte stored. Every object number at a later gate is read
against it. ~~And a small query's tail on a connection that also carries object frames,
inside whatever Q26 set.~~ And a small query's tail on the client's other connections beside a
stream, against its tail with none: Q26 set that object frames never share a connection with
small queries ([X11](streamed-bodies.md#3-a-small-request-beside-a-stream)).

*Not at this gate:* an object is still a row, so both peers hold one whole. The window that
bounds a stream is tested at M15, where bytes first leave as they arrive.

## Group 1: one node

### M14. Devices and pools on one node

**Closed before it.** Q22, the device store's layout and its way of applying an update
(✅ on SSDs by [X6](device-store-ssd.md), [recorded](contract.md#q22-in-part-the-device-store-on-ssd-2026-10-04):
a file a chunk from a pool written ahead, the journal and the apply in place, no clone, XFS
preferred and btrfs refused, one slice for each SSD). Q24, where object work runs
(✅ in part by [X9](table-latency.md), [recorded](contract.md#q24-and-q15-in-part-table-latency-beside-object-work-2026-10-09): executors of
their own, a four-core node giving one core up, and every object loop in steps a latency goal can
cut; which executor owns a slice, the lane and the memory budget are built here).

**Lands first.** ~~Known issue 46, a claim that tells an empty directory from a marked one and
refuses anything else.~~ ✅ landed as
[Resolved #46](../appendix/resolved/unmarked-directory-refused.md). A failure domain on a member. Free bytes reported for every storage
root.

**Delivers.**

- A device and its slices: their markers and ids, a slice's lock, the claim with three
  outcomes, the device's class as a label ([S4](pools-and-devices.md#a-device-has-slices)).
- Devices and their slices reported when a node joins and in every status report, with each
  device's size and free bytes.
- The pool map, committed and ~~pushed~~ derived on every node: devices with their seats and
  placement weights, slices and their states, generations kept as change records, exceptions
  ([S5](placement.md#the-pool-map)). The placement function X2 chose, weighted rendezvous
  for the set, with its score's table frozen, and each placement group's answer cached by
  generation. The planner's fitted weights and exceptions. Readiness that names what a pool is
  short of, and the capacity a pool as wide as its domains can use.
- The device store ([S6](device-store.md)): stripe chunks, the pool of files written ahead that
  whole chunks are written into, the journal, stage, apply, read and discard, a checksum for every chunk unit bound to its place, space taken at the stage.
- The executor that owns each slice, the budget every object buffer is drawn from, and the
  order of work on a slice ([S13](isolation.md)). Object executors are dedicated, on cores named
  in `resources` and refused if a shard holds one, with every object loop in steps of at most
  64 KiB of input on a queue with a latency goal ([X9](table-latency.md#recommendation)). The budget counts a stream's window and the
  kernel's socket buffers beside it, up to 4 MiB a side
  ([X11](streamed-bodies.md#2-the-window-and-what-a-stream-holds)).
- Devices and their slices in the fixture and in an inventory, and a bench cluster that
  moves them.

**Acceptance.** S4's device, slice, class and report rows. S5's ~~two~~ four placement rows. S6's
five rows. S13's device-loss, ownership and budget rows. S14's readiness row. S15's inventory
and capture rows.

**Evidence/exit.** X9's reference cell beside the store's own work on its dedicated executor, held
to S15's 1.25 times its p99 alone with the pool on its own device, as
[X9](table-latency.md#1-the-cell-alone-and-beside-work-on-a-core-of-its-own) found the spike's work
held at 0.97 to 1.06 times. X6's figures taken again from the store as built, against what
[X6](device-store-ssd.md) measured: a small write staged and applied (2 ms rested and 6 ms
loaded at 4 KiB on the 970 EVO, 81 µs on the Optane), a whole chunk from the pool (0.71 of a
shared file's rate at 1 MiB on the 970 EVO's XFS), and a cold listing (37.5 s for a million
chunks there). The pool map's frame at the lab's shape and at sixty-four members, against
what X2 predicted: 2,747 bytes for lab-2's six devices, and 301 bytes a device
([X2](placement-simulation.md#the-pool-maps-frame)). The reference cell's p99 beside a synthetic load on a slice's executor,
against [S15](performance.md#the-acceptance-numbers)'s budget, labelled as sharing a disk
with the tables where it does.

*Not at this gate:* no client's byte reaches a device. The store is driven by tests. The
lane that carries stripe chunks between nodes is M15's, where there is something to carry.

## Group 2: replicated pools

### M15. Replicated pools across nodes

**Closed before it.** Q27, what a small write in place costs and whether small writes ride
the metadata log ([X8](spikes.md#x8-one-small-write-three-ways)). The commit command's shape
depends on it, and a command is persisted in a log. ✅ Recorded by [X8](small-writes.md) on
2026-10-08 ([Q27](contract.md#q27-and-q14-in-part-one-small-write-three-ways-2026-10-08)): a write below 64 KiB rides inside its commit on a device
whose sync flushes its cache, once M14's slice shares one flush among its applies, so the commit
command carries a write's bytes and the stripe row a field of them; on a device whose cache writes
through every write is staged.

**Delivers.**

- The write path of [S7](write-path.md) for a replicated pool: read, stage, the conditional
  commit, the acknowledgement rule, apply; labels; a whole object in two commits; writes
  that span stripes; extending, appending and truncating.
- A losing stage discarded on a committed fact, and the leader's no-op for a stage whose
  stager died.
- The read path of [S9](read-path.md): default and strong reads, a reader moving its row
  forward, a stale chunk read around, holes, streams inside a window.
- The object lane between nodes, behind its capability and an activated wire version
  ([S13](isolation.md#a-lane-for-object-bytes)), each connection handed to the executor of the
  slice it names ([X11](streamed-bodies.md#4-a-connection-handed-over-and-bytes-that-hop)).
- The standalone node's path ([S4](pools-and-devices.md#the-standalone-node)).
- Crash points for a stripe write in the fixture ([S16](testing.md#the-fixture)).
- Every commit recording which holders missed it. Nothing acts on the record yet.
- The small write in its commit, for a replicated pool on a device whose sync flushes its cache, as
  the model has it ([S7](write-path.md#small-writes), [X1](stripe-model.md#a-small-write-in-its-commit)): the row's pending bytes, merged
  and bounded; every position confirmed; the fold; the leader's clear; and a staged write, a read, a
  rebuild and a move over pending bytes.

**Acceptance.** S7's ~~eight~~ nine rows. S9's ~~three~~ four replicated rows. S3's hole, truncate, replace
and metadata-cost rows. S12's window and seek rows. S13's lane row. S4's refusal row. S5's
pool map row. S18's two failure-model rows.

**Evidence/exit.** The first of [S15](performance.md#what-is-compared-with-what)'s
experiments: stripes on devices against the M13 baseline at each size, on the lab and over
loopback. A streaming put bounded by the slower of the devices and the network, with the
bound named. A small write in place against a table write on the same hosts. The reference
cell's p99 beside an object load. Nothing left staged when a run ends.

*Not at this gate:* a holder that missed a write stays stale; reads go round it and nothing
rebuilds it. Nothing is reclaimed but a losing stage. No erasure coded pool takes a write.

### M16. Recovery

**Closed before it.** Q17, how a slice learns what it missed and how that record survives
a checkpoint and a snapshot. Q29, the budgets for recovery and moves
([X12](spikes.md#x12-recovery-and-scrub-rates)). ✅ Q29 is recorded and Q17's granularity in part
([the record](contract.md#q28-and-q29-and-q17-in-part-recovery-and-scrub-rates-2026-10-09)): a rebuild is paced by its device's idle time under a byte ceiling, a
rotational pool survives a second loss while it rebuilds, and a missed write's record keeps unit
ranges up to the device's break-even. Q17's bound and snapshot are still open.

**Lands first.** The engine's walk of one tablet's rows, offered to a driver.

**Delivers.** [S10](recovery.md), reclamation apart. The missed record, derived at apply,
bounded, persisted beside the checkpoint and carried in a snapshot. The rebuild driver on
the group's leader, its progress committed. Backfill from the tablet walk and the holders'
inventories. A device failed on a live node rebuilt at once, and a node's devices held
through its grace. Moves between generations, up to the switch, whose commit records each
placement group's positions, and the planner over placement groups. A byte budget for each device,
as a ceiling under which a rebuild takes the slice's idle time (X12).

**Acceptance.** S10's ~~six~~ seven recovery and move rows, `rebuild_waits_for_the_slices_idle_time` added by X12. S5's ~~two~~ three generation and position
rows.

**Evidence/exit.** The event arms: a device killed, a node killed and returned inside its
grace, a device added. Each records the foreground's distribution before, during and after,
as the rebalance arms do today. A rebuild's rate at the default budget against X12's: about
22 MiB/s into a disk and 70 to 90 into the 970 EVO, paced by idle time. Zero final errors, and the
foreground's p99 under twice its own, through a rebuild and through a move. X12 found that line
held on two disks of three paced by idle time and on no SSD at any pace that rebuilt anything, so what an SSD's
foreground is held to is stated at this gate, before its arms run.

*Not at this gate:* the stripe chunks a move leaves behind are not removed until M20.

### M17. Scrub and repair

**Closed before it.** Q28, a scrub's cadence, its budget, and what a deep scrub verifies
(✅ X12, ✅ [X14](ceph-and-s3-sources.md)). X14 settled the last in part: a deep scrub checks a
stripe's chunks against each other, which Ceph's does not for an overwritable pool
([Q28, in part](contract.md#q32-and-q14-q20-q28-in-part-ceph-and-s3-at-the-source-2026-10-05)).
~~The cadence and the budget are X12's.~~ ✅ X12 recorded the cadence and the budget
([the record](contract.md#q28-and-q29-and-q17-in-part-recovery-and-scrub-rates-2026-10-09)): a deep scrub paced by its device's idle time under a ceiling, its
interval following the device's size, and the parity check on a sample of deep scrubs.

**Delivers.** [S11](scrub.md) for replicated pools. A checksum failure on a read reported
and not only refused. The light scrub: holders' inventories against the rows and against
each other. The deep scrub: every chunk unit read and verified where it lies, and the copies'
checksum tables compared. A cursor committed as it goes. Quarantine, a rebuild on a
checksum's own evidence, and everything else stopped with its evidence. A schedule, a
window, a stagger, and a byte budget shared with recovery. On by default.

**Acceptance.** S11's ~~six~~ seven rows for this gate, `scrub_waits_for_the_slices_idle_time` added by X12.

**Evidence/exit.** A background arm in the shape of today's
`macro/cluster/background/repair`: a deep scrub asked for a third of the way through, and
the foreground's p99 within S15's budget at the default. The time to scrub a device at that
budget, against X12's arithmetic: 3.3 to 4.4 days for 16 TiB on the lab's disks, three hours for
4 TiB on the 970 EVO. X12 found S15's 1.25× held on a disk at the median and not wholly, so a
disk's objective is stated here before the arm runs. A flipped bit found by a read, and found by a scrub when
nothing reads it.

*Not at this gate:* the check that parity matches data, which needs parity. A chunk nothing
explains is reported, and removed at M20.

## Group 3: erasure coding and rotational devices

### M18. Erasure coding

**Closed before it.** Q20, the code, the crate and the geometry
([X4](spikes.md#x4-erasure-coding-crates-performance-and-tradeoffs), X14). The code and the
crate are closed ✅: Reed-Solomon on ISA-L's Cauchy matrix through `rusty_erasure`, with XOR at
one parity chunk ([X4's record](erasure-coding-crates.md),
[S18](contract.md#q20-in-part-the-code-and-the-crate-2026-10-03)); the geometry ~~and X14 are~~ is
not. ✅ [X14](ceph-and-s3-sources.md) has reported and took nothing of it from Ceph.
The dependency is added here and not before, pinned exactly, after its `unsafe` (about a
hundred lines, all in `rusty_erasure-accel`) has been read.

**Delivers**, in three steps that are each a place to stop:

1. **Whole stripes.** Encode once and stage whole chunks. A healthy read that decodes
   nothing, a degraded read that decodes only what it needs, a rebuild by decode, short
   stripes with no padding, and plain XOR at one parity chunk ([S8](erasure-coding.md)).
2. **Reconstruct-write**, for a write of part of a stripe.
3. **Parity delta**, where the code has an update form.

With them, the third row of [the acknowledgement rule](write-path.md#the-acknowledgement-rule)
and the deep scrub's check of parity by summaries. The stripe's row has kept a label for
each chunk since M15, so no persisted row changes here.

**Acceptance.** S8's seven rows. S9's three erasure coded rows. S11's parity row.

**Evidence/exit.** Replicated against k+m on the same devices: what encoding costs a write
and decoding a read. A write of part of a stripe against the same write to a replicated
pool. A degraded read as a curve against `k`. A Zen1 core's encode rate inside a node
against X4's figure for the same crate. Across the lab's three hosts only 2+1 runs, and at
`f = 1` it takes no write with a host down; wider layouts run under a device failure domain
or in the fixture, and every table says which.

### M19. Rotational devices

**Closed before it.** ✅ Q23, what a rotational device needs
([X7](device-store-hdd.md), recorded on [S18](contract.md#q23-what-a-rotational-device-needs-2026-10-06)
2026-10-06).

**Lands first.** ✅ Disks, fitted to the lab on 2026-10-06, one a host
([S1](prerequisites.md#what-the-lab-needs-fitted)).

**Delivers.** What Q23 decided ~~among the three choices of
[S6](device-store.md#what-a-rotational-device-changes): where a disk's journal lives, with
the failure domain a shared journal makes; whether a disk has an executor to itself; how a
disk is read ahead. Applies deferred and batched in offset order.~~, as
[S6](device-store.md#what-a-rotational-device-changes) now has it:

- a rotational device runs with its write cache off, checked at start;
- its journal on an SSD of the node, with the failure domain a shared journal makes;
- its slice on an executor never shared with an SSD's;
- applies deferred and issued a whole batch at a time, the batch bounded by its time, with no
  offset order;
- whole chunks of 4 MiB or more, read whole;
- XFS only;
- a deep scrub paced by the arm's idle time, under its byte budget.

Budgets and a pool's defaults sized for a disk, from ~~X12~~ ✅ [X12](recovery-scrub-rates.md): a
rebuild and a deep scrub paced by the disk's idle time under a ceiling, and a default layout that
survives a second loss while a disk rebuilds.

**Acceptance.** ~~S13's `rotational_applies_are_batched_in_offset_order`~~ S13's
`rotational_applies_are_batched_and_bounded` and `rotational_slice_never_shares_an_ssd_executor`;
S6's `rotational_device_with_its_cache_on_is_refused`, `rotational_device_needs_a_journal_device`
and `rotational_stage_is_acknowledged_from_its_journal_device`; S4's
`rotational_pool_of_one_loss_is_refused`, added by X12; and every test of M14 to M18 that
names a device, run again with the fixture's devices marked rotational. ~~**This gate owns one
named test today, and that is deliberate.**~~ R16 is the same code under other costs, so most of
its acceptance is still the suites that already exist; the ~~five~~ six named tests are what Q23's
answer and X12's added.

~~*Not at this gate, yet:* the rows that depend on Q23's answer. They are added to the device
store's table, and named here, when the answer is given.~~ The rows Q23's answer added are the
five above, in S6's and S13's tables.

**Evidence/exit.** This is the gate judged mostly by measurement. X7's and X12's figures
taken again through a cluster, on the fitted disks. An SSD pool against a rotational pool on
the same nodes. The foreground's tail during applies and during a scrub at the default
budget. How long a disk of the fitted size takes to rebuild, and whether a deep scrub
finishes inside its interval.

## Group 4: lifecycle and operations

### M20. Reclamation and device lifecycle

**Delivers.** [S10's reclamation](recovery.md#reclamation): the stripe chunks and rows of a
replaced or deleted object and of an abandoned put, the stripes past a truncate and the
floors that hid them, what a move left behind, and what a light scrub cannot explain. Each
on its committed fact, with absence judged behind a read barrier, and with a grace for
readers that is not a permission. And a device's life ([S14](operations.md#admin-operations)):
drained, removed and tombstoned, reweighted, added and filled, and drained without being
asked once its checksum failures pass a threshold.

**Acceptance.** S10's two reclamation rows. S11's unexplained-chunk row. S14's drain and
tombstone rows.

**Evidence/exit.** Space comes back: a bucket filled, deleted and reclaimed leaves its
devices within a stated margin of empty. A long run of puts, replaces and deletes holds a
steady footprint. A drain under load ends with zero final errors and the foreground's p99
under twice its own.

### M21. Operations and the real cluster

**Closed before it.** [Q31](contract.md#questions-to-answer): what a schema change, a
backup, a restore and `force_recover` mean for a cluster that holds object bytes
([S14](operations.md#a-schema-change-a-backup-and-a-restore)). It is decided by design and
not by a spike, and this gate does not open without it.

**Delivers.** The whole of the operator's surface: every admin kind versioned, idempotent,
authorized, audited and advertised; the figures for a bucket, a device and a pool; pools in
the wizard, in the stats view and in the cluster tab; activation through a rolling upgrade
from a build that knows no objects. Whatever Q31 decides, and under any answer a bucket that
refuses by name when its rows are older than its stripe chunks. The six procedures of
[day two](operations.md#day-two) as runbooks.

**Acceptance.** S14's two rows for this gate.

**Evidence/exit.** Each runbook run on the lab against a pool with data in it, and its
acknowledged writes checked afterwards. A schema with a bucket deployed by `shoaladm deploy`
as a cluster of its own, benchmarked by `shoaladm bench`, upgraded in place and torn down.
The `tmdb` cluster's data is never the subject.

## Later, and unscheduled

Named so that nothing above is read as including them.

| Item | Waits on |
| --- | --- |
| An ordered listing of a bucket | [Q32](contract.md#questions-to-answer), ~~and~~ recorded by X14 (an index updated pending then complete, as RGW's is, over paths as unnormalised bytes), and a decision of its own |
| A gateway that speaks S3 | Listing, and a reason |
| Clients that place, encode and write stripe chunks themselves | [D7](../direction/shard-aware-routing.md), and a measurement that the crossing is worth removing |
| Changing a pool's redundancy; moving a bucket between pools | A migration between two pools, designed as one |
| A backup of object bytes | Whatever Q31 leaves undone |
| Authorization for a bucket | Authorization for a table, built once for both |
| Codes that rebuild from fewer chunks; a raw block device; tiering | Nothing here precludes them, and nothing here asks for them |

## What the spikes can still move

The reason this page is provisional, spike by spike.

| Spike | If it finds | Then |
| --- | --- | --- |
| ~~X1~~ | ~~A violation the safe policy cannot be repaired for~~ None: it found ten rules the pages stated broke a clause, each repaired by a local rule ([X1](stripe-model.md)) | ~~Nothing after the first gate stands~~ The gates stand. M15 builds the repaired rules: tags a try, the row's tombstones, the truncate's fence, a holder's confirmation for an untouched chunk, a previous state kept, and rows read at `Quorum` |
| ~~X2~~ | ~~The rule balances the lab's shape badly~~ It does not: 3.7% over at one placement group a tablet, 0.9% at four ([X2](placement-simulation.md)). It found instead that no function keeps positions, that a replacement needs a seat, and that mixed sizes need fitted weights | ~~M14 carries exceptions from the start, and the planner's part of M16 comes forward~~ M14's pool map carries seats, placement weights and exceptions, and M16's switch records positions |
| ~~X3, X8~~ | Rows within reach of the devices' own rate for a replicated pool. ~~X3~~ found them several times short: stripes as rows wrote a byte about twice and stored 0.20 to 0.27 of a replicated pool's device rate a copy, bound by the nodes' cpu, and slowed a table beside them a hundredfold ([X3](bytes-through-groups.md)). ~~X8 asks it again for one small write~~ X8 found the same for one small write: from 128 KiB a stage beat bytes in the log on every device, and on the Optane at every size under load ([X8](small-writes.md)) | ~~Replicated SSD pools stay rows. M15 to M17 are built for erasure coding and rotational disks first~~ Replicated SSD pools are not rows, and M15 to M17 are built in the order they stand. ~~X8 can still move small writes into the commit~~ X8 moved small writes into the commit on a device that flushes, below 64 KiB, and nowhere else |
| ~~X4~~ | ~~No candidate has an update form~~ Three have one, the chosen crate in its public API ([X4](erasure-coding-crates.md)) | ~~M18 ends at its second step~~ M18 has all three steps |
| ~~X4, X9~~ | ~~A Zen1 core encodes below a device's rate, or~~ shared executors move a table's tail past its budget. X4 measured the first half: a Zen1 core encodes 4+2 at 7.6 GiB/s out of cache ([X4](erasure-coding-crates.md)). X9 found the second at 1 MiB units: on the table shards a step of a unit moved the reference cell's read p99 2.0 to 3.8 times, with or without a latency goal, while an executor of its own cost it nothing ([X9](table-latency.md)) | M14 delivers dedicated executors only, and a four-core node gives up a core ~~or serves no pool~~: on titan the coordinating core's other thread carried 500 MiB/s. Every object loop yields in steps a latency goal can cut |
| ~~X5~~ | ~~gxhash's output is not stable across builds~~ It was stable across every cpu and build, but not across ways of feeding it ([X5](checksums.md)) | ~~A second checksum is a new dependency before M13~~ It is: CRC-64/NVME through `crc-fast`, added at M13 |
| ~~X6~~ | ~~A file a stripe chunk is not viable at small sizes, or a clone is worth requiring~~ Neither: a file a chunk is viable from 1 MiB on a device that flushes, at the line, and from 256 KiB on the Optane; the clone failed every condition but one ([X6](device-store-ssd.md)) | ~~M14's store changes layout; or a clone call lands in the glommio fork first and the filesystems M14 accepts narrow~~ M14's store keeps S6's layout, with its whole chunks written into files a slice keeps written ahead. No clone call is added, and M14 accepts XFS and ext4 and refuses btrfs |
| ~~X7~~ | ~~A disk needs a journal on an SSD, or an executor to itself~~ It needs both, and its write cache off: a stage on the disk took 42 to 251 ms beside applies, an SSD's slice on an executor shared with a disk's slowed a hundredfold, and the lab's disks with their cache on stalled a read behind a flush or acknowledged a sync before the platter could ([X7](device-store-hdd.md)) | M19 grows by that, and a shared journal ~~becomes~~ is a failure domain on S5, as S6 says. M19 also drops the offset order, reads whole chunks of 4 MiB or more, and refuses ext4 |
| ~~X8~~ | ~~A size below which bytes in the commit win~~ One on a device whose sync flushes its cache, 64 KiB once a slice shares one flush among its applies, and none on a device whose cache writes through ([X8](small-writes.md)) | M15 gains the small-write path ~~, and~~ for pools of the first kind, and M14 a shared flush for a slice's applies. ~~item 202~~ The append batch bound ([Resolved #202](../appendix/resolved/append-batch-bytes.md)) is built, and ~~item 208 carry~~ item 208 carries no more weight: a commit of less than 64 KiB is far below a frame |
| ~~X10~~ | ~~A commit to a cold stripe row stalls its group~~ It does, under load and not at depth one: 0.62× a group's rate on the 970 EVO. The read S7 already makes, sent to the group's leader, removes the stall ([X10](stripe-row-costs.md)) | ~~The rows of M12 change shape, or M15 keeps stripe rows resident and pays for it in memory~~ M12's rows keep S3's shape and stay cold; M15's commit reads its stripe row at the leader first, and the inline threshold defaults to 16 KiB |
| ~~X11~~ | ~~A shared connection hurts small queries; a connection cannot be handed to another executor~~ The first came out, ~~9 to 32~~ 4 to 29 times at 1 MiB ([item 213](../appendix/resolved/x11-setup-fifo.md) measured its reads again), and the second did not: a connection under kTLS is handed over at no cost, while bytes hopping cost 1.3 to 1.7 times the cpu ([X11](streamed-bodies.md)) | ~~M13 sets connections aside for object bytes; the hop between executors stays in M14~~ M13's client sets connections apart for object bytes (S1's prerequisite builds them for queries), and M14 hands the object lane's connections to the slice's executor |
| ~~X12~~ | A rebuild inside a tolerable budget takes days. It does, on a disk: 200 to 470 hours for 16 TiB onto one destination at the fastest pace its foreground tolerated, and 75 to 80 with none ([X12](recovery-scrub-rates.md)) | ~~The defaults for `k + m` and `f` change before M18, and M16's budget has to adapt to the foreground~~ A rotational pool's default layout survives a second loss while it rebuilds, and its rebuild is spread over its devices; M16's budget adapts to the foreground, a ceiling over the slice's idle time. M18's layouts and M19's defaults take that up |
| ~~X13~~ | ~~The driver's kinds do not generalize~~ (they do: [F69](../features/driver-operation-kinds.md)) ~~One core cannot make seeded bytes as fast as a pool takes them~~ It can, for any one lab device: a Zen1 core put 1.8 GiB/s of made and checksummed frames against the 970 EVO's 722 MiB/s ([X13](benchmark-shape.md)) | ~~M11 builds a second driver beside the first~~ ~~M13's driver runs on several cores, and its capture proves it had them~~ M13's driver makes a stream's bytes on the stream's own task; its capture keeps each driver thread's busy share, since a pool of several devices takes more than one core |
| ~~X14~~ | ~~A mechanism taken from Ceph works otherwise~~ Three did: the truncate sequence, a pool shared by Ceph's three clients, and what an acknowledgement waits for since Tentacle ([X14](ceph-and-s3-sources.md#what-would-have-changed-the-design)) | The page that leaned on it is corrected before its gate: ~~before its gate~~ S3, S4 and S17 on 2026-10-05, and no milestone changed |

## The order is a claim

- **The contract and its model come before any format.** A protocol that is wrong is
  cheapest to find while it is a few hundred lines of pure code.
- **A prerequisite lands at the start of the gate that needs it, or sooner.** Nothing
  required is skipped, and no workaround is frozen into a key, a file or a frame
  ([S1](prerequisites.md#the-rule-the-labels-follow)).
- **The wire comes before the devices**, so that every gate after it has a client to be
  driven by and a benchmark to be judged by, and so that the baseline exists before the
  thing it is a baseline for.
- **The device store comes before distribution.** Staging, applying and tearing are proved
  on one node under injected faults before a network is added to them.
- **Replication comes before erasure coding.** A replicated stripe is the same protocol with
  `k = 1` and no arithmetic.
- **Recovery comes before scrub.** A scrub's repair is a rebuild.
- **Whole stripes come before partial overwrites, and parity delta last.** It is the step
  with the most ways to be subtly wrong and the one a pool can do without.

Two places in the order are convenience and not dependency. Rotational devices follow
erasure coding only because the lab ~~has no disk yet~~ had no disk when the order was drawn; it
has one a host since 2026-10-06: M19 depends on M14 to M17 and on nothing in M18. And reclamation waits until M20 only because nothing before it needs space
back: if devices fill during the testing of M15 to M18, it moves forward of them.

## Related

[Spikes](spikes.md) for what has to report first; [S1](prerequisites.md) for the rows the
gates begin with; [S18](contract.md) for the questions and the record;
[S16](testing.md#the-acceptance-index) for where each test lives;
[Distributed Shoal's milestones](../distributed/milestones.md) for M0 to M10c and for the
form of a gate once it has been set and met.
