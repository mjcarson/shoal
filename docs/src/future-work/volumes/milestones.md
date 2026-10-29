# Milestones

**Nothing below is delivered, and no gate has been set.** This page is an order of work, drawn before
any [spike](spikes.md) has reported, so that its dependencies can be argued with. It becomes a plan
gate by gate: each is set once the questions it waits on are decided on
[W27](contract.md#questions-to-answer), with its acceptance tests named on their owning pages and its
evidence stated before its work starts.

M29–M34 continue the Scans plan's [M22–M28](../scans/milestones.md) and are stable identifiers.
Acceptance tests live on their owning W pages, indexed by [W27](contract.md#the-acceptance-index),
and each names one gate below. [The filesystem's](../filesystem/milestones.md) M35–M39 follow, and its
M38 waits on M32 here.

**Every gate keeps the cluster's rules.** No gate adds a consensus protocol, a coordination service or
a clock that decides anything ([R7](../../distributed/overview.md#what-is-asked-of-it)); no gate is
passed by weakening a clause of [the contract](contract.md#the-contract); and no gate a host can reach
is passed without [W23](isolation.md#the-budget)'s budget measured beside it.

## How a gate is written

| Line | Holds |
| --- | --- |
| **Closed before it** | The questions of [W27](contract.md#questions-to-answer) decided, with their evidence on the record, before the gate's work starts |
| **Lands first** | The required rows of [W14](prerequisites.md#required) placed at its start. Each is a change of its own, and nothing stops it landing sooner |
| **Delivers** | What exists afterwards |
| **Acceptance** | The owning pages' rows that name this gate |
| **Evidence/exit** | What is measured or demonstrated beyond tests passing. Every number labelled by host, cpu and governor |
| *Not at this gate* | What a reader might expect here and will not find |

## The gates at a glance

| Gate | In a line | Closed before it | Lands first |
| --- | --- | --- | --- |
| Before M29 | The contract agreed and the model passing | P27–P35; X23 | — |
| M29 | The harness: the bench's block kinds, the volume oracle, the model's schedules as tests | — | The bench's block kinds and volume events |
| M30 | The lane on one node | Q44, Q46, Q47, Q51 | Bindings and a pool's write policy, the client lane, the lane queue, grants for volume sets |
| M31 | Across nodes: marks, bars, takeovers | Q43, Q45 | A node ceiling on client-lane bytes |
| M32 | Placement under a writer: moves, rebuilds, backfill, scrub, reclamation | Q48 | — |
| M33 | The host block device | Q49 | — |
| M34 | Snapshots and clones | Q50 | — |

## The gates

### Before M29: the contract agreed

**Not settled.** It is settled when P27–P35 on [W27](contract.md#the-contract) are agreed, each beside
the violation that breaks it and the tests that own it, and when [X23](spikes.md#x23-the-lanes-model)'s
model passes its safe policy and breaks each unsafe setting's property, as the object store's contract
was agreed and X1 passed before M11
([S18's record](../../object-storage/contract.md#before-m11-the-contract-agreed-and-q27s-path-modelled-2026-10-09)).
No type, frame or file format of a volume is added at this gate.

### M29. The harness

**Closed before it.** The contract.

**Lands first.** The bench's block kinds and volume events ([W14](prerequisites.md#the-benchs-block-kinds-and-volume-events)).

**Delivers.**
- The block kinds in process, preconditioning, and the events `writer_kill`, `holder_kill`,
  `takeover` and `snapshot` ([W25](performance.md#what-the-driver-gains)).
- The fixture's volume oracle: P27 to P34 checked a stripe at a time against the history, and nothing
  across stripes.
- The model's schedules under `shoal-model/schedules/volume/` as acceptance tests.
- `shoal-bench/tests/acceptance_tables.rs` learning M29 to M39, so a test name is unique across every
  part.
- X24's, X26's and X28's baselines on the record.

**Acceptance.** [W26](model.md#acceptance-tests)'s and [W25](performance.md#acceptance-tests)'s M29 rows,
and W27's oracle test.

**Evidence/exit.** X24's tables: what a 4 KiB write costs the lab three ways, and whether the lane's
case holds on the 970 EVO.

*Not at this gate*: any change to the engine, the wire or the client.

### M30. The lane on one node

**Closed before it.** Q44, a label's granule and the merge rule; Q46, the durability default, with the
user; Q47, the geometry, by X25; Q51, the budget, by X28.

**Lands first.** Bindings for consumers other than buckets and a pool's write policy; the client lane
to object executors; the lane queue with admission; grants for volume sets ([W14](prerequisites.md#required)).

**Delivers.**
- `Volumes<M>`, its generated tables, attached pools and `storage.volumes` ([W16](volumes-and-rows.md)).
- The volume row with its writer number and tract bitmap; create, resize up, delete; attach and detach.
- The lane chunk format, the lane record, apply, and a start's classification ([W19](lane-chunks.md)).
- Writes, write-through, and reads through the client's attachment in process, on a standalone node
  whose attached pool's copies are on several of its devices ([W17](writer-lane.md)).
- The lane queue's admission and a volume's limits at the writer ([W23](isolation.md)).

**Acceptance.** The M30 rows of [W16](volumes-and-rows.md#acceptance-tests), [W17](writer-lane.md#acceptance-tests),
[W18](attach-and-takeover.md#acceptance-tests), [W19](lane-chunks.md#acceptance-tests),
[W23](isolation.md#acceptance-tests) and [W25](performance.md#acceptance-tests).

**Evidence/exit.** X24's figures retaken on what was built; the budget beside the reference cell on a
standalone node on the lab; `dm-log-writes` replayed over lane applies on XFS and ext4.

*Not at this gate*: a cluster, a takeover, a move, a host device, snapshots.

### M31. Across nodes

**Closed before it.** Q43, where a writer is barred, confirmed on what M30 built; Q45, the tracts, by
X26.

**Lands first.** A node ceiling on client-lane bytes ([W14](prerequisites.md#a-node-ceiling-on-client-lane-bytes)).

**Delivers.**
- Positions on several nodes; missed marks a group and position; the acknowledgement at `k + f` with
  the rest marked ([W17](writer-lane.md#a-position-that-does-not-answer)).
- The three bars, forced takeover, first touch, and a takeover's completion ([W18](attach-and-takeover.md)).
- Marks told to holders, refused reads, and slices that fetch their marks at a start
  ([W20](placement-under-a-writer.md#reads-while-a-position-is-stale)).
- Write-back as a named setting, with a restarted position marked from the last flush.
- A volume's limits kept at every holder; the node ceiling.
- The volumes' wire version, activated as every addition since [F48](../../features/rolling-compatibility.md)
  is, and `volumes:` grantable only behind it.

**Acceptance.** The M31 rows of W16, W17, W18, W19, [W20](placement-under-a-writer.md#acceptance-tests),
W23 and W25.

**Evidence/exit.** A takeover through a writer's kill and through a partition in the fixture, P27 to
P31 checked by the oracle; the budget on the lab's three nodes, a bucket arm beside it; a takeover's
time against X26's prediction.

*Not at this gate*: a move under a writer, a host device, snapshots.

### M32. Placement under a writer

**Closed before it.** Q48, how a move meets a writer, confirmed on what M31 built.

**Delivers.**
- Every driver dispatching on its pool's policy ([W20](placement-under-a-writer.md#the-pools-policy-chooses-the-rules)).
- Epochs installed at holders; moves under writes; rebuilds and backfill by listing.
- Light scrubs of quiet stripes, deep scrubs, and quarantine.
- Reclamation of deleted volumes a placement group at a time; allocation counts and whole-stripe
  discards.

**Acceptance.** The M32 rows of W19 and W20.

**Evidence/exit.** F88's events, a device failed, added and drained, beside block writes on the lab,
every acknowledged write read back; the volume's p99 during a move within the recovery bound
([W25](performance.md#the-budgets)).

*Not at this gate*: a host device, snapshots.

### M33. The host block device

**Closed before it.** Q49, the copy mode and the client's runtime, by X27.

**Delivers.**
- The ublk server in the schema's program, `shoaladm volume attach` and `detach` ([W22](host-device.md)).
- FLUSH and FUA, discard and write-zeroes, through the device.
- A server's recovery across its restart, as a new writer number.
- The device mode of the bench's block kinds.

**Acceptance.** The M33 rows of W17, W18, [W21](snapshots-and-clones.md#acceptance-tests),
[W22](host-device.md#acceptance-tests) and W25.

**Evidence/exit.** W25's arms through `/dev/ublkbN` on the lab within the budget, `fio` beside them as a
cross-check; X27's figures retaken on what was built.

*Not at this gate*: snapshots, clones, NBD, a gateway.

### M34. Snapshots and clones

**Closed before it.** Q50, a copy or a reflink, by X29.

**Delivers.** Snapshots by boundary, their completion and their copies; read-only attachments of
snapshots; clones and their parents' protection; growing a volume past 4,096 tracts
([W21](snapshots-and-clones.md)).

**Acceptance.** The M34 rows of W16, W21 and W22.

**Evidence/exit.** P33 and P34 under kills and takeovers in the fixture; X29's figures on what was
built; a stream of snapshots beside the reference cell within the budget.

*Not at this gate*: flattening, mirroring.

## Optional, any time

None of these is needed by a gate, and each can be added later without changing anything built:

- **Forwarding large writes** by one holder, at the threshold X24 sets ([W17](writer-lane.md#forwarding-large-writes-optional)).
- **NBD and vhost-user-blk** front ends ([W22](host-device.md#the-other-front-ends)).
- **An io_uring-native client**, should X27 ask for one.
- **XFS reflink** for a snapshot's copy, should X29 ask for one.
- **A server push to a barred writer**, so it fails its host's I/O sooner ([W18](attach-and-takeover.md#the-cut-a-takeover-makes)).
- **Flattening a clone** in the background ([W21](snapshots-and-clones.md#clones)).
- **An NVMe-oF/TCP gateway on the nodes**, a gate of its own when it is set ([W22](host-device.md#the-other-front-ends)).
- **Write-back as a deployment's default for devices with power-loss protection**, if the user later
  asks, as a named setting still.

Recorded and not scheduled: mirroring between clusters, encryption at rest, a write-back cache on a
host's own SSD. Not in these gates: erasure coded volumes ([Q52](contract.md#questions-to-answer)).

## The order is a claim

The order above argues for itself:

- **The model before any code.** Every rule the lane depends on is checked against every interleaving
  first, as X1 came before M11.
- **One node before a cluster.** The format, the record and the lane on one node, then the marks, bars
  and takeovers that only several nodes need.
- **Placement under a writer before a host can attach.** A volume that cannot survive a move, a rebuild
  and a scrub is not offered to a host, though M33 could start beside M32 once M31 is done.
- **The lane before the device**, the engine before the wire, as the Scans plan's M23 came before its
  M24.
- **Snapshots last**: everything before them is what a volume needs to be a disk at all.

## Related

[W14](prerequisites.md) for the rows the gates begin with; [Spikes](spikes.md) for what has to report
first; [W27](contract.md) for the questions and the record; [What's left to do](whats-left-todo.md) for
all of it drawn; [the filesystem's milestones](../filesystem/milestones.md), which follow.
