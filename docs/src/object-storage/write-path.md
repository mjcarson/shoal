# S7. The write path

## Context

An object can be written at any offset and truncated (decided 2026-10-02), its bytes are
replicated or erasure coded (R11), and a write that is acknowledged has to survive the losses
its pool was sized for. The hard case is the one Ceph spent years on: a write that changes
part of an erasure coded stripe and reaches some of its holders. Until it has reached enough
of them it must be possible to abandon it; once it has, it must be finished everywhere; and
at no instant may a reader or a decoder be handed pieces from both sides of it.

This page is that protocol. It carries four candidates, prefers one, and is explicit that the
preference is a hypothesis: [Q14](contract.md#questions-to-answer) is settled at the gate
before M11 by a model and two cost spikes, and not here.

## What exists today

A table write on a cluster node is one path.

- The shard that accepted the bundle builds a `Command { table, tablet, request, payload }`,
  whose payload is "the table's own intent, archived once by the node that accepted the write
  and carried as bytes from there to every replica's log and state machine"
  (`shoal-proto/src/shared/protocol/peer/replicate.rs:303-316`).
- `propose_write` puts it through the tablet's group
  (`shoal-core/src/server/shard/groups.rs:1997`), with one hop to the leader when this shard
  does not lead. Every replica applies it once, in committed order, and derives its result
  there. The client is answered after a durable majority and the local apply
  ([C5](../distributed/replication.md#a-writes-path)).
- A leader whose lease lapsed refuses to append (`Lease::of`,
  `shoal-core/src/server/replication/lease.rs:53`).
- A retried write is recognised by its request identity and answered what its first try was;
  a write whose answer never came is `OutcomeUnknown`, and only the client retries it
  ([C5](../distributed/replication.md#what-the-client-is-promised)).

What that path costs when the payload is large is measured.

- **Every byte is written at least twice**: to the shard's WAL, then into the archives by a
  merge that rewrites the partition whole. Under a whole load on the lab one host wrote
  114 MB/s to its archives and 24.5 MB/s to its WAL while applying about 15 MiB/s of rows
  ([O79](../appendix/optimizations.md#o79-a-merge-rewrites-every-partition-it-touches-whole));
  fragmented partitions have since halved the archive half for sorted tables
  ([F61](../features/fragmented-partitions.md)).
- **A write is bounded** by a 64 MiB frame and by 64 MiB of proposed and unanswered bytes a
  group (`shoal-core/src/server/conf/cluster.rs:472`), and an append batch is bounded in
  entries alone:
  [item 202](../appendix/known-issues.md#202-nothing-bounds-an-append-batch-in-bytes).
- **A row stays in memory** until the segment it was logged in is merged.

And what it cannot do: an update carries no condition
([S1](prerequisites.md#required)), nothing is atomic across two tablets, and a client does
not choose the node it writes to ([D7](../direction/shard-aware-routing.md)).

## The design

### The candidates

| | Candidate | Durable rounds | What it needs that Shoal lacks | What it gives up |
| --- | --- | --- | --- | --- |
| A | **Stripes are rows.** A stripe's bytes are a row of a generated table, replicated by its tablet group | 1 | A way to patch part of a row; a byte bound on an append batch | Erasure coding, storage pools and devices altogether. Every byte through the WAL, the archives and the compactor, and a 4 KiB change rewrites the stripe at the next merge |
| B | **The row's group orders; the bytes stay out of its log.** Holders stage, one conditional commit of the stripe's row decides, holders apply | 2 | A device store, a conditional write, a pool map | One round of latency against A |
| C | **A group among the holders**, one a placement group, with a log on every device: a Raft group with payloads out of band, or a primary and peering as RADOS has | 1 | A quorum of `k + f` with a different payload for each member; or a peering protocol | The first was not found to be offered by openraft; the second is the custom protocol [C13](../distributed/protocol.md#alternatives-rejected) declined. Either puts a consensus log on the pool's devices |
| D | **Redirect on write.** Every write makes new extents; the row holds a map of them | 2 | Collection of dead extents; a larger row | Nothing is applied in place, so nothing tears; but a stripe fragments, a rotational disk reads it badly, and the row grows with every overwrite |

**B is preferred. A is the baseline every measurement is read against**, and a candidate in
its own right for small writes ([below](#small-writes)). C stays the alternative Q14 is
judged against. D is rejected for the reasons in its row, with one part kept: a whole piece
replaced is written beside the old one and renamed, which is redirect on write at the size
where it is free ([S6](device-store.md#staging-two-cases)).

One sentence carries the difference between B and Ceph: **Ceph keeps undo and applies at
once; B keeps redo and applies after the decision.** Ceph's write is "a two-phase process:
commit and rollforward", committed "in place, possibly leaving some information required for
a rollback in a write-aside object" (`doc/dev/osd_internals/erasure_coding/ecbackend.rst` at
`v20.2.0`). Deciding which writes to roll back after a failure is what peering is for. B
moves the decision in front of the apply, makes it one commit in a group that already
exists, and pays a round for it.

Ceph's own proposal for overwrites had the same shape as B's holders: "When a prepare
operation is performed, the new data is written into a temporary object", "The apply
operation moves the data from the temporary object into the correct position within the base
object", and "an unapplied prepare operation can easily be rolled back simply by deleting
the associated temporary object" (`doc/dev/osd_internals/erasure_coding/proposals.rst`, a
proposal and marked as one). What B changes is who decides.

### The preferred direction, step by step

```mermaid
sequenceDiagram
    participant C as client
    participant N as coordinating shard
    participant H as holders, k+m devices
    participant G as the row's tablet group
    C->>N: write_at(path, offset, bytes), under an identity
    N->>G: read the object's entry (strong), the stripe's row,<br/>the placement group's generation
    Note over N: label = (sequence + 1, tag of this identity)<br/>new bytes for each piece the write touches
    N->>H: stage(piece, label, the label it expects, bytes)
    H-->>N: staged, after fdatasync
    Note over N: are k + f pieces current or staged?
    N->>G: commit, if sequence, epoch and generation are as read
    Note over G: applied in committed order,<br/>the condition judged there
    G-->>N: applied, or refused and why
    N-->>C: acknowledged
    N-)H: apply(label)
    Note over H: fold in, fdatasync, drop the staged copy.<br/>A holder that never hears asks the row
```

1. **Read.** The coordinating shard reads the object's entry with a strong read, for the
   truncate epoch ([S3](objects.md#size-holes-and-truncate)), then the row of each stripe
   the write touches and its placement group's generation ([S5](placement.md#generations)).
2. **Stage.** It computes the new bytes of every piece the write touches and sends each to
   its holder under the write's label. A holder stages durably and answers
   ([S6](device-store.md#staging-two-cases)).
3. **Commit.** With enough pieces staged ([below](#the-acknowledgement-rule)) it proposes
   one command to the row's group: move the sequence, set the labels of the touched pieces,
   record which holders did not stage, **if** the row's sequence, the object's truncate
   epoch and the placement group's generation are the ones this write read.
4. **Acknowledge.** The client is answered when that command has applied on a durable
   majority, as any table write is.
5. **Apply.** Holders are told and fold the staged bytes into their pieces. A holder that
   is not told learns from the next read, or asks the row.
6. **If refused**, the row has moved under another write. This write's staged bytes are now
   excluded by a committed fact, and holders drop them when they learn of it. The
   coordinator reads again and tries again, or fails by name.
7. **If the coordinator dies** between staging and proposing, the staged bytes are
   undecided: the row never moved, so nothing excludes them. The group's leader, prompted by
   a timer, commits a no-op that moves the sequence without changing a label. That commit is
   the fact that lets holders discard; the timer only asked
   ([P16](contract.md#the-contract)).

### What breaks without the condition

A partial write of an erasure coded stripe computes new parity from the stripe as it read
it. If another write committed in between, that parity describes a stripe that no longer
exists. With today's unconditional update the second commit simply lands: every label is
"current", every unit verifies, and the stripe decodes to garbage the first time a data
piece is missing. Nothing else in the design can catch it, which is why the conditional
write is the first row of [S1](prerequisites.md#required).

### Labels, not numbers

Two shards can stage against one row at once: two clients, or one write retried through a
new leader. Both produce "sequence plus one" with different bytes. A holder told only that
the next sequence had committed would apply whichever it had staged, and the pieces of one
stripe would then hold two different writes under one number.

So a piece is labelled by the sequence **and a tag derived from the write's request
identity**, the row keeps a label for each piece, and a holder applies a staged write only
when the row names that write's tag for its piece. The loser of a race needs no abort: the
committed row naming another tag is the fact that excludes it.

### Who stages

**Preferred: the shard the client's bytes arrived at**, with the group's leader doing
nothing but order commits ([Q15](contract.md#questions-to-answer)).

The other answer, the leader stages, serializes a stripe's writers and wastes no work. It
costs a network crossing: clients do not route by topology, so the bytes land on whichever
node the connection reached and would have to be forwarded to the leader's before they are
sent to the holders. On the lab's 1 GbE one 4 MiB stripe is about 34 ms a crossing. And it
does not remove the race it seems to: a leader change mid-write makes two stagers anyway,
so labels, idempotent staging and the cleanup of a loser are needed either way.

What the preferred answer costs is a wasted stage for every loser, and a livelock if two
writers keep meeting on one stripe. For that the leader can grant an **advisory
reservation** of a stripe: it orders who tries next and promises nothing, so losing it or
ignoring it is slow and never unsafe.

### The acknowledgement rule

There is no single number of stages that is "enough". The rule is about what is true after
the commit: **at least `k + f` pieces are current**, in distinct failure domains, where a
replicated pool has `k = 1`. `f` is the pool's, and at least one
([P11](contract.md#the-contract)).

| Write, with every piece current beforehand | Pieces it touches | Stages it needs |
| --- | --- | --- |
| Replicated, `r` copies | `r` | `1 + f` |
| Erasure coded, the whole stripe | `k + m` | `k + f` |
| Erasure coded, part of the stripe, `d` data pieces | `d + m` | Enough that at most `m - f` pieces are stale afterwards |

The third row is the one that is easy to get wrong. A 4+2 write that touches one data piece
touches three pieces. Acknowledged after one stage, it is held by one device, and losing
that device loses an acknowledged write, though four pieces of the stripe are "current"
by their old labels. Pieces already stale before the write count against the same budget.

A read, by contrast, needs no quorum: any `k` pieces the row calls current, each confirming
its label ([S9](read-path.md)). The row arbitrates, so `W + R > N` has no part here.

A pool that cannot meet the rule refuses the write by name. A weaker acknowledgement is a
setting with its own name and its own line in readiness, never what happens when holders
are short.

On the lab the rule has a consequence worth stating early: three hosts under a host failure
domain allow only 2+1, and at `f = 1` a 2+1 pool acknowledges nothing while any host is
down.

### A whole object in one commit

A put of a whole object does not use the three steps, because nothing can read its pieces
until the object exists.

1. The new object's id is recorded in the path's entry as a replacement in flight.
2. Its pieces are written straight into place, labelled with sequence zero and the object's
   own tag. No stripe gets a row.
3. When every stripe has `k + f` pieces durable, one conditional commit makes the id
   current and retires the old one.

That is two commits for an object of any size, and it is what makes
[P19](contract.md#the-contract)'s one exception true: a reader sees the old object or the
new one. A put that is abandoned leaves an id the entry names as in flight; aborting it is a
commit to that entry, after which its pieces are named nowhere and are discardable
([S10](recovery.md#reclamation)).

### Writes that span stripes

A write over several stripes is several stripe writes, sent in parallel, each atomic and the
whole not ([P19](contract.md#the-contract)). A reader in between can see some stripes new
and some old. A crash leaves some done.

What keeps that tolerable is identity: each stripe's tag is derived from the write's request
identity and the stripe's index, so the retry of a write restages the same bytes under the
same tags. A stripe whose row already carries that tag answers as it did the first time, and
the retry finishes the rest.

### Extending and appending

- **A write past the end** commits its stripes first and the size after, conditional on the
  truncate epoch ([S3](objects.md#size-holes-and-truncate)). A definite failure before the
  size moved leaves bytes beyond the end that no reader is shown.
- **An append** is a write at the size its writer read. Two appenders meet at the row of the
  tail stripe; one commit wins, the other is refused, reads the new size and tries again.

### Small writes

B's floor is two durable rounds in sequence, and a partial write of an erasure coded stripe
reads before it stages. On the lab's Zen1 hosts a sync is 3 ms alone and 5.9 ms under six
writers, so the floor there is two of those where a table write pays one; on europa's Optane
it is two of 0.2 ms. On those hosts the pool's device and the WAL are also the same disk.

One answer is candidate A at small sizes: a write under a threshold rides **inside** the
commit command, is durable when the group's log is, and is folded into the pieces
afterwards. It is one round. Its bytes cross the WAL, the row holds them until they are
folded, and a read overlays them. Whether the threshold exists, and where, is
[Q27](contract.md#questions-to-answer); [X8](spikes.md#x8-one-small-write-three-ways) finds
the size at which the two paths cross on each kind of device.

### The schedules that shaped it

Each is a schedule the model has to reject under the safe policy and reproduce under an
unsafe one ([X1](spikes.md#x1-the-stripe-protocol-as-a-model)). The first five are the
faults a review found in the first form of this design.

| Schedule | What goes wrong | What prevents it |
| --- | --- | --- |
| Two stagers on one base, by a second client or a leader change | Pieces of one stripe hold two writes under one number | Labels carry a tag; the row names one |
| A parity delta built from a stale row, committed unconditionally | Parity describes a stripe that no longer exists | The commit is conditional on the sequence |
| A device returns after an hour | It cannot learn which pieces it missed | A placement group is inside a tablet, whose group recorded it ([S10](recovery.md)) |
| A writer reads size 100 stripes; a truncate to 10 commits; the writer commits stripe 50; the object is later extended to 60 | Truncated bytes return | The commit stamps the epoch it read; the stripe is under a floor ([S3](objects.md#size-holes-and-truncate)) |
| The pool map changes during a write | Pieces committed where no reader looks | The commit names its generation ([S5](placement.md#generations)) |
| A stager times out and tells holders to drop, while its commit is in flight to the leader | An acknowledged write whose bytes are gone | Holders discard only on a committed fact |
| A 4+2 write touching one data piece is acknowledged after one stage; that device dies | An acknowledged write is lost | The acknowledgement rule |
| Parity staged as a patch; a crash after the apply; the record replayed | Parity corrupt under a current label | Staged records hold new values |
| A crash during an apply in place | A torn unit | The staged copy outlives the apply; the unit's checksum finds it |
| A stager paused for minutes resumes and proposes | Nothing: the condition refuses it | The condition |
| A stage's acknowledgement is lost; the commit proceeds without that holder | The row calls a current piece stale | A rebuild first asks the holder what it holds |
| A disk swapped for an empty one at the same path | The row calls an empty directory current | A device's identity ([S4](pools-and-devices.md#a-device-has-an-identity)) |
| The disk fills between the stage and the apply | A failure after the commit | Space is taken at the stage |
| A holder discards because a lagging replica shows no row | Bytes of a live stripe dropped | Absence on one replica is not a committed fact; only a state that cannot be undone is |

## Alternatives rejected

**A as the only path.** It has no erasure coding, no pools and no devices, and it moves
every byte through three structures built for rows. It is kept as the measured baseline, and
possibly as the path for small writes.

**C.** See the candidates' table and [S18](contract.md#alternatives-rejected).

**D.** See the candidates' table.

**Applying before the decision**, as Ceph does. It saves the round and needs both a cheap
way to keep what was overwritten and a protocol to decide what to undo.

**Holders that vote.** If a holder's durable promise bound it to a write, staging and the
commit could run together in one round. A promise that survives a restart and can be
revoked is a vote, and a protocol of votes among holders is the peering this design is
built to avoid.

**The leader stages.** See [Who stages](#who-stages).

## What it costs

- **Two durable rounds in sequence**, and a round of reads before them for a partial write
  of an erasure coded stripe.
- **Every byte crosses the network twice on its way in**: client to coordinator, coordinator
  to holders, with parity added on the second leg.
- **A stage wasted for every writer that loses a race**, and staged bytes held on a device
  until a commit decides them. A device bounds what it will hold staged and refuses beyond
  it.
- **A commit command a write**, small: a label for each touched piece and the holders that
  missed.
- **A strong read of the object's entry** before each write in place.

## What it breaks

- "A write is a table's intent in a group's log": an object write is a commit about bytes
  that are elsewhere.
- "A write's result is whether it happened": a refusal says the row moved, and a caller
  acts on that.
- "A write is durable when a majority has synced it"
  ([P3](../distributed/protocol.md#the-contract)): still true of the commit. The bytes'
  durability is the pool's, by [P11](contract.md#the-contract).

## Invariants to uphold

- A holder applies a staged write only when the row names its tag for that piece, and
  discards one only when a committed row state excludes it.
- The commit is conditional on the row's sequence, the object's truncate epoch and the
  placement group's generation, judged at apply.
- A staged record holds new values, and staging the same write twice is staging it once.
- No acknowledgement precedes `k + f` current pieces and a durable majority.
- A partial write never makes a stale piece current; only a write of the whole piece does.
- Nothing is decided by a timer. A timer may cause a commit.
- A write's tags are a function of its request identity, so its retry is the same write.

## Prerequisites

[S1](prerequisites.md#required): the conditional write with a typed refusal, and a byte
bound on an append batch if small writes ride the log. [S3](objects.md), [S5](placement.md)
and [S6](device-store.md). [S16](testing.md#the-model)'s model before any of it.

## How it would be measured

- [X1](spikes.md#x1-the-stripe-protocol-as-a-model): every schedule above, saved, against
  the contract.
- [X3](spikes.md#x3-bytes-through-the-tablet-groups): candidate A as it is today, with wide
  rows standing in for stripes. Bytes a second, bytes written to the device for each byte
  stored, memory held, and a neighbouring table's tail.
- [X8](spikes.md#x8-one-small-write-three-ways): one small write through A, through B, and
  through B with the bytes in the commit.
- The object arms of [S15](performance.md), which record bytes completed and durable, what
  was left staged, and the tail.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `stripe_write_is_atomic_at_every_crash_point` | A coordinator, a holder or the leader killed at each step leaves the stripe at its old state or its new one on every piece that is read | M15 |
| `stale_stager_cannot_commit` | A stager that lost a race, was paused across a leader change or read a stale row is refused, and its staged bytes are dropped only afterwards | M15 |
| `uncommitted_bytes_never_replace_committed` | No holder changes a piece for a write whose tag the row does not name | M15 |
| `ack_requires_k_plus_f_current` | No write to a replicated pool is acknowledged with fewer current pieces, in distinct failure domains, than the pool's setting | M15 |
| `staged_bytes_outlive_a_stagers_timeout` | A stager that gives up and tells holders to drop, while its commit is in flight, loses nothing: a holder keeps what it staged until the row excludes it | M15 |
| `retried_write_is_the_same_write` | A write retried across a leader change, with some stripes done, completes without changing a stripe twice | M15 |
| `whole_object_replace_is_atomic` | A reader during a put sees the old object or the new | M15 |
| `append_race_loses_no_bytes` | Two appenders both succeed, in some order, with neither's bytes overwritten | M15 |

## Related

[S18](contract.md) for the clauses; [S6](device-store.md) for staging and applying;
[S8](erasure-coding.md) for what a partial write reads; [S9](read-path.md) for what a
reader sees while a write is in flight; [S10](recovery.md) for a holder that missed one;
[C5](../distributed/replication.md) for the table write this is built beside;
[S17](prior-art.md#ceph) for RADOS.
