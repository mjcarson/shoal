# F100. Strong reads inside a leader's lease, as a setting

Since this feature a node can serve a strong read on a group it leads inside the leader's lease,
without the heartbeat round every strong read sends otherwise. It is a setting,
`cluster.replication.lease_reads`, off by default. The round stays every strong read's way unless a
deployment turns the setting on.

## Context

A `Quorum` read confirms its group's leader before it is served ([F41](read-consistency.md)).
openraft's `ReadPolicy::ReadIndex` does that with a round: an empty append to every other voter,
answered by a quorum. [F99](strong-read-rounds.md) counted the rounds' parts on the lab
([A round by its parts](../object-storage/store-optimizations/small-read-tail.md#a-round-by-its-parts)):
- every strong read sends a round of its own;
- at sixteen in flight, the rounds' work on busy shards of the Zen1 hosts is the small reads' tail;
- sharing a round between a group's reads
  ([O125](../appendix/optimizations.md#o125-every-strong-read-sends-a-round-of-its-own-however-many-of-its-groups-are-in-flight))
  would take few away, since the reads spread over a table's 90 groups.

openraft 0.10 also has `ReadPolicy::LeaseRead`. A leader whose lease holds answers with its read
log id at once. The lease is the one openraft keeps for elections. A follower that heard from its
leader refuses every other candidate for `election_timeout_max`. So no other leader can be elected,
and nothing else committed, until the lease a quorum granted runs out.

The protocol's decision record left it unbuilt
([Q6](../distributed/protocol.md#the-questions-and-where-each-was-decided)). A lease rests on clock,
expiry and pause assumptions the barrier does not, and Q6 takes one only with a timing argument and
a measured benefit. On 2026-10-21 the user chose to measure a lease first
([Every get and stat is a strong read](../object-storage/store-optimizations/strong-reads.md)).
This feature builds it as a setting so that it can be measured, and states what it assumes. The
barrier stays the default.

## What it does

`cluster.replication.lease_reads: true` changes one thing on a node: how its shards confirm a
barrier on a group they lead. Two barriers go through `linearizer_of` (`shard/reads.rs`): a strong
read's own, asked of its shard's handle (`read_barrier`), and one another member hops to this node
for (`ReplicateKind::ReadBarrier`, `shard/groups.rs`).
- **If the shard leads, and a quorum acknowledged it within half its lease**
  (`Lease::holds_for_reads`, `replication/lease.rs`), it asks openraft for a `LeaseRead`. openraft
  checks its own lease, the whole of it, and answers the read log id with no round.
- **Otherwise, or if openraft refuses the lease read,** it asks for a `ReadIndex`, as every strong
  read did before: a round of its own, whose answer names the leader or the quorum it could not
  reach.

The read then waits for its own copy to apply through the read log id, and is served, as before.
Everything else is unchanged:
- the checks before the barrier run first: a quorum quiet on the network, and a lapsed lease
  answered `QuorumUnavailable` by name;
- a read on a member that does not lead hops to the leader;
- a placement read still takes a round.

An inventory sets it for every node in its `replication` block. `replication: {lease_reads: true}`
renders `cluster.replication.lease_reads: true` into each node's `shoal.yml`. `stores-lab.sh ab`
takes it for one side as `AB_<side>_LEASE=true`.

## Design choices

- **A setting, off by default.** Q6 asks for a measured benefit before a lease is taken. A setting
  lets one build be measured both ways, as O120's inline threshold was, and a deployment that does
  not set it is unchanged.
- **Half the lease, not openraft's whole.** openraft's leader counts its lease from when it sent the
  heartbeat a quorum acknowledged. A follower counts from when it received it. Each counts
  `election_timeout_max` by its own clock. The two lengths are equal, so openraft's own check leaves
  only the heartbeat's one-way trip for the clocks to disagree in. Shoal takes a lease read only in
  the first half, which leaves the other half: a follower's clock would have to run twice as fast
  as the leader's for a read inside it to be stale. The heartbeat is every tenth of the lease, so a
  healthy leader is always inside the half.
- **openraft's check as well as Shoal's.** Shoal judges from the group's metrics, which are
  published after the core's state moves, and so are only ever older than it. openraft judges from
  the core's own state when the read is asked. Both have to hold.
- **A refused lease read falls back to a round.** Each of these asks a `ReadIndex`: a leader whose
  lease does not hold for reads, a member that does not lead, and a lease openraft refuses. Its
  answers already name the leader to hop to or the quorum that could not be reached. The lease
  never changes what a read is refused for.
- **The leader's answer to a hop takes the same lease.** A member that does not lead asks its
  group's leader for a read log id over the lane. The leader answers inside its lease where it
  holds, so a read through any member sends no round.
- **Deployment-wide in the inventory.** A read policy is the cluster's, where the WAL commit delay
  is a device's ([F57](cluster-reconfigure.md)). So the inventory names it once, in the replication
  block every node renders. The engine takes it per node, as it takes every `cluster.replication`
  key.

## Alternatives rejected

- **A lease of Shoal's own.** Shoal's `Lease` judges a handle's authority for writes and barriers
  from openraft's metrics ([F42](primary-failover.md)). A lease read through openraft keeps one
  lease, the one elections keep. A second lease would have to be kept in step with it.
- **openraft's whole lease.** It leaves the clocks no margin but the heartbeat's trip
  ([Design choices](#design-choices)).
- **One round shared by a group's reads
  ([O125](../appendix/optimizations.md#o125-every-strong-read-sends-a-round-of-its-own-however-many-of-its-groups-are-in-flight)).**
  It needs no clock, but would take few rounds away. A table's reads spread over its 90 groups, and
  at sixteen in flight about one read in thirteen finds its group's round out.
- **Reads at `One` by default.** They are not linearizable, and the caller chooses them
  ([F95](bench-object-read-levels.md)).
- **Lease reads for placement reads too.** A placement read is a mover's or a rebuilder's, rare and
  off the client's path. It keeps its round.

## Limitations

- **With lease reads on, a strong read rests on the clocks.** Every member's clock has to run at
  the leader's rate to within half the lease, measured by the monotonic clock. **A clock that stops
  breaks it.** Linux's monotonic clock does not count a suspended host's time, and a virtual
  machine paused by its host may not either. So a leader that resumes may believe its lease holds
  after the others have elected a new leader and committed past it. Until it hears the higher
  term, its lease reads may be stale. The barrier assumes nothing of clocks.
- **Off by default, and not measured as a default.** The lab's figures are on this page. ~~The
  default is the user's choice, made with them.~~ The user kept it off on 2026-10-21, with them.
- **A node built before this feature refuses the key.** `cluster.replication` denies unknown
  fields, so an inventory that names `lease_reads` deploys only builds that know it.
- **Only at the inventory's deployment level.** No group or node names its own.
- **Placement reads keep their round.**
- **Read when a node starts**, as every `cluster.replication` key is.

## Invariants to uphold

- **A lease read is taken only while both leases hold**: Shoal's half, from the metrics, and
  openraft's whole, from its core. Never call `ReadPolicy::LeaseRead` without
  `Lease::holds_for_reads` first.
- **A refused lease read falls back to `ReadIndex`, never to another lease read.** The round is
  what names the leader or the missing quorum.
- **The heartbeat stays well inside half the lease.** `heartbeat_interval` is a tenth of the
  failover base, and the lease is the base (`group_config`). A heartbeat slower than half the lease
  would push leaders out of their lease reads between heartbeats, into rounds.
- **The setting changes how a barrier is confirmed, never whether one is asked.** Every check
  before it (`Lease::quorum_quiet`, a lapsed lease) runs as before, and the read still waits for
  its own copy's apply through the read log id.

## Performance

Measured on the lab's SSD leg with three copies on 2026-10-21 (`f100-ssd-rep3`, under
`shoal-spike/results/stores-ab/`).
- **The sides** ran one build, F101's commit (`21466829`), with `lease_reads` off (round) and on
  (lease).
- **The captures** ran in the order round, lease, lease, round, each on devices made fresh, with
  F95's arms: small reads at five depths.
- **A side's figure** is the median of its eight runs, with the lowest and highest beside it.

Reads a second:

| Arm | In flight | Round | Lease | Lease ÷ round | p99 ms |
| --- | --- | --- | --- | --- | --- |
| Gets, 4 KiB | 1 | 2,123 [1,640–2,145] | 4,102 [2,717–4,281] | **1.93, ahead** | 0.78 → 0.51 |
| | 2 | 4,260 [3,675–4,364] | 8,815 [6,702–9,114] | **2.07, ahead** | 0.84 → 0.47 |
| | 4 | 7,908 [7,454–8,008] | 17,205 [15,071–17,987] | **2.18, ahead** | 1.09 → 0.51 |
| | 8 | 12,770 [12,747–12,805] | 31,729 [31,213–32,490] | **2.48, ahead** | 2.03 → 0.60 |
| | 16 | 15,784 [15,756–15,828] | 49,120 [47,945–50,300] | **3.11, ahead** | 7.46 → 0.89 |
| Stats, 4 KiB | 1 | 2,390 [2,379–2,403] | 5,456 [5,399–5,553] | **2.28, ahead** | 0.68 → 0.41 |
| | 16 | 16,516 [15,247–16,963] | 68,054 [65,067–70,202] | **4.12, ahead** | 7.46 → 0.69 |
| Gets, 64 KiB | 1 | 618 [583–650] | 797 [782–810] | **1.29, ahead** | 2.91 → 2.27 |
| | 8 | 2,778 [2,609–2,951] | 3,100 [3,036–3,154] | **1.12, ahead** | 6.98 → 6.44 |
| | 16 | 3,435 [3,199–3,679] | 3,261 [3,200–3,333] | 0.95, within | 10.9 → 14.5, higher |

- **Every small read ran faster, at every depth, the ranges disjoint**: 4 KiB gets 1.9 to 3.1 times
  as fast and stats 2.3 to 4.1 times, more the deeper. Stats at two, four and eight in flight ran
  2.5, 2.6 and 3.0 times as fast.
- **The small reads' tail went with the round.** At sixteen in flight their p99 fell from 7.46 ms to
  0.89 for gets and 0.69 for stats
  ([Small reads have a tail across hosts](../object-storage/store-optimizations/small-read-tail.md)).
- **The round is gone.** A 4 KiB read's barrier took 0.02 to 0.04 ms on average, where a round's took
  0.24 to 0.62 ms. The lease side's members sent 1,750 to 2,000 empty appends a second together
  at every depth, about what the round side sent beside its rounds: the groups' heartbeats. So
  next to no lease read fell back to a round.
- **A read costs a quarter of the cpu.** On titan and hyperion a 4 KiB get at sixteen in flight
  took 53 to 61 µs of the node's cpu, where it took 201 to 229 µs. The rounds were most of a small
  read's cpu, as [F99](strong-read-rounds.md) found.
- **What bounds them now is europa's link.** The bench's driver runs on europa, and at sixteen in
  flight 4 KiB gets ran its 1 GbE link to 110 MiB/s received, its ceiling. Stats, whose answers are
  small, ran 68,054 a second with no link near its ceiling.
- **64 KiB gets ran 1.12 to 1.29 times as fast at one to eight in flight, and are within at
  sixteen.** There europa's link bounds them on both sides, at 105 and 114 MiB/s received. Their
  p99 rose from 10.9 to 14.5 ms: the reads that no longer wait for a round queue at the link
  instead.
- **Beside reads at `One`** ([F95](bench-object-read-levels.md)'s, another day and build, so a
  guide): a 4 KiB get at `One` ran 13,334 a second at one in flight and 75,334 at sixteen, a lease
  read 4,102 and 49,120. A lease read still goes to its group's leader, two in three across the
  link, where a read at `One` goes to a holder.
- **Beside the comparison's own captures of the other stores**, which ran on another day:
  - 4 KiB gets at sixteen in flight are ahead of Ceph and SeaweedFS by at least 2.8 and 2.6 times,
    where with the round they were behind by at least 2.7% and 13%;
  - stats are ahead of Ceph by at least 3.1 times and of SeaweedFS by at least 62%.
- **Nothing was lost.** The check at each capture's end read 2,048 objects back.

What a lease read rests on is under [Limitations](#limitations). ~~Whether lease reads are the default
is the user's choice, made with these figures.~~ The user kept the round as the default on
2026-10-21, with these figures: lease reads are a deployment's choice
([configuration](../getting-started/configuration.md#cluster)).

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal` `cluster_fixture` `lease_reads_send_no_round_and_see_every_acknowledged_write` | Ten writes through a group's leader, each followed by a strong read through every node that sees it: the leader's own and the hops to it. Then 600 strong reads alone count a barrier each, ~~and send fewer appends than half their number, where a round a read is two~~ every one obtained by its leader and fewer than a quarter with a round. The appends counted the groups' heartbeats too, which grow with the reads' time on a loaded host ([Resolved #302](../appendix/resolved/lease-read-rounds-counted.md)) |
| `shoal` `cluster_fixture` `a_lease_read_refuses_an_isolated_old_primary` | With lease reads on, an isolated old primary answers no strong read with the value from before the survivors' write. It is refused once its lease lapsed, and after the heal its reads hop to the new leader. Its twin without lease reads is `strong_read_refuses_isolated_old_primary` |
| `shoal-core` `conf::cluster::tests::the_replication_block_parses_with_its_defaults` | The setting defaults off and parses |
| `shoaladm` `deploy::render::tests::a_node_file_says_how_it_enters_and_holds_no_password` | The inventory's key renders into the replication block, and nowhere unless named |
| `shoal-bench` `deploy_render` `a_group_split_renders_the_roots_the_engine_claims` | The rendered key reaches the engine through its own loader |

## Related

- [F41. Read consistency](read-consistency.md), what a strong read's barrier is.
- [F42. Primary failover](primary-failover.md), Shoal's `Lease` and a lapsed lease answered by name.
- [F99. A strong read's round in a capture](strong-read-rounds.md), whose counts placed the tail.
- [Q6 in the protocol's decision record](../distributed/protocol.md#the-questions-and-where-each-was-decided).
- [Every get and stat is a strong read](../object-storage/store-optimizations/strong-reads.md) and
  [O118](../appendix/optimizations.md#o118-a-striped-get-pays-two-strong-reads-its-entrys-and-its-stripes-rows),
  whose default this decides.
