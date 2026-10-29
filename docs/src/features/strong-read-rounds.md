# F99. A strong read's round in a capture

Since this feature a capture says, for every run and every member, three things about the member's
strong reads:
- how long they waited for their read barriers;
- how long the empty appends the member sent took to be answered;
- how long the member took to answer the empty appends it was sent.

Those are the parts of the round a strong read confirms its leader with.
[Small reads have a tail across hosts](../object-storage/store-optimizations/small-read-tail.md)
needed them: its tail goes at `One`, so it is in the round, and a round can wait in three places.

## Context

[F95](bench-object-read-levels.md)'s figures put the small reads' tail inside the strong read
([At One and by depth](../object-storage/store-optimizations/small-read-tail.md#at-one-and-by-depth)).
At sixteen in flight the p99 of 4 KiB gets was 0.60 ms at `One` and 6.62 ms at `Quorum`.

A strong read on a group's leader confirms the lead with a round ([F41](read-consistency.md)).
openraft sends an empty append to every other voter, and the read goes on once a quorum has
answered. In openraft 0.10 every read sends a round of its own
([O125](../appendix/optimizations.md#o125-every-strong-read-sends-a-round-of-its-own-however-many-of-its-groups-are-in-flight)).
A round can wait in three places:
- **on the leader**, between the read asking for its barrier and the appends going out, and again
  between their answers and the read going on;
- **on the link**, the append out and its answer back;
- **on a follower**, between the append's arrival and its answer, which the group's core gives once
  the follower's copy has taken the append.

The node already counted each shard's barriers and the nanoseconds they took (`ReadStats`, F41),
but only as a total and a maximum, and no capture kept them. A tail is not in a mean.

[O122](../appendix/optimizations.md#o122-a-small-reads-p99-across-hosts-is-six-times-its-mean-and-nobody-knows-why)'s
page asked for a trace of a slow read. Tracing every span of reads at fourteen thousand a second
costs more than the reads do. It would also say nothing that a count by bucket of each part does
not.

## What it does

**On a node**, each shard counts three kinds of wait, each by bucket. The buckets are the ones a
client query's wait is counted in ([F65](query-figures-home-tab.md), `meter::bucket_of`): one per
microsecond under four, then four to every power of two.
- **`barriers`:** each strong read's wait for its barriers, as the shard that served it waited
  (`shard/reads.rs`, beside `ReadStats`).
- **`sent`:** each empty append the shard sent and had answered, from its send to its answer
  (`GroupPeer::append_entries`, `replication/network.rs`).
- **`answered`:** each empty append the shard answered, from its arrival to its answer (the
  shard's `AppendEntries` handler, `shard/groups.rs`).

Since [Resolved #302](../appendix/resolved/lease-read-rounds-counted.md) a shard also counts each
strong read's barrier it obtained as its group's leader, by how it obtained it:
- `by_lease`, inside its lease with no round;
- `by_round`, with a round of its own.

The count is taken in `reads::linearizer_of`, for the shard's own reads and the hops it answered.
These two tell a read's rounds from the groups' heartbeats, which `sent` cannot. A capture does not
keep them yet ([TODOs](../appendix/todos.md#a-capture-keeps-the-replication-reports-newer-counts)).

The counts are a shard's `RoundStats` (`replication/report.rs`), kept in its `ShardReplication`.
The node folds them into `NodeReplication.rounds`, which the `Replication` admin read answers. The
fixture reads them with `ROUNDS`.

**In a capture**, `shoaladm bench` reads every member's `Replication` report as the admin twice a
run (`bench/rounds.rs`): just before the run's clock starts, and once its last answer is in. These
are the moments its device counters are read. The run's `rounds` keeps what each member's counts
gained, by member (`MemberRounds`, `shoal-loadgen/src/results.rs`):
- its barriers, the nanoseconds they took, and the nanoseconds its copies waited to apply through
  them;
- the three lists.

A member that answers neither read is left out.

**In a report**, `stores-ab-report.py --rounds` prints, for each arm and side, every member and
run summed:
- the barriers a second, their mean, and their p50, p90 and p99;
- the empty appends sent a barrier, which is two a read when every read sends a round of its own to
  two other voters;
- the p50, p90 and p99 of the appends as their leaders waited for them and as their followers
  answered them.

## Design choices

- **Counts by bucket, not a trace.** A tail is a share of the reads, and buckets keep every read's
  share at the cost of an increment. They also need nothing outside the node and the bench.
- **The buckets a client query's wait is counted in.** A barrier can then be set beside the read's
  own wait. A quarter of an octave is fine enough for a tail measured in milliseconds.
- **Every empty append, a heartbeat's too.** Shoal's network cannot tell a read's round from a
  heartbeat: openraft sends both as an empty append through the same kind of client. Under a strong
  read arm the rounds outnumber the heartbeats many times over. On a cluster reading at `One` the
  lists hold the heartbeats alone, which is the floor. Since #302, a leader's choice of barrier is
  counted where it is made, which is where a round can be told from a heartbeat.
- **Timed where Shoal's code is.** A round's start and the read's continuation are inside
  openraft's core. Shoal's code sees the barrier whole, each append it sends and each append it
  answers. The leader's own part is read from the distributions: a barrier's time less what its
  appends took.
- **Read when the device counters are.** The figures cover the whole run, warmup and drain with it,
  as a run's device counters do.
- **Kept by member.** Every node leads some groups and follows others, and their cpus differ. A
  member slower than the rest shows as itself.

## Alternatives rejected

- **A trace of a slow read through [F35](wire-trace-context.md).** Spans need a collector and a
  sampling rule. At these rates a trace samples the tail away or costs more than the reads. F35
  joins a client query's spans to the server's, and an object operation's internal queries are not
  joined to its client's.
- **Timing the leader's part inside openraft.** That needs a fork of openraft. The three
  distributions bound the leader's part already.
- **The `Stats` frame ([F52](cluster-stats.md)).** Its figures are rates the leader keeps for the
  stats view. A capture wants counts over its run, as its devices give.
- **A sampler beside the bench**, as the flush counts on the lab are taken. The bench already holds
  a client of every member and knows each run's window.

## Limitations

- **The leader's own part is not timed.** A barrier that took long while its appends were answered
  quickly says the time was on the leader. It does not say where on the leader.
- **A barrier and its appends are not joined.** The lists are distributions over the run, not a
  read's own parts.
- **`sent` and `answered` count heartbeats as well as rounds.**
- **The figures cover warmup and drain**, as a run's device counters do.
- **A barrier asked of another node's leader** (a `barrier_hops` hop) is counted by the shard that
  waited for it. Its appends are counted on the leader's node.
- **Only Shoal's captures.** `bench s3` has no member to read.

## Invariants to uphold

- **A member's counts only grow.** A capture keeps what they gained, and a count below its earlier
  reading is taken whole, since that member started again.
- **Counting is an increment.** A strong read and every empty append add one to a bucket. A list
  grows only when a slower wait than any before arrives.
- **`Replication` keeps every field it had.** `rounds` is a field of its own with a default, so an
  older node's report reads as empty lists and an older reader ignores it.

## Performance

Measured on the lab's SSD leg with three copies on 2026-10-20 (`f99-ssd-rep3`, under
`shoal-spike/results/stores-ab/`).
- **The sides.** F97's build before and F99's after, in the order before, after, after, before,
  each on devices made fresh.
- **The arm** is 4 KiB gets at `Quorum`, one to sixteen in flight, over 1,024 objects preloaded.
- **Every figure** is the median of eight runs, with the lowest and highest.
- **Nothing was lost.** The check at each capture's end read 1,024 objects back.

**Counting costs nothing measured.** Every rate and every p99 is within:

| In flight | Before, gets/s | After | Ratio | p99 ms, before → after |
| --- | --- | --- | --- | --- |
| 1 | 2,082 [1,654–2,118] | 2,127 [1,623–2,157] | 1.02, within | 0.78 → 0.79, within |
| 2 | 4,112 [3,575–4,204] | 4,245 [3,517–4,297] | 1.03, within | 0.86 → 0.84, within |
| 4 | 7,581 [6,937–7,734] | 7,792 [7,127–7,952] | 1.03, within | 1.14 → 1.12, within |
| 8 | 11,694 [11,342–12,017] | 12,083 [11,706–12,355] | 1.03, within | 2.14 → 2.24, within |
| 16 | 14,316 [13,471–15,122] | 14,053 [13,609–14,538] | 0.98, within | 6.57 → 7.35, within |

The p50 was 0.07 to 0.11 ms lower after at every depth, with the means within 0.02 ms. That is one
capture's, not F99's: the second before capture's p50 was 0.48 ms at one in flight, where the other
three's were 0.38 and 0.39. Each capture runs on a cluster made for it. About half the gets are
coordinated on europa, the driver's host, and the rest across the link, so the median sits where
the two meet.

**What the counts said.** At sixteen in flight, both captures of F99's build, every run summed, in
ms:

| Member | Barriers a second | Barrier p50 / p90 / p99 | Its appends, sent to answered | Its answers, arrival to answer |
| --- | --- | --- | --- | --- |
| europa | 7,258 | 0.29 / 0.38 / 0.51 | 0.31 / 1.49 / 4.08 | 0.01 / 0.01 / 0.04 |
| titan | 2,861 | 0.34 / 0.67 / 2.13 | 0.39 / 2.57 / 4.42 | 0.02 / 0.10 / 0.52 |
| hyperion | 3,646 | 0.36 / 3.77 / 5.89 | 0.41 / 2.53 / 3.98 | 0.02 / 1.05 / 1.94 |
| all | 13,765 | 0.31 / 0.52 / 4.90 | 0.35 / 2.23 / 4.09 | 0.02 / 0.14 / 1.63 |

The appends sent a barrier were 2.91 at one in flight and 2.13 at sixteen. Less about 1,790
heartbeats a second, which every depth shares, that is 2.00 at every depth: every strong read sent
a round of its own. What the counts place is on
[Small reads have a tail across hosts](../object-storage/store-optimizations/small-read-tail.md#a-round-by-its-parts): the
round's work on busy shards of the Zen1 hosts, hyperion's most. It is not the link, and not a queue
of one group's rounds.

## Tests

| Test | What fails if the feature is reverted |
| --- | --- |
| `shoal-core` `replication::report::tests::a_round_is_counted_by_bucket_and_folded` | A wait is counted in the bucket a client query's wait of the same length is, and a node folds its shards' lists bucket by bucket |
| `shoal` `cluster_fixture` `a_strong_reads_round_is_counted_by_its_parts` | Strong gets through every node of three count a barrier each, each obtained by its leader with a round and none by a lease (since #302), and their rounds two appends sent and answered each |
| `shoaladm` `bench::rounds::tests::a_report_is_read_for_its_rounds` | A member's report is read for its barriers, waits and buckets, and an older build's for empty lists |
| `shoaladm` `bench::rounds::tests::a_run_keeps_what_each_member_gained` | A run keeps what each member's counts gained, a member that started again counted whole and one read only once left out |

## Related

- [F41. Read consistency](read-consistency.md), what a strong read's barrier is.
- [F65. Every node counts what its clients were answered](query-figures-home-tab.md), whose buckets these are.
- [F71. Device counters and node memory in a bench capture](bench-device-memory.md), read at the
  same two moments.
- [F95. Object reads at a level in the bench](bench-object-read-levels.md), whose figures asked for
  these.
- [Small reads have a tail across hosts](../object-storage/store-optimizations/small-read-tail.md)
  and
  [O125](../appendix/optimizations.md#o125-every-strong-read-sends-a-round-of-its-own-however-many-of-its-groups-are-in-flight),
  which these figures decide.
