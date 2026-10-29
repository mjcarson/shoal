# Small reads have a tail across hosts

| | |
| --- | --- |
| **Entry** | [O122](../../appendix/optimizations.md#o122-a-small-reads-p99-across-hosts-is-six-times-its-mean-and-nobody-knows-why), filed from the comparison |
| **Priority** | [7 of 9](../store-optimizations.md#in-order) |
| **Status** | ~~to diagnose: the cause is not known; since [F95](../../features/bench-object-read-levels.md) the arm can be run at `One`~~ ~~to diagnose: the tail goes at `One`, so it is the strong read's ([At One and by depth](#at-one-and-by-depth)); which part of the round is not known, and a trace of a slow read comes next~~ placed by [F99](../../features/strong-read-rounds.md) (2026-10-20): the round's work on its leaders' and followers' shard loops on the Zen1 hosts, hyperion's most. It is not the link, and not a queue of one group's rounds ([A round by its parts](#a-round-by-its-parts)). ~~The change is the user's choice~~ The user chose a lease, measured first: with [F100](../../features/lease-reads.md#performance)'s lease reads on, the tail is gone, the p99 of 4 KiB gets and stats at sixteen in flight 0.89 and 0.69 ms where it was 7.46 (2026-10-21). ~~Whether they are the default is the user's choice~~ The user kept the round as the default, lease reads a deployment's choice, so the tail stays where a deployment keeps the round |

## What the comparison found

The p99 of 4 KiB gets and stats at sixteen in flight, Shoal's beside the best store's, three
copies:

| Leg | Shoal's gets | Shoal's stats | Shoal's mean | Ceph's gets | SeaweedFS's stats |
| --- | --- | --- | --- | --- | --- |
| SSDs over 1 GbE | 5.78 to 7.46 ms | 5.96 to 7.21 ms | about 1.1 ms | 2.09 to 2.65 ms | 0.82 to 0.85 ms |
| Disks over 1 GbE | 6.33 to 7.27 ms | 6.29 to 7.19 ms | about 1.2 ms | 2.10 to 2.65 ms | 0.82 ms |
| europa's loopback | 0.83 to 0.86 ms | 0.79 to 0.80 ms | 0.41 to 0.43 ms | 1.00 to 1.04 ms | 0.70 to 0.72 ms |

Across hosts Shoal's p99 is five to seven times its mean, and behind every store at both legs and
in both groups: by at least 2.1 to 2.9 times beside Ceph and SeaweedFS with three copies on SSDs.
On one host the same reads have a p99 twice their mean and ahead of Ceph's; at 2+1 there, 0.81
to 0.86 ms for gets and 0.79 to 0.81 for stats, ahead of every store. At one in flight the
tail is small on every leg: 0.77 to 0.82 ms on disks against Ceph's 0.56 to 0.68.

**Measured again** in the comparison (2026-10-25 and 26, [Shoal measured again](../store-comparison.md#shoal-measured-again)), every
read still a strong read and no lease: the p99 of 4 KiB gets and stats at sixteen in flight was 2.2
to 3.6 ms on the SSD and disk legs, where the first run's was 5.8 to 7.9, against a mean near 0.7 ms,
and 0.52 to 0.65 ms on loopback. What took it down was not separated: the second run changed Shoal's
lanes, europa's kernel and its hosts' slow start after idle at once, and no before and after of this
wave ran a 4 KiB read.

## Why it would help

A store answering small reads at a p99 of 2 ms rather than 7 serves a caller that waits on several
of them, a page of thumbnails or a listing's stats, three times sooner.

What is known about the cause:
- **It comes with the link.** On loopback the round to a quorum is cheap and the tail is gone.
- **It comes with depth.** It is small at one in flight and large at sixteen.
- **Every one of these reads is a strong read.** Each is a ReadIndex round from its group's leader
  to a quorum before it answers
  ([Every get and stat is a strong read](strong-reads.md)), and Ceph's primary answers without one.
- **It goes at `One`**, which takes no round, though ten of sixteen streams there cross the link
  (since [F95](../../features/bench-object-read-levels.md), [At One and by depth](#at-one-and-by-depth)).

Candidates, ~~none tested~~ the third made unlikely by F95's figures and the second replaced by
what openraft's source says:
- the round's messages queueing on a peer lane behind other traffic;
  [O94](../../appendix/optimizations.md#o94-a-nodes-sockets-set-no-tcp_notsent_lowat) is one such
  queue;
- ~~the round waiting on openraft's own scheduling of heartbeats~~ not so: openraft 0.10 sends a
  read's round at once, not with its periodic heartbeats. But it sends every read a round of its
  own, so sixteen reads of a group in flight are sixteen rounds, each answered by every follower's
  core in turn
  ([O125](../../appendix/optimizations.md#o125-every-strong-read-sends-a-round-of-its-own-however-many-of-its-groups-are-in-flight));
- ~~the shard's loop holding a read's answer behind the other fifteen~~ unlikely: at `One` sixteen
  streams ran on the same shards' loops at five times the rate, with no tail.

## What would show it worked

First, what the tail is:
- **The same arm at `One`**, ~~once the driver can name a level
  ([F94's limitations](../../features/bench-object-endpoints.md#limitations))~~ which the driver
  can name since [F95](../../features/bench-object-read-levels.md) (`--read-level one`).
  `get100/s4k/c16` and `stat100/s4k/c16` on the SSD leg at `One` and `Quorum`. If the tail goes at
  `One`, it is the round's. It went (2026-10-20, [At One and by depth](#at-one-and-by-depth)).
- **A depth sweep,** the same arms at 2, 4 and 8 in flight, for where it begins. It begins between
  four and eight in flight.
- **A trace of a slow read**, which is what is left, ~~through the engine's trace propagation
  ([F35](../../features/wire-trace-context.md)), with each peer lane's socket queues sampled while
  the arm runs.~~
  counted by its parts since [F99](../../features/strong-read-rounds.md): each member's barrier
  waits, and its empty appends as their leaders waited for them and as their followers answered
  them, by bucket in every capture. ~~The same arms before and after F99 are the next run.~~ They
  ran on 2026-10-20 ([A round by its parts](#a-round-by-its-parts)).

Then, for whatever change follows, the same arms before and after. It worked if their p99 runs are
below the before captures', disjoint, and their rates are no lower.

## At One and by depth

Measured with [F95](../../features/bench-object-read-levels.md) on the SSD leg, three copies
(2026-10-20). These are the captures that
[Every get and stat is a strong read](strong-reads.md#one-beside-quorum) reports, where the rates
are. Latencies are in ms, each the median of eight runs. Every p99 run at `One` is below every p99
run at `Quorum`:

| Arm | In flight | p50, Quorum → One | p90 | Mean | p99 |
| --- | --- | --- | --- | --- | --- |
| Gets, 4 KiB | 1 | 0.40 → 0.08 | 0.67 → 0.08 | 0.48 → 0.07 | 0.79 → 0.10 |
| | 2 | 0.39 → 0.08 | 0.69 → 0.28 | 0.48 → 0.12 | 0.86 → 0.32 |
| | 4 | 0.41 → 0.08 | 0.80 → 0.29 | 0.51 → 0.12 | 1.10 → 0.36 |
| | 8 | 0.49 → 0.10 | 1.27 → 0.33 | 0.67 → 0.16 | 2.19 → 0.42 |
| | 16 | 0.54 → 0.13 | 3.08 → 0.43 | 1.10 → 0.21 | 6.62 → 0.60 |
| Stats, 4 KiB | 1 | 0.37 → 0.06 | 0.58 → 0.07 | 0.43 → 0.06 | 0.69 → 0.09 |
| | 2 | 0.37 → 0.07 | 0.60 → 0.19 | 0.43 → 0.09 | 0.78 → 0.25 |
| | 4 | 0.40 → 0.07 | 0.72 → 0.20 | 0.47 → 0.10 | 1.03 → 0.27 |
| | 8 | 0.45 → 0.08 | 1.17 → 0.24 | 0.61 → 0.13 | 2.13 → 0.32 |
| | 16 | 0.49 → 0.10 | 2.96 → 0.30 | 1.04 → 0.15 | 6.63 → 0.43 |

The `Quorum` side's p99 at sixteen in flight, 6.34 to 6.93 ms for gets and 6.22 to 7.24 for stats,
is the comparison's tail again.

At `One` a read goes to the node its stream's client was made for, and the bench deals its streams
round the three nodes, europa's first. So at one in flight every read stayed on europa, the driver's
own host. From two in flight the p90 and p99 at `One` are the streams that read titan's and
hyperion's copies across the link, about 0.27 ms a get, and the p50 is europa's own, under 0.1 ms
([Every get and stat is a strong read](strong-reads.md#one-beside-quorum)).

What this says about the cause:
- **The tail goes at `One`.** At sixteen in flight the p99 falls from 6.62 to 0.60 ms for gets and
  from 6.63 to 0.43 for stats. By this page's own test, it is the round's.
- **It is not the link's alone.** At `One` ten of the sixteen streams read across the link, and
  their reads make its p99 of 0.60 ms.
- **It is unlikely to be the shard's loop.** At `One` sixteen streams ran on the same shards' loops
  at five times the rate, with no tail.
- **It begins between four and eight in flight.** From one to sixteen in flight the p99 at `Quorum`
  is 0.79, 0.86, 1.10, 2.19 and 6.62 ms, while the p50 moves only from 0.40 to 0.54.
- **It is not a few reads.** At sixteen in flight a tenth of the reads at `Quorum` take more than
  3 ms (the p90 is 3.08), and the mean, 1.10 ms, is twice the median.
- **The rate flattens where the tail grows.** From four to eight in flight the reads at `Quorum` rose
  55%, and from eight to sixteen 21%, so something every strong read shares is near its limit.
  No host was busy as a whole: titan's and hyperion's cpus were 40 to 43% busy at sixteen in
  flight, and europa's 8%. One shard's core could still be.

## A round by its parts

Measured with [F99](../../features/strong-read-rounds.md) on the SSD leg, three copies
(2026-10-20, `f99-ssd-rep3`). These are 4 KiB gets at `Quorum`, one to sixteen in flight. Each
member's counts are summed over both captures of F99's build, eight runs an arm. F99's page has
the before and after ([Performance](../../features/strong-read-rounds.md#performance)). Times are in
ms:

| In flight | Member | Barriers a second | Barrier p50 / p90 / p99 | Its appends, sent to answered, p90 / p99 | Its answers as a follower, p90 / p99 |
| --- | --- | --- | --- | --- | --- |
| 8 | europa | 6,235 | 0.27 / 0.34 / 0.45 | 0.55 / 1.43 | 0.01 / 0.04 |
| | titan | 2,457 | 0.31 / 0.54 / 1.10 | 0.79 / 1.53 | 0.06 / 0.22 |
| | hyperion | 3,121 | 0.33 / 0.79 / 1.48 | 0.80 / 1.41 | 0.11 / 0.37 |
| 16 | europa | 7,258 | 0.29 / 0.38 / 0.51 | 1.49 / 4.08 | 0.01 / 0.04 |
| | titan | 2,861 | 0.34 / 0.67 / 2.13 | 2.57 / 4.42 | 0.10 / 0.52 |
| | hyperion | 3,646 | 0.36 / 3.77 / **5.89** | 2.53 / 3.98 | **1.05 / 1.94** |

From one to four in flight every member's barrier p99 was 0.54 to 0.76 ms, and its answers' p99
0.12 ms or less.

How to read them:
- **A barrier is counted where its read waited.** A strong read is sent to its group's leader, so
  that is nearly always the leader. Europa leads about half the groups, by its weight of two.
- **A member's appends are the rounds it sent as a leader**, two a round, each timed on the leader
  from its send to its answer. A round needs one answer of the two.
- **A member's answers are the appends it was sent as a follower**, timed from their arrival on its
  shard to its answer.

What they place:
- **Every strong read sends a round of its own.** The appends sent a barrier were 2.91 at one in
  flight and 2.13 at sixteen. Less about 1,790 heartbeats a second, which every depth shares, that
  is 2.00 at every depth.
- **The tail is the reads hyperion leads, and some of titan's.** At sixteen in flight hyperion's
  barriers had a p99 of 5.89 ms, titan's 2.13 and europa's 0.51, though europa waited for more than
  half of them.
- **It is not the link.** Europa's own appends took as long as the others' to be answered, a p99
  of 4.08 ms, yet its barriers had no tail. One prompt answer of the two is enough.
- **It is on the Zen1 leaders themselves.** Europa answered appends within 0.04 ms at the p99, a
  link's round trip from hyperion. Yet hyperion's barriers' p99, 5.89 ms, is above its appends'
  3.98, and a barrier needs only the sooner of its two. So the time is on hyperion, around the
  round: its shard taking the read's request, sending the appends, and taking their answers back.
- **Hyperion's shards were slow as followers too.** A tenth of the appends hyperion was sent waited
  more than 1.05 ms for their answer, against 0.01 on europa and 0.10 on titan. At sixteen in
  flight hyperion's shard loops were busy enough that every step of a round waited there.
- **It is not a queue of one group's rounds**
  ([O125](../../appendix/optimizations.md#o125-every-strong-read-sends-a-round-of-its-own-however-many-of-its-groups-are-in-flight)).
  The bucket's table has 90 groups, its 4,096 tablets' replica sets: a shard on each node, in three
  orders. At sixteen in flight a group's reads arrive about 150 times a second, and a barrier lasts
  about half a millisecond on average. So about one read in thirteen finds its group's round out,
  and about one in seven on hyperion. That is an estimate from these figures, not a count. Sharing
  rounds would take few away.
- **Why hyperion more than titan was not traced.** The two are the same hardware. Hyperion waited
  for 26% of the barriers and titan for 21%, so hyperion led more of the groups read.

## What it could cost

~~Not known until the cause is.~~ The cause is a round's work on a busy shard, so the cost is that
of taking rounds away. A lease costs a bound on clock drift, and `One` costs linearizability
([Every get and stat is a strong read](strong-reads.md#what-it-could-cost)). Sharing a group's
rounds costs a read that arrives just after one left up to two rounds, and would win little here.

## Related

[O122](../../appendix/optimizations.md#o122-a-small-reads-p99-across-hosts-is-six-times-its-mean-and-nobody-knows-why);
[Every get and stat is a strong read](strong-reads.md);
[O94](../../appendix/optimizations.md#o94-a-nodes-sockets-set-no-tcp_notsent_lowat).
