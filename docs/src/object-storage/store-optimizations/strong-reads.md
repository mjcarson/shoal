# Every get and stat is a strong read

| | |
| --- | --- |
| **Entry** | [O118](../../appendix/optimizations.md#o118-a-striped-get-pays-two-strong-reads-its-entrys-and-its-stripes-rows), filed from the comparison's SSD leg; [O49](../../appendix/optimizations.md#o49-one-barrier-per-group-per-bundle-rather-than-per-read), filed from F41 |
| **Priority** | [4 of 9](../store-optimizations.md#in-order) |
| **Status** | ~~not started~~ ~~measurable since [F95](../../features/bench-object-read-levels.md): reads at `One` beside `Quorum` come first~~ measured at `One` beside `Quorum` ([One beside Quorum](#one-beside-quorum)): the round and its apply are about 0.3 ms of a small read across the link, and much of `One`'s lead is the link it never crosses. ~~Which of the three to build is chosen next~~ Chosen on 2026-10-20: the small reads' tail placed first by [F99](../../features/strong-read-rounds.md); then [O125](../../appendix/optimizations.md#o125-every-strong-read-sends-a-round-of-its-own-however-many-of-its-groups-are-in-flight), one round for a group's reads in flight, if the round's queue is the cause; a lease decided on O125's figures. F99 placed the tail the same day ([A round by its parts](small-read-tail.md#a-round-by-its-parts)): a round's work on busy shards of the Zen1 hosts, not a queue of one group's rounds. So O125 is not built, and the lease, or `One` by the caller, is the user's choice now, on F99's figures. The user chose a lease, measured first (2026-10-21): [F100](../../features/lease-reads.md) builds it as a setting, off by default, ~~and the SSD leg measures it~~ measured on the SSD leg (2026-10-21, [F100's figures](../../features/lease-reads.md#performance)): with lease reads on, 4 KiB gets ran 1.9 to 3.1 times as fast and stats 2.3 to 4.1 times, their p99 at sixteen in flight 0.89 and 0.69 ms where it was 7.46. ~~Whether they are the default is the user's choice~~ The user kept the round as the default (2026-10-21), lease reads a deployment's choice |

## What the comparison found

Across hosts Shoal's small reads were a little behind Ceph and well behind SeaweedFS's stats.
Shoal's verdict beside each, three copies, at one in flight, then at sixteen:

| Arm | Beside Ceph | Beside SeaweedFS |
| --- | --- | --- |
| Gets, 4 KiB, SSDs over 1 GbE | behind ≥7.1%, ≥3.2% | behind ≥7.4%, ≥13% |
| Stats, 4 KiB, SSDs over 1 GbE | behind ≥16%, ≥16% | behind ≥2.2×, ≥2.3× |
| Gets, 4 KiB, disks | behind ≥11%, ≥8.8% | behind ≥12%, ≥20% |
| Stats, 4 KiB, disks | behind ≥20%, ≥20% | behind ≥2.4×, ≥2.5× |

At 2+1 Shoal's 4 KiB gets were within Ceph's, and its stats behind by at least 15% to 21%. On one
host, over europa's loopback, the same reads were ahead:

| Arm, three copies, sixteen in flight, ops/s | Shoal | Ceph |
| --- | --- | --- |
| Gets, 4 KiB | 36,856 to 37,573 | 25,425 to 25,902 |
| Stats, 4 KiB | 38,859 to 39,181 | 28,539 to 28,991 |

At 2+1 on loopback Shoal's 4 KiB gets and stats were ahead of every store, by at least 34% to 3.4
times, and with three copies of every store but SeaweedFS's stats at sixteen in flight, which led
by at least 12%. Shoal's gets cost the least cpu of any store there, 0.24 to 0.25 cpu-seconds a
thousand against Ceph's 0.74 to 0.88.

A get past the inline threshold pays more. At 64 KiB, one in flight, three copies on SSDs, Shoal
got 547 to 597 objects a second with a p99 of 2.76 to 2.98 ms, where Ceph got 1,193 to 1,222 with
1.21 to 1.32 ms
([where Shoal is better, and where worse](../store-comparison.md#where-shoal-is-better-and-where-worse)).

**Measured again** in the comparison (2026-10-25 and 26, [Shoal measured again](../store-comparison.md#shoal-measured-again)), every read
still strong: across hosts 4 KiB gets and stats lead Ceph at sixteen in flight by at least 10% to 60%;
at one in flight they are within Ceph's gets with three copies and behind its stats by at least 2.8%
to 6.6%. 64 KiB gets at one in flight are behind Ceph by at least 13% to 35%.

## Why it would help

The driver names no read level, so every Shoal get and stat is at `Quorum`, the server's default
(`shard/objects/mod.rs`, 105). A `Quorum` read confirms its leader's lead with a round to a quorum
of its group before it answers, openraft's `ReadPolicy::ReadIndex` (`shard/reads.rs`, 959). A get
past the inline threshold makes two such reads, one after the other: its entry's, then every
stripe's row (`read.rs`, 191).

Ceph's primary OSD answers from its own state, and SeaweedFS's stat is answered by its filer
without reaching a volume server. On europa's loopback the round costs little, and Shoal's small
reads lead Ceph's by 36% to 45%. Across hosts each round crosses 1 GbE and Shoal falls behind.

Three ways to take the round off, from least to most reach:
- **One round, not two.** A striped get reads its stripe rows under its entry's barrier, where
  both groups are led on one node (O118).
- **A lease.** openraft 0.10 has `ReadPolicy::LeaseRead`: a leader inside its lease answers without
  a round (O118). Built as a setting, off by default, by [F100](../../features/lease-reads.md).
- **`One`.** A client that can take a holder's copy asks for it, and its get goes to a holder
  rather than the leader. A get at `One` never mixes two writes' bytes (the fixture's
  `one_reads_never_mix`), but it is not linearizable.

[O49](../../appendix/optimizations.md#o49-one-barrier-per-group-per-bundle-rather-than-per-read),
one barrier for every read of a group in a bundle, would not move these arms: the driver sends
every read as a bundle of its own.

## What would show it worked

- **First, what the round costs here.** ~~The driver cannot yet name a level
  ([F94's limitations](../../features/bench-object-endpoints.md#limitations)). Once it can,~~
  Since [F95](../../features/bench-object-read-levels.md) the driver reads at `--read-level`:
  `get100` and `stat100` at 4 KiB and `get100` at 64 KiB, one in flight and sixteen, on the SSD
  leg, at `One` beside `Quorum`. ~~The difference is the most any of the three can win.~~
  Measured on 2026-10-20 ([One beside Quorum](#one-beside-quorum)). At 4 KiB the difference bounds
  what any of the three can win, and is more than a lease could win, since at one in flight it is
  the link as well. At 64 KiB it is not a bound: a get at `One` still makes its stripe rows' strong
  read, which a lease would take away too.
- **Then each change, before and after,** on the same arms, at `Quorum`. It worked if the after
  captures' intervals are above the before captures', disjoint. `get100/s64k/c1` is O118's arm.
- **What must not get worse:** the fixture's strong read tests (`strong_read_observes`,
  `one_reads_never_mix`) and every capture's read back.

## One beside Quorum

Measured with [F95](../../features/bench-object-read-levels.md) on the SSD leg, three copies
(2026-10-20). One build, F97's, ran four captures, `Quorum` first and last, each on devices made
fresh. Operations a second are the median of eight runs [lowest–highest], and the p99 is the median
of each side's runs. Every difference, in rate and in p99, is disjoint:

| Arm | In flight | Quorum | One | One ÷ Quorum | p99 ms, Quorum → One |
| --- | --- | --- | --- | --- | --- |
| Gets, 4 KiB | 1 | 2,094 [1,629–2,118] | 13,334 [13,259–13,935] | 6.37 | 0.79 → 0.10 |
| | 2 | 4,199 [3,638–4,298] | 16,834 [16,679–17,343] | 4.01 | 0.86 → 0.32 |
| | 4 | 7,770 [7,224–7,987] | 32,368 [30,064–32,772] | 4.17 | 1.10 → 0.36 |
| | 8 | 12,004 [11,895–12,150] | 48,973 [48,635–49,151] | 4.08 | 2.19 → 0.42 |
| | 16 | 14,524 [14,493–14,539] | 75,334 [74,098–76,485] | 5.19 | 6.62 → 0.60 |
| Stats, 4 KiB | 1 | 2,343 [2,329–2,403] | 16,105 [15,943–16,173] | 6.88 | 0.69 → 0.09 |
| | 2 | 4,661 [4,616–4,752] | 20,981 [20,715–21,218] | 4.50 | 0.78 → 0.25 |
| | 4 | 8,469 [8,442–8,719] | 40,325 [40,038–40,530] | 4.76 | 1.03 → 0.27 |
| | 8 | 13,042 [12,724–13,695] | 63,602 [57,812–65,104] | 4.88 | 2.13 → 0.32 |
| | 16 | 15,305 [15,261–16,601] | 105,545 [84,647–111,388] | 6.90 | 6.63 → 0.43 |
| Gets, 64 KiB | 1 | 593 [590–612] | 1,591 [1,553–1,626] | 2.68 | 2.81 → 0.87 |
| | 2 | 1,105 [1,095–1,123] | 1,949 [1,903–2,004] | 1.76 | 3.49 → 2.33 |
| | 4 | 1,863 [1,845–1,915] | 3,259 [3,170–3,384] | 1.75 | 4.70 → 3.00 |
| | 8 | 2,641 [2,620–2,663] | 3,562 [3,477–3,645] | 1.35 | 7.14 → 5.27 |
| | 16 | 3,244 [3,231–3,275] | 4,010 [3,922–4,216] | 1.24 | 11.1 → 8.69 |

The `Quorum` side matches the comparison's, where Shoal's 4 KiB gets ran 2,007 to 2,159 a second at
one in flight and its 64 KiB gets 547 to 597. Every one of the 22.5 million reads at `Quorum` and
107 million at `One` was answered and its bytes checked, with no miss, retry or error, and each
capture's read back found all 2,048 objects.

**At one in flight, most of `One`'s lead is the link, not the round.** A read at `One` is sent to
the node its client was made for ([F74](../../features/client-routing.md)). With three copies on
three nodes, that node holds every row. The bench makes a client a node and deals its streams round
them, europa's first, as the comparison deals them round an S3 store's endpoints. So at one in
flight every read at `One` was answered on europa, the driver's own host, over loopback: europa's
loopback carried 123 MB/s and no host's link more than 1.2 MB/s. At sixteen in flight, six streams
of the sixteen stayed on europa. A read at `Quorum` goes to its row's group leader. By europa's
loopback bytes that leader was on europa for about half the reads, and across the link for the rest.

The two apart, for a 4 KiB get:
- **On europa, at `One`:** 0.075 ms, 13,334 a second for one stream.
- **Across the link, at `One`:** about 0.27 ms. At two in flight the second stream read hyperion's
  copy, and hyperion's link bytes put that stream at about 3,700 a second.
- **At `Quorum`, one in flight:** 0.48 ms on average.

So the round, and what waits on it, is about 0.3 ms of a small read across the lab's 1 GbE.
Reaching the leader is the rest. By these figures, which are an estimate and not a measurement,
each of the three ways could win at most:
- **A lease** keeps the leader and takes the round away. A 4 KiB get at one in flight would fall
  from 0.48 ms to about 0.17, near three times `Quorum`'s rate. A 64 KiB get would lose both its
  rounds.
- **One round, not two** wins nothing at 4 KiB, where a get or a stat is one strong read of an
  inline entry. It takes one of a 64 KiB get's two.
- **`One`** takes the round away. For a caller on a holder's host it takes the link too, which is
  the 6.4 times above. A caller on no holder's host reads across the link, at about 1.8 times
  `Quorum`'s rate. At 64 KiB it takes only the entry's round: a get at `One` still reads its
  stripes' rows at `Quorum` (`read.rs`, 191). Its 2.68 times at one in flight is that round, plus
  the 64 KiB answer over loopback rather than across the link for about half the reads. 64 KiB is
  half a millisecond of 1 GbE.

**At sixteen in flight the round is the tail as well.** At `Quorum` the 4 KiB arms' p99 climbs to
6.6 ms, and a tenth of the reads take over 3 ms
([Small reads have a tail across hosts](small-read-tail.md#at-one-and-by-depth)). At `One` the
sixteen streams' p99 is 0.43 to 0.60 ms, though ten of them cross the link. In openraft 0.10 every
strong read sends a round of its own, so sixteen reads of a group in flight are sixteen rounds
([O125](../../appendix/optimizations.md#o125-every-strong-read-sends-a-round-of-its-own-however-many-of-its-groups-are-in-flight)).
One round shared by the reads that arrived before it was sent would keep every read linearizable
and need no clock, and would win nothing at one in flight. F99's counts (2026-10-20) found the
arms' reads spread over the table's 90 groups: at sixteen in flight about one read in thirteen
finds its group's round out, so sharing rounds would take few away. The tail is a round's work on
busy shards of the Zen1 hosts ([A round by its parts](small-read-tail.md#a-round-by-its-parts)),
which only a lease or `One` takes away.

**Beside the other stores.** This is a guide only, since the comparison's captures ran on another
day:

| Arm | Ceph: Quorum → One | SeaweedFS: Quorum → One |
| --- | --- | --- |
| Gets, 4 KiB, one in flight | behind ≥8.9% → ahead ≥5.4× | behind ≥9.1% → ahead ≥5.5× |
| Gets, 4 KiB, sixteen | behind ≥10% → ahead ≥4.3× | behind ≥20% → ahead ≥4.0× |
| Stats, 4 KiB, one | behind ≥16% → ahead ≥5.3× | behind ≥2.2× → ahead ≥2.7× |
| Stats, 4 KiB, sixteen | behind ≥14% → ahead ≥4.0× | behind ≥2.3× → ahead ≥2.1× |
| Gets, 64 KiB, one | behind ≥48% → ahead ≥27% | behind ≥43% → ahead ≥37% |
| Gets, 64 KiB, sixteen | ahead ≥54% → ahead ≥87% | ahead ≥43% → ahead ≥74% |

At one in flight this sets Shoal's reads, which never left europa at `One`, beside gateways on
europa that read wherever their store placed the object. SeaweedFS's stats are answered by its
filer on europa, so they stay on europa too.

## What it could cost

- **A lease** holds only while clocks drift less than its margin, and nothing on a node measures
  that. Every strong read of every table would rest on it.
- **A stripe row read under its entry's barrier** is linearizable only if every commit that changes
  a stripe's row is ordered behind the entry the read saw. Nobody has established that.
- **`One`** gives up linearizability, which is the caller's to choose, not the store's.

## Related

[O118](../../appendix/optimizations.md#o118-a-striped-get-pays-two-strong-reads-its-entrys-and-its-stripes-rows);
[O49](../../appendix/optimizations.md#o49-one-barrier-per-group-per-bundle-rather-than-per-read);
[Small reads have a tail across hosts](small-read-tail.md), the same reads' p99;
[F41. Read consistency](../../features/read-consistency.md); [S9. The read path](../read-path.md).
