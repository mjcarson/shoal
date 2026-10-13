# X12. Recovery and scrub rates, measured

**Reported 2026-10-09.** This is the record of spike
[X12](spikes.md#x12-recovery-and-scrub-rates). It ran the pipelines without the protocol around
them, on stripes of real parity and real checksums, beside a foreground on every device the lab
has:

- a rebuild that reads `k` chunks, computes one and writes it whole;
- a deep scrub that reads every unit, verifies it, and checks a stripe's parity by its summaries.

Each ran at four paces: none, a fixed byte budget, the slice's idle time with and without a
ceiling, and no bound. It ran on the rotational disk and the SSD of all three hosts, four rounds a
device, and then a rebuild across hosts whose survivors came over 1 GbE. Two supplements followed
the rounds: the cpu measurement with a shorter summary, and the disks' scrub and rebuild with
windows four and a half times as long.

[S18](contract.md#q28-and-q29-and-q17-in-part-recovery-and-scrub-rates-2026-10-09) records the
answer to Q28 and Q29, and Q17 in part:

**A device's rebuild and scrub are paced by its idle time, one piece in flight, with a byte
ceiling; a rotational pool is laid out to survive a second loss while it rebuilds, and its rebuild
is spread over many destinations; a deep scrub's interval follows the device's size; the parity
check runs on a sample of deep scrubs.**

Eight facts decide it:

- **A fixed byte budget is the wrong shape.** It issues its pieces whatever the foreground is
  doing. On the 970 EVO a fixed 50 MiB/s scrub doubled the foreground's read p99, while one paced
  by idle time read the device at 385 MiB/s and left it at 1.0×. On hyperion's disk a rebuild paced
  by idle time ran 2.3 times as fast as the best fixed budget that stayed under 2×, and on titan's
  no fixed budget stayed under it (R2 fires on both). With windows of 90 s R2 fired on all three
  disks, idle pacing 1.96 to 2.57 times the best fixed budget.
- **A disk rebuilds slowly whatever its pace, so R1 fires on every disk.** Whole chunks written
  with the disk's cache off went at 58 to 63 MiB/s unbounded and 18 to 26 MiB/s paced by idle time.
  The fastest pace inside 2× put a 16 TiB disk at 200 to 470 hours onto one destination in the
  rounds, and 183 to 254 in the supplement. Unbounded it would still take 75 to 80 hours, and
  1 GbE caps a 4+2 rebuild at 28 MiB/s anyway.
- **A disk's deep scrub paced by idle time reads 44 to 59 MiB/s**, so a 16 TiB disk takes 3.3 to
  4.4 days. No pace above 5 MiB/s kept the foreground wholly within
  1.25×, even with windows of 90 s, and a fixed budget of 5 or 10 MiB/s cost the foreground about
  what idle pacing did at five to ten times the rate. So S1 fires on every disk: a scrub there
  runs as fast as idle time allows, and its interval is how long a pass takes.
- **An SSD's scrub paced by idle time costs its foreground nothing.** On the 970 EVO it read
  385 MiB/s, 4 TiB in three hours, with the read p99 at 1.0× and the write p99 at 0.98×. A rebuild
  into it is another matter: whole chunks synced beside a foreground's synced writes cost 2× or
  more at every pace that rebuilt anything.
- **On the Optane no pace stays under either line.** The foreground's own read p99 there is
  86 µs. A 1 MiB piece in flight is several times that, and so is the 0.2 ms the background's cpu
  holds the executor under a 100 µs goal: the scrub's cpu alone, with no reads, raised the read p99
  6.6 times.
- **The parity check is a second pass over every unit, and P1 fires.** Folded in the checksum's
  own pass, it added 0.49 of the checksum's cost on Zen1 and 0.71 on Zen4. Folded to one 4 KiB block
  it added 0.43 and 0.62, so the cost is the pass, not the summary. The check runs on a sample of
  deep scrubs. Folded either way, a planted parity fault was found every time the walk reached it.
- **Q17: rebuilding a whole chunk for one missed unit costs 2.7 to 3.7 units' rebuilds on a disk,
  12 to 17 on the 970 EVO and 31 to 38 on the Optane.** That is where a record of unit ranges stops
  paying for itself.
- **Across 1 GbE a rebuild reaches its link's bound**: 0.95 and 0.96 of 112 MiB/s ÷ k for 2+1 and
  4+2 into the Optane. Into a disk it is held by the disk instead: 45 MiB/s for a copy.

**What was found on the way**:

- europa's executor, beside its rotational disk, started a foreground operation 0.6 to 0.9 ms late at
  the p99 with no background at all, where titan's and hyperion's started 0.06 to 0.09 ms late.
- titan's and hyperion's 970 EVO acknowledged a 4 KiB stage at 14.7 ms at the p99 when it carried
  only the disk leg's journal.
- A whole-chunk write at one piece in flight cannot exceed about 30 MiB/s on a disk with its cache
  off.

What X12 did not settle is under [What X12 does not settle](#what-x12-does-not-settle).

## The question

[Q29](contract.md#questions-to-answer) asks what a device's budget for recovery and moves should
be, and [Q28](contract.md#questions-to-answer) a scrub's cadence and budget, and whether a deep
scrub's check of parity runs on every pass or on a sample. [S10](recovery.md#budgets) holds a
rebuild to bytes a second for each device, as a source and as a destination, and
[S11](scrub.md#schedule-and-budget) a scrub to the same budget, last. Neither says what the budget
is. [X7](device-store-hdd.md#11-a-foreground-beside-a-scrub) found a fixed one does not protect a
disk's foreground: no budget of 10 MiB/s or more kept a small write's p99 wholly within 1.25×, and
on europa's disk 10 MiB/s raised it 1.7 to 2.7 times. So [Q23](contract.md#q23-what-a-rotational-device-needs-2026-10-06)
paced a disk's scrub by the arm's idle time with the budget as its ceiling, and handed X12 the
pacing and the ceiling. [Q17](contract.md#questions-to-answer) asks, among other things, at what
granularity a slice's record of missed writes is kept: a record of stripes rebuilds a whole chunk
for a missed 4 KiB write, a record of unit ranges rebuilds less and is larger
([S10](recovery.md#how-a-slice-learns-what-it-missed)).

The spike's section named two results in advance that would change the design:

- a device's rebuild at a budget the foreground tolerates takes long enough that the default
  `k + m` and `f` leave a pool exposed for days: then the defaults change, or the budget adapts to
  the foreground, which a fixed one does not;
- a deep scrub inside its budget cannot finish in its interval: then the interval is a function of
  the device's size and not a constant.

## How it was judged

The plan's two results became five triggers, agreed with the user on 2026-10-09 before any
harness code was written, each a line `shoal-spike device report` judges as written. The lines on
the foreground are [S15's hypotheses](performance.md#the-acceptance-numbers): a rebuild keeps the
foreground's p99 under twice its own, a deep scrub within 1.25 times. Every comparison is between
intervals over four rounds, each side's lowest and highest round with the median between, and a
ratio is taken round by round, as [X7](device-store-hdd.md#how-it-was-judged) judged its own. A
line fires when a figure's whole interval lies past it, and is at the line when the interval
straddles it.

| Trigger | Fires when | Then |
| --- | --- | --- |
| **R1. A rebuild leaves a pool exposed for days** | At the fastest pace whose foreground read and write p99 both lie wholly under **2×** their p99 alone, rebuilding a **16 TiB** disk onto one destination takes more than **48 hours**, or a **4 TiB** SSD more than **12**, the device's own rate and not the network's | A rotational pool's defaults change to a layout that survives a second loss during a rebuild, and its budget adapts to the foreground |
| **R2. A fixed budget wastes the device** | Pacing by idle time, inside the same 2×, rebuilds more than **1.5×** as fast as the best fixed budget inside it | A device's rebuild budget adapts to the foreground: idle-paced, with a ceiling |
| **S1. A deep scrub cannot finish in its interval** | At the fastest pace whose foreground lies wholly within **1.25×**, a deep scrub of a **16 TiB** disk takes more than **7 days**, Ceph's default interval, or of a **4 TiB** SSD more than **one** | The scrub interval is a function of the device's size and not a constant |
| **S2. Idle pacing alone does not keep the foreground clear** | A scrub paced by idle time with no ceiling lies wholly above **1.25×**, in the read or the write, at every piece size measured (256 KiB, 1 MiB, 4 MiB) | A ceiling below the device's rate is required, and X12 names the largest inside the line |
| **P1. The parity check is too dear for every pass** | Folding a chunk's units into its summary, in the checksum's own pass, adds more than **a quarter** of the checksum's cost on one core | The summary check runs on a sample of deep scrubs. Otherwise on every one |

Reported without a trigger, as the plan asked: MiB a second for a copy and for a decode; the
foreground's tail at every pace; the rate a device gives as a rebuild's source inside 2×; the
rebuild across hosts against its link's bound; and the hours every rate means for devices of 1, 4
and 16 TiB, with how many destinations share a 16 TiB rebuild to finish it in a day. Reported for
Q17: one missed unit rebuilt as its whole chunk and as the unit alone, and the ratio between them.
And X7's H4 again, a fixed 10 MiB/s scrub against none, now with the disk's cache off and the
foreground's journal on the SSD, as Q23 has a rotational device.

## What was run

### The harness

X12 is X6's and X7's harness, `shoal-spike device` ([X6](device-store-ssd.md#the-harness)), with
measurements of its own that run only when named, so `device all` runs X6's and X7's as it did.
It writes bytes nothing in the product parses, and is thrown away like every spike's code. It
adds `rusty_erasure` 0.4.1 to `shoal-spike`'s dependencies, an edge in the lockfile and no crate:
X9 brought the crate into the lockfile, and its sub-crates stay at the 0.4.1 X4 measured.

**Real stripes.** X7's population is seeded noise nobody verifies. X12's is stripes as S6 and S8
describe them (`shoal-spike/src/device/stripes.rs`):

- every chunk a file of its own, of 4 MiB in 64 KiB units, X7's floor under a rotational pool's
  chunk;
- a header block naming the chunk's stripe and position and carrying a CRC-64/NVME of every unit
  ([X5](checksums.md), through `crc-fast`);
- the data chunks seeded noise, and the parity encoded by `rusty_erasure` on ISA-L's Cauchy
  matrix ([X4](erasure-coding-crates.md));
- a 4+2 population of 192 stripes and a 2+1 of 256, about 7.5 GiB; a copy is read from the 4+2
  data chunks, every copy of a chunk being the same bytes;
- a stripe's positions in one object directory, spread over 64 placement groups as X7's chunks
  are, since one device stands for every device a rebuild reads or writes.

Two stripes of the 4+2 population carry a fault planted as they are written: a parity byte
changed with its unit's checksum made to match, which only the summary check can see, and a data
byte changed with its checksum left as it was. Every rebuilt chunk is checked against the one it
replaces, and a deep scrub that reaches a planted stripe must report it and nothing else.

**Four paces** (`paced.rs`). Every background piece passes a pacer before it is issued:

- `none`: no background, the foreground alone;
- `fixed-N`: a budget of N MiB/s of device bytes, its allowance capped at one piece. X7's
  schedule from the start caught up after a stall, in a burst just when the foreground had been
  slow; this one loses the stall instead, and each side records how much of its budget it got;
- `idle` and `idle-ceil-N`: a piece issued only while the slice has no foreground operation in
  flight, which a gauge on the slice's executor counts from each operation's slot to its end, with
  and without a ceiling;
- `unbounded`: as fast as it goes, eight pieces in flight.

A fixed budget and idle pacing keep one piece in flight on a disk, and a fixed budget four on an
SSD, since one at a time cannot reach an SSD's larger budgets. The background runs in a task
queue of its own below the foreground's, as [S13](isolation.md#io-on-a-slice) orders a slice's
work, with a latency goal of 100 µs and its cpu in steps of a 64 KiB unit, which is what
[X9](table-latency.md#4-steps-inside-a-unit-the-supplement) found holds a shared executor to the
goal and a step.

**The foreground** (`foreground.rs`) is X7's: small writes made as S6 makes them, a 4 KiB record
staged on a journal and then the unit and the header written in place and the chunk synced, and
64 KiB reads of a random unit, each timed from its slot. On a disk the journal is on the host's
SSD, as Q23 decided, and the rates are X7's, 20 reads and 10 writes a second; on an SSD the
journal is on the device and the rates are those X7's `shared` gave an SSD's slice, 1,000 and
200. The slots are drawn from a seeded Poisson process, the same for every side of a round, so a
periodic foreground cannot fall into step with a periodic background. A write is judged to
durable in place, which continues X7's H4; its stage, which is what a client is acknowledged
after, and how late each operation started, which is the time the executor was held, are kept
beside it.

X12's measurements:

| Measurement | What one side does | Cells |
| --- | --- | --- |
| **codec** | One pinned core over stripes held in memory, out of cache: the checksum of every unit of a chunk, the fold of every unit into a summary, the two in one pass, a copy, a decode of a data and of a parity chunk, a rebuild's whole cpu for one chunk (its survivors verified, the chunk made, its own checksums), and one 4+2 stripe's summaries encoded and compared; the planted faults checked in memory | 1 s an op |
| **rebuild** `role=dest` | The destination: the survivors of eight 4+2 stripes held in memory, standing in for chunks that arrived over the network, each verified as received; the lost chunk decoded, checksummed, checked and written whole as S6 writes a chunk (into a file of a pool written ahead with zeros, synced, renamed over the chunk's name, its object's directory synced) | `none`, fixed budgets (disk 10, 20, 40, 80 MiB/s; SSD 100, 200, 400, 800), `idle`, `idle-ceil` (40; 400), `unbounded` (two rebuilds at once on a disk, four on an SSD); 3 s warm-up, 20 s counted |
| **rebuild** `role=local` | The spike's literal pipeline on one device: the survivors read whole, verified, the lost chunk decoded and written whole, for a copy, a 2+1 and a 4+2 | `idle`, `unbounded` |
| **deep** | A deep scrub: every chunk of a stripe read whole, every unit verified against its header and folded, and the stripe's summaries checked when every unit passed; a stripe at a time, placement group by placement group from a starting group drawn for the side | `none`, fixed budgets (disk 5, 10, 20, 40 MiB/s; SSD 50, 100, 200, 400), `idle`, `idle-ceil` (20; 200), `unbounded`, in pieces of 1 MiB; `idle` again in pieces of 256 KiB and 4 MiB; on an SSD a `cpu` side, the same verification over stripes in memory with no reads |
| **granularity** | Q17's: one missed unit rebuilt as its whole chunk (the survivors read whole and verified, the chunk decoded and written whole) and as the unit alone (the unit read from each survivor and verified, decoded, written in place with its header, the chunk synced), one at a time with nothing beside it | 2+1 and 4+2; 2 s warm-up, 10 s counted |
| **rebuild-net** | A destination whose survivors come over the network: `device serve` on titan and hyperion answers a request for a chunk of its disk's populations with the chunk read whole and verified, and europa fetches every survivor of a stripe at once, each from a peer of its own where there are enough, two rebuilds in flight, beside its foreground | copy, 2+1 and 4+2; europa's disk and its Optane as the destination |

**Rounds and legs.** A leg is a device in a round: the disk at `/hdd`, and the SSD. A round runs
both on every host, the disk first in odd rounds and the SSD first in even ones, and inside a
measurement the sides run in their declared order in odd rounds and reversed in even ones. Four
rounds after a quick pass, `shoal-spike/results/x12-lab.sh`, each host a systemd unit of its own,
the three at once; then the rebuild across hosts, four rounds of its own. `device report` merges
the rounds and judges the triggers; its output is `shoal-spike/results/x12-report.md`.

### Where, and on what

| Leg | Host | CPU | Device | Beside it |
| --- | --- | --- | --- | --- |
| **titan hdd** | titan | Ryzen Embedded V1756B (Zen1); the executor on cpu 2, its blocking thread on 3 | WDC WD140EDFZ-11A0VA0, firmware 81.00A81, 12.7 TiB, CMR, XFS at `/hdd`, **write cache off** | The foreground's journal on the 970 EVO's XFS volume at `/xfs` |
| **titan ssd** | titan | as above | Samsung 970 EVO 500 GB on one PCIe lane, an 80 GiB XFS volume at `/xfs` | — |
| **hyperion hdd**, **hyperion ssd** | hyperion | as titan | the same models as titan | as titan |
| **europa hdd** | europa | Ryzen 9 7945HX (Zen4); the executor on cpu 8, its blocking thread on 24 | WDC WD6001FZWX-00A2VA0, 5.5 TiB, 7200 rpm, CMR, XFS at `/hdd`, **write cache off** | The foreground's journal on the Optane at `/optane` |
| **europa ssd** | europa | as above | Intel Optane 900P, 280 GB, XFS at `/optane` | — |
| **europa net** | europa, from titan and hyperion | as above; each server on its host's cpu 2 | europa's disk, then its Optane, as the destination; the survivors read from titan's and hyperion's disks | 1 GbE, plain TCP |

- **The disks' write cache was off for every measured leg**, `hdparm -W0` and a rescan, so the
  kernel read `write through`. The harness refused a disk that read otherwise. The lab script put
  it back on every way out, and every disk was left with it on, as found.
- **Governor `performance`** for every run. It was put back afterwards, `schedutil` on titan and
  hyperion and `powersave` on europa, with the e2scrub and fstrim timers held and started again.
  shoal-tmdb was inactive on all three before and after, and europa has no deployment at all.
- **Locked memory unlimited**, as a deployed node's is, through each run's systemd unit.
- **One `znver1` build** for all three hosts. rustc 1.100.0-nightly (2026-09-04), kernel
  7.0.0-34, the glommio fork at `f4643f7`, `rusty_erasure` 0.4.1 dispatching to `x86_64/avx2` on
  Zen1 and `x86_64/avx2_gfni` on Zen4.
- **The disks were 43 to 50 °C** at every leg's end.
- **Every change to a host** is in `shoal-spike/results/x12-host-changes.txt`.

**Where the run departed from the plan** on [the spikes page](spikes.md#x12-recovery-and-scrub-rates):

- *"Beside a foreground load from X6 or X7's harness"* is X7's scrub foreground, with Q23's
  journal on the SSD and seeded Poisson arrivals.
- *"Read `k` chunks, compute one, write it"* became three measurements. The device's two roles are
  judged apart, as S10 budgets them: `rebuild role=dest` for a destination, and `deep` for a
  source, whose reads are a scrub's. The literal pipeline on one device, `role=local`, is reported.
- *"The arithmetic"* is printed by `device report` for every leg, beside the hours onto one
  destination: how many destinations share a 16 TiB rebuild to finish it in a day.
- *Q17* was in the spike's table and not its method. `granularity` was added for it.
- **The background's latency goal was set at 100 µs after the quick pass.** On the Optane the
  foreground started 0.46 ms late at the p99 under 250 µs, 0.21 ms under 100 µs and 0.12 to
  0.18 ms under 50 µs. 100 µs is X9's, and the goal is a figure of every side.
- **Two supplements were added after the rounds, each under legs of its own, and are labelled
  wherever they are quoted:**
  - the cpu measurement with a summary folded to one 4 KiB block, when P1 fired on a summary a
    unit long (`<host> codec`);
  - the disks' scrub and rebuild at the paces the verdicts turn on, with windows of 90 s in place
    of 20, when a fixed 5 MiB/s scrub straddled 1.25× on two disks of three (`<host> hdd xfs long`).
    A 20 s side holds 200 small writes, so its p99 is its second slowest.
- **The lab script was edited while `net` ran**, to add the supplements. `sh` reads its script as
  it goes, so the run stopped on a parse error, but only after its last step: both servers had
  stopped and every host was put back. X8's page warns against exactly this.

## How to read the tables

Every figure is the median of four rounds, with the lowest and the highest in brackets. A ratio to
the foreground alone is taken round by round, against the `none` side of the same round, before
its interval is taken. Within a line means the whole interval lies at or under it, above means
wholly over it, and at the line means it straddles. A leg is named by host and device: *disk* for
`/hdd`, *970 EVO* and *Optane* for the SSDs. Absolute figures are milliseconds unless they say
otherwise.

## 1. A core's cpu

`codec`, one pinned core, 4 MiB chunks out of cache, four rounds. GiB a second:

| Op | titan (Zen1) | hyperion (Zen1) | europa (Zen4) |
| --- | --- | --- | --- |
| Checksum every unit (CRC-64/NVME) | 11.56 | 11.56 | 50.5 |
| Fold every unit into a unit-long summary | 13.9 | 14.1 | 22.7 |
| The two in one pass | 7.76 | 7.74 | 28.9 |
| Supplement: fold into one 4 KiB block | 14.7 | 14.7 | 48.0 |
| Supplement: checksum and block fold in one pass | 8.00 | 8.01 | 30.9 |
| A copy, in memory | 6.64 | 6.6 | 28.7 |
| Decode a 2+1 chunk, data or parity | 5.14 | 5.1 | 17.4 |
| Decode a 4+2 chunk, data or parity | 3.03 | 3.0 | 9.27 |
| A rebuild's whole cpu for a chunk: copy / 2+1 / 4+2 | 3.20 / 2.23 / 1.30 | 3.2 / 2.2 / 1.3 | 21.6 / 8.78 / 4.58 |
| A 4+2 stripe's summaries encoded and compared, µs: unit-long / block | 27.9 / 1.62 | 28.3 / 1.61 | 4.98 / 0.33 |

- **P1 fires everywhere it was judged.** In the checksum's pass, a unit-long summary added 0.49
  [0.46–0.51] of the checksum's cost on titan and hyperion and 0.71 to 0.74 on europa. A block-long
  one added 0.43 to 0.44 and 0.62. The line was 0.25.
- **The cost is the second pass, not the summary.** Folding into one 4 KiB block, which stays in
  the first cache, barely moved it on Zen1. A Zen1 core checksums at the rate it can read memory,
  and a fold reads every byte again.
- **In absolute terms the fold is small.** At the rates a scrub ran at beside its foreground, its
  checksums and fold took 1% of a Zen1 core on a disk and 6% on the 970 EVO
  ([4](#4-a-deep-scrub-on-an-ssd)). P1's line was relative, and it fired.
- **A rebuild's cpu is not what bounds it on any lab device.** A Zen1 core rebuilds a 4+2 chunk at
  1.3 GiB/s, five times what the 970 EVO took and twenty times a disk. X4 measured the decode alone
  at the same 3.03 GiB/s on titan.
- **The planted faults, checked in memory on every leg: both found every round, none missed, no
  false alarm.**

## 2. A rebuild's destination

`rebuild role=dest`: 4+2 chunks decoded from survivors held in memory and written whole, beside the
foreground. Rebuilt MiB/s, and the foreground's read and write p99 ÷ alone:

| Pace | titan disk | hyperion disk | europa disk |
| --- | --- | --- | --- |
| Alone: read p99, write p99, ms | 67, 81 | 80, 76 | 43, 53 |
| fixed-10 | 9.65; 1.39 [1.35–2.95], 1.78 [0.82–2.27] | 9.88; 1.17, 1.32; under 2× | 9.95; 1.51, 1.43; under 2× |
| fixed-20 | 16.7 (0.84 of it); 1.88, 1.76 | 16.7 (0.83); 1.62, 1.69 | 17.9 (0.90); 2.07, 1.73 |
| fixed-80 | 28.1 (**0.35** of it); 5.23, 5.08 | 29.9 (0.37); 4.09, 4.71 | 28.1 (0.35); 2.48, 1.91 |
| idle | **21.7** [20.3–22.4]; 1.49, 1.38; under 2× | **22.9** [21.6–23.3]; 1.11, 1.18; under 2× | 26.2; 1.54 [1.30–2.03], 1.55 |
| unbounded | 58.3; 7.94, 6.23 | 59.7; 5.26, 6.96 | 62.6; 5.08, 5.43 |

| Pace | titan 970 EVO | hyperion 970 EVO | europa Optane |
| --- | --- | --- | --- |
| Alone: read p99, write p99, ms | 3.5, 19.7 | 3.0, 19.4 | 0.086, 0.162 |
| fixed-100 | 90.4; 2.56, 2.20 | 92.4; 2.57, 2.30 | 99.8; 11.1, 6.99 |
| fixed-400 | 128 (0.32 of it); 3.01, 2.50 | 141 (0.35); 3.54, 2.80 | 400; 14.8, 8.35 |
| idle | 68.8 [38.9–88.4]; 1.85, 2.02 | 89.7 [51.2–108]; 2.13, 2.05 | 1,305; 6.43, 6.88 |
| unbounded | 271; 7.88, 4.75 | 267; 9.22, 5.45 | 1,773; 71.3, 52.2 |

- **R1 fires on every leg.**
  - On the disks the fastest pace inside 2× was idle on titan and hyperion, at 21.7 and
    22.9 MiB/s, and fixed-10 on europa, at 9.95. A 16 TiB disk onto one destination takes 215, 203
    and 468 hours.
  - Unbounded, which no foreground tolerated, still takes 75 to 80 hours, above the 48 hour line.
  - On the SSDs no pace stayed under 2×, so R1 fires there for another reason: not that a rebuild
    is slow, since unbounded rebuilds a 4 TiB 970 EVO in 4.3 hours and the Optane in 0.66.
- **R2 fires on titan's and hyperion's disks.** On hyperion idle rebuilt 2.32 [2.23–2.36] times as
  fast as fixed-10, the only fixed budget inside 2×; on titan no fixed budget stayed inside and
  idle did. On europa's disk idle straddled 2× in the read and does not fire.
- **With windows of 90 s, the supplement, R2 fires on all three disks.** Idle pacing stayed under
  2× on each, at 18.4 MiB/s on titan, 19.1 on hyperion and 25.4 on europa (read p99 1.34, 1.29 and
  1.47×). It rebuilt 1.96 [1.89–2.05] times as fast as fixed-10 on hyperion and 2.57 [2.47–2.63] on
  europa, and on titan fixed-10 did not stay under 2×. R1's hours from those rates are 254, 244
  and 183.
- **A disk with its cache off takes a whole chunk slowly at one piece in flight.** Every write
  waits for its sector, and a 4 MiB chunk is four of them plus its sync, rename and directory sync.
  A fixed budget above 20 MiB/s got a third of what it asked for. Sixteen pieces in flight across
  two rebuilds reached 58 to 63 MiB/s.
- **On the 970 EVO the destination's syncs are what the foreground waits for.** Its own writes sync
  at 200 a second, and every rebuilt chunk adds a data sync and a directory sync. Idle pacing came
  closest, with medians of 1.85 to 2.13.
- **On the Optane the line is a tenth of a millisecond wide.** A rebuild paced by idle time added
  0.47 ms to the read p99 and 0.95 ms to the write p99, which is 6.4 and 6.9 times.

## 3. The whole pipeline on one device

`rebuild role=local`: the survivors read from the device, verified, decoded and written whole into
it, so one device does all of a rebuild's I/O. Rebuilt MiB/s, unbounded, and in brackets the device
MiB/s it moved:

| Layout | titan disk | hyperion disk | europa disk | titan 970 EVO | hyperion 970 EVO | europa Optane |
| --- | --- | --- | --- | --- | --- | --- |
| copy | 32.7 (65) | 33.3 (67) | 36.2 (72) | 222 (445) | 223 (446) | 1,172 (2,344) |
| 2+1 | 25.2 (76) | 25.5 (77) | 25.1 (75) | 176 (528) | 175 (526) | 796 (2,388) |
| 4+2 | 16.3 (82) | 16.9 (85) | 15.9 (80) | 113 (567) | 114 (570) | 489 (2,447) |

- **A decode costs what its reads cost.** Each device moved about the same bytes a second whatever
  the layout, so a 4+2 rebuild ran at the device's rate ÷ (k + 1). This is the one-device view of a
  declustered rebuild, and it is reported, not judged: in a pool a device is the source of some
  rebuilds and the destination of others.
- Paced by idle time, the same pipelines ran at 8 to 17 MiB/s on a disk and 29 to 77 on the
  970 EVO.

## 4. A deep scrub on an SSD

`deep`: every chunk of a 4+2 stripe read whole, every unit verified and folded, the stripe's
summaries checked; a stripe at a time, placement group by placement group. Scrubbed MiB/s, and the
foreground's read and write p99 ÷ alone:

| Pace | titan 970 EVO | hyperion 970 EVO | europa Optane |
| --- | --- | --- | --- |
| Alone: read p99, write p99, ms | 2.27, 9.44 | 2.21, 9.89 | 0.086, 0.162 |
| fixed-50 | 50.1; 1.98 [1.46–2.46], 1.21 | 50.1; 2.04 [1.30–2.20], 1.07 | 50.1; 9.73, 7.18 |
| fixed-400 | 281 (0.70 of it); 2.37, 1.54 | 280 (0.70); 2.48, 1.70 | 399; 18.0, 12.3 |
| idle, pieces of 1 MiB | **388** [78–391]; **1.00** [0.95–1.16], **0.98** [0.90–1.11]; within 1.25× | **385** [101–388]; **0.98** [0.87–1.13], **0.90** [0.89–1.03]; within 1.25× | 2,121; 6.39, 7.60 |
| idle-ceil-200 | 181; 1.06, 0.98; within 1.25× | 180; 0.96, 0.85; within 1.25× | 200; 4.98, 3.08 |
| unbounded | 546; 2.75, 3.00 | 544; 2.85, 2.89 | 2,243; 19.5, 18.3 |
| `cpu`: the same verification over stripes in memory, no reads | 6,168; 1.70 [0.90–3.44], 2.30 | 6,181; 1.77 [0.39–3.25], 2.22 | 22,346; **6.55**, **11.5** |

Idle pacing by the piece (S2), read and write p99 ÷ alone:

| Piece | titan 970 EVO | hyperion 970 EVO | europa Optane |
| --- | --- | --- | --- |
| 256 KiB | 319 MiB/s; 0.96, 0.92; within | 318; 1.06, 0.97; within | 1,876; 6.10, 6.60 |
| 1 MiB | 388; 1.00, 0.98; within | 385; 0.98, 0.90; within | 2,121; 6.39, 7.60 |
| 4 MiB | 446; **2.12** [0.86–2.68], 1.22 | 443; **2.19** [0.90–2.37], 1.18 | 2,214; 19.7, 14.6 |

- **On the 970 EVO a scrub paced by idle time costs the foreground nothing measurable**, at 385 to
  388 MiB/s in pieces of 1 MiB and 318 to 319 in 256 KiB. A 4 TiB device is read in three hours,
  so S1 does not fire there.
- **A fixed budget is worse at a seventh of the rate.** At 50 MiB/s, a 1 MiB read every 20 ms
  issued whatever the foreground was doing, the read p99 doubled.
- **Pieces of 4 MiB are too large even in the gaps.** A foreground read that arrives just after
  one is issued waits behind 4 MiB on one PCIe lane: 2.1 to 2.2 times.
- **On the Optane nothing stays under either line, so S1 and S2 fire there.** The foreground's own
  read p99 is 86 µs, and two things are each several times that:
  - a piece in flight: 50 MiB/s of 1 MiB reads raised the read p99 9.7 times while the executor
    started every foreground operation within 27 µs of its slot;
  - the background's cpu: the `cpu` side, verifying stripes in memory with no reads at all, raised
    it 6.6 times. Its executor started foreground operations 0.23 ms late at the p99, the 100 µs
    goal and a step.

  In milliseconds, idle pacing put the read p99 at 0.55 and the write p99 at 1.24. A line drawn as
  a multiple of the Optane's own tail is a line the size of one piece or one hold.
- **The scrub's cpu is a few percent of a core at an SSD's idle-paced rate.** A Zen1 core verified
  and folded 6,168 MiB/s from memory beside the foreground, sixteen times what the 970 EVO gave a
  scrub paced by idle time.
- **As a rebuild's source**, the fastest pace under 2× read the 970 EVO at 385 to 388 MiB/s. On the
  Optane none did.

## 5. A deep scrub on a disk

`deep` on the disks, write cache off, the foreground's journal on the SSD. Scrubbed MiB/s, and the
foreground's read and write p99 ÷ alone. The rounds' sides were 20 s:

| Pace | titan disk | hyperion disk | europa disk |
| --- | --- | --- | --- |
| Alone: read p99, write p99, ms | 87, 108 | 82, 84 | 57, 60 |
| fixed-5 | 5.00; 1.00, 0.99; within 1.25× | 5.00; 1.13 [1.05–2.30], 1.33 [1.24–2.12] | 5.00; 1.24 [1.01–1.47], 1.21 |
| fixed-10 | 10.0; 1.25 [1.07–1.76], 1.07 | 10.0; 1.21, 1.19 | 10.0; 1.16, 1.17; within 1.25× |
| fixed-20 | 19.4; 1.44, 1.27 | 19.5; 1.35, 1.37 | 19.8; 1.36, 1.35 |
| fixed-40 | 32.4 (0.81 of it); 1.86, 1.46 | 32.7 (0.82); 1.91, 1.82 | 34.5 (0.86); 2.01, 2.18 |
| idle | **46.4**; 1.24 [1.11–1.31], 1.03 | **47.3**; 1.34 [0.91–1.39], 1.31 | **57.8**; 1.13 [1.03–1.39], 1.13 |
| idle-ceil-20 | 17.7; 1.12, 1.09 | 17.6; 1.00, 1.16 | 18.8; 1.09, 1.08; within 1.25× |
| unbounded | 82.9; 6.21, 5.60 | 83.4; 7.95, 7.70 | 76.9; 4.54, 4.62 |
| idle, pieces of 256 KiB / 4 MiB | 42.7; 1.32, 1.13 / 53.3; 1.27, 1.15 | 43.7; 1.20, 1.48 / 54.0; 1.44, 1.47 | 57.3; 1.42, 1.38 / 72.0; 1.59, 1.64 |

**The supplement, with windows of 90 s**, so each side's p99 comes from 900 writes and not 200,
four rounds on each disk under a leg of its own:

| Pace | titan disk | hyperion disk | europa disk |
| --- | --- | --- | --- |
| Alone: read p99, write p99, ms | 100, 115 | 91, 98 | 54, 61 |
| fixed-5 | 5.00; 1.08, 1.04; **within 1.25×** | 5.00; 1.12 [0.98–1.30], 1.20 [1.05–1.32] | 5.00; 1.15 [1.12–1.26], 1.17 |
| fixed-10 | 10.0; 1.20 [1.10–1.41], 1.16 | 10.0; 1.25 [1.16–1.29], 1.21 | 10.0; 1.23, **1.28** [1.26–1.45] |
| idle | **45.2**; 1.15 [1.00–1.35], 1.10 | **44.5**; 1.34 [1.16–1.42], 1.19 | **59.0**; 1.23 [1.19–1.36], 1.29 |
| idle-ceil-20 | 16.8; 1.13, 1.12 | 16.8; 1.34, 1.25 | 18.9; 1.14, 1.07 |

- **S1 fires on every disk, in the rounds and in the supplement.** No pace above 5 MiB/s kept the
  foreground wholly within 1.25×.
  - At the fastest pace that did, titan's fixed-5 in both, a deep scrub of 16 TiB takes 39 days.
  - On europa's disk idle-ceil-20 stayed within in the rounds, at 18.8 MiB/s, which is 10 days;
    with windows of 90 s it did not.
  - On hyperion's disk nothing stayed within.
- **On a disk the foreground pays about the same for any scrub at all.** A scrub paced by idle time
  read 44 to 59 MiB/s at medians of 1.10 to 1.34× its foreground's p99. A fixed 5 or 10 MiB/s,
  a tenth of the rate, cost 1.04 to 1.28×. With a ceiling of 20 MiB/s, idle pacing cost what it
  did with none. A fixed budget's cost grew with its rate: 1.27 to 1.44 at 20 MiB/s and 1.46 to
  2.18 at 40.
- **So a disk's scrub is paced by idle time and run as fast as that allows**: 16 TiB in 3.3 to 4.4
  days. Its interval is how long a pass takes at its device's foreground, since no ceiling buys the
  foreground anything back.
- **S2 does not fire on a disk**: no piece size lay wholly above 1.25×. Pieces of 1 MiB were the
  best of the three in the rounds, 256 KiB and 4 MiB both worse, so a disk scrubs in 1 MiB pieces.
- **The planted faults**: wherever a walk reached them both were found, every round of the
  supplement, and nothing else was reported.
- **H4 again**, X7's trigger at a fixed 10 MiB/s, now with the journal on the SSD and the cache
  off:
  - the write's p99 lay within 1.25× on titan and hyperion in the supplement (1.16, 1.21);
  - it lay above it on europa's WD Black, 1.28 [1.26–1.45], as X7 found with the journal on the
    disk;
  - in the rounds' 20 s sides it straddled the line on titan and hyperion.

## 6. One missed unit (Q17)

`granularity`: one missed unit of a stripe rebuilt as its whole chunk (the survivors read whole,
verified, decoded, the chunk written whole) and as the unit alone (the unit read from each
survivor, verified, decoded, and written in place with its header, then synced). One at a time,
with nothing beside it. Each figure is the median ms of one rebuild:

| | titan disk | hyperion disk | europa disk | titan 970 EVO | hyperion 970 EVO | europa Optane |
| --- | --- | --- | --- | --- | --- | --- |
| 2+1: chunk / unit / ÷ | 125 / 45.7 / **2.75** | 125 / 46.5 / **2.69** | 109 / 33.5 / **3.32** | 20.3 / 1.68 / **12.1** | 20.3 / 1.68 / **12.0** | 5.40 / 0.176 / **30.7** |
| 4+2: chunk / unit / ÷ | 179 / 49.7 / **3.61** | 179 / 49.0 / **3.66** | 162 / 44.2 / **3.68** | 31.5 / 1.88 / **16.7** | 31.5 / 1.91 / **16.5** | 8.94 / 0.235 / **37.9** |

- **A record of unit ranges pays where a chunk's rebuild is much dearer than a unit's**, which is
  an SSD's case. A disk pays a seek and a revolution either way, so whole chunks are only 3 to 4
  times dearer.
- Every rebuild was checked against the chunk it replaced, and none was wrong.
- The device's bytes were what the granularity predicts: 4,100 to 4,200 KiB written for a chunk and
  68 KiB for a unit, its header with it.

## 7. A rebuild across hosts

`rebuild-net` on europa: every survivor of a stripe fetched at once over 1 GbE from `device serve`
on titan and hyperion, verified as received, decoded, and written whole into europa's disk or its
Optane, two rebuilds in flight, beside the foreground. Rebuilt MiB/s, its share of the link's bound
of 112 MiB/s ÷ k, and the foreground's read p99 ÷ alone:

| Layout | Into the Optane | Into the disk |
| --- | --- | --- |
| copy (bound 112) | 85.1, 0.76; 16× | 44.9, 0.40; 2.6× |
| 2+1 (bound 56) | 53.0 [51.8–53.0], **0.95**; 13× | 37.2, 0.66; 2.1× |
| 4+2 (bound 28) | 26.8 [26.4–27.2], **0.96**; 6.1× | 21.8, 0.78; 2.3× |

- **A decode across the lab reaches its link**, 105 MiB/s received into the Optane for 2+1 and 4+2
  alike. A copy fetched one chunk a rebuild, two at a time, and did not fill the link.
- **Into a disk, the disk is the bound**, as section 2 found.
- Every chunk rebuilt across hosts matched the one it replaced, and no source refused one.
- The foreground beside it paid what a destination pays: section 2's figures, now at the link's
  rate.

## 8. The arithmetic

From each leg's own rates, at their medians. Hours onto one destination, and how many destinations
a 16 TiB rebuild would be spread over to finish in a day:

| Rate | MiB/s | 1 TiB | 4 TiB | 16 TiB | Destinations for 16 TiB in a day |
| --- | --- | --- | --- | --- | --- |
| A disk's rebuild, idle (titan, hyperion) | 21.7 to 22.9 | 13 h | 52 h | 203 to 215 h | 9 |
| A disk's rebuild, unbounded | 58 to 63 | 4.7 h | 19 h | 75 to 80 h | 4 |
| A 970 EVO's rebuild, idle | 69 to 90 | 3.2 to 4.2 h | 13 to 17 h | — | — |
| A 970 EVO's rebuild, unbounded | 267 to 271 | 1.1 h | 4.3 h | — | — |
| The Optane's rebuild, idle | 1,305 | 0.22 h | 0.89 h | — | — |
| Across 1 GbE: copy, 2+1, 4+2, the bound | 112, 56, 28 | 2.6, 5.2, 10.4 h | 10, 21, 42 h | 42, 83, 166 h | 2, 4, 7 |
| A disk's deep scrub, idle | 46 to 58 | 5 to 6.3 h | 20 to 25 h | 3.4 to 4.2 days | — |
| A 970 EVO's deep scrub, idle | 385 to 388 | 0.75 h | 3.0 h | — | — |

- **A large disk is exposed for days unless its rebuild is spread.** At the pace its foreground
  tolerates, a 16 TiB disk's rebuild needs nine destinations to finish in a day, and a host's
  1 GbE caps it near 28 MiB/s for 4+2 however many there are. On the lab's three hosts a 2+1 pool's
  rebuild has one destination.
- **A device of a few TiB on an SSD rebuilds in hours** at any pace the device allows. What it
  costs is the foreground's tail, not exposure.

## What would have changed the design

| Result named in advance | Found | So |
| --- | --- | --- |
| **R1.** A rebuild leaves a pool exposed for days: 16 TiB onto one destination above 48 hours at the fastest pace inside 2×, or 4 TiB above 12 | **Yes, on every leg.** On the disks 203 to 468 hours, and 75 to 80 even unbounded. On the SSDs no pace stayed under 2× at all | **A rotational pool's default layout survives a second loss during its rebuild**, never 2+1 or two copies, and its rebuild is spread over the pool's devices. An SSD pool's exposure is hours; its objective is the foreground's, and is M16's to state |
| **R2.** Idle pacing rebuilds more than 1.5× as fast as the best fixed budget inside 2× | **Yes on titan's and hyperion's disks** in the rounds: 2.32× on hyperion, and on titan no fixed budget stayed inside 2×. **On all three disks** in the supplement, 1.96 to 2.57×. Not on the SSDs, where idle did not stay inside 2× | **A device's rebuild is paced by its idle time, with a ceiling**, not held to a fixed rate |
| **S1.** A deep scrub inside 1.25× cannot finish 16 TiB in 7 days, or 4 TiB in one | **Yes, on every disk and the Optane.** On the disks no pace above 5 MiB/s stayed wholly within 1.25×, with windows of 20 s or 90 s, so the fastest inside it reads 16 TiB in 39 days. On the 970 EVO idle pacing stayed within at 385 to 388 MiB/s, 4 TiB in three hours. On the Optane nothing stayed within | **A deep scrub's interval follows its device's size**: on a disk the scrub takes the idle time at whatever rate that gives, 3.3 to 4.4 days for 16 TiB beside the lab's foreground, and its interval is how long a pass takes, not a constant. S15's 1.25× for a scrub holds on a disk at the median and not wholly; M17 states the disk's objective with these figures |
| **S2.** Idle pacing with no ceiling above 1.25× at every piece size | **No, on the disks and the 970 EVO**: on the 970 EVO idle pacing stayed within at 256 KiB and 1 MiB; on the disks no piece size lay wholly above. **Yes on the Optane**, at every piece, where no ceiling measured helped either: 200 MiB/s cost 5.0× | **A scrub reads in pieces of 1 MiB**, never 4 MiB, and needs no ceiling for its foreground's sake on a disk or the 970 EVO: on a disk a ceiling of 20 MiB/s cost what none did. The Optane is the SSD question M16 and M17 inherit |
| **P1.** The parity check's fold adds more than a quarter of the checksum's cost | **Yes, everywhere**: 0.49 on Zen1 and 0.71 on Zen4 for a unit-long summary; 0.43 and 0.62 folded to a block, in the supplement | **The parity check runs on a sample of deep scrubs.** Its sample is M17's to set |

## The comparison

**How a background is paced**, on a disk, a scrub's pieces of 1 MiB:

| | A fixed byte budget | The slice's idle time, with a ceiling (recommended) |
| --- | --- | --- |
| When a piece is issued | On a schedule, whatever the foreground is doing | Only while the slice has no foreground operation in flight |
| What a foreground operation waits behind | Whatever pieces are in flight when it arrives | At most the one piece issued in a gap it then arrived in |
| A disk's scrub (titan, hyperion) | 10 MiB/s at 1.07 to 1.25× its write and read p99; 20 MiB/s at 1.27 to 1.44× | 46 to 47 MiB/s at 1.03 to 1.34× |
| The 970 EVO's scrub | 50 MiB/s at 1.98 to 2.04× its read p99 | 385 MiB/s at 0.98 to 1.0× |
| A disk's rebuild into it (hyperion) | 9.9 MiB/s inside 2×; 16.7 at 1.62 to 1.69× | 22.9 MiB/s at 1.11 to 1.18× |
| What it guarantees | A device's background share, which a ceiling keeps | That the background takes the gaps, and nothing else unless a ceiling allows |

**Where the parity check runs**, a 4+2 stripe on Zen1:

| | Every deep scrub | A sample of deep scrubs (recommended) |
| --- | --- | --- |
| Its cost | A second pass over every unit, 0.49 of the checksum's cpu | The same pass, on the sampled scrubs only |
| What it finds | A parity computed wrongly, on the pass after it is written | The same, on the sampled pass |
| In absolute terms | The fold alone, about 50 µs of a Zen1 core a MiB: 0.2% of a core at a disk's scrub rate, 2% at the 970 EVO's | That share of the sample |

## Recommendation

**A device's background takes its idle time, under a ceiling.**
[S18](contract.md#q28-and-q29-and-q17-in-part-recovery-and-scrub-rates-2026-10-09) records the
answer to Q28 and Q29, and Q17 in part.

1. **A rebuild's and a scrub's pieces are issued only while the slice has no foreground operation
   in flight, one at a time, under the device's byte ceiling.**
   - The slice's executor knows when it is idle, since every foreground operation is its own.
   - The budget S10 and S11 give each device is that ceiling, shared, with recovery before scrub as
     S13 orders them. It is not a rate the background is held to.
   - The figures: R2 fired on every disk in the supplement, idle pacing 1.96 to 2.57 times the
     best fixed budget inside 2×. On the 970 EVO a scrub paced so read seven times what a fixed
     50 MiB/s did, and left the read p99 at 1.0× where the fixed budget doubled it.
   - The acceptance tests are `rebuild_waits_for_the_slices_idle_time` (S10) and
     `scrub_waits_for_the_slices_idle_time` (S11).
2. **The background's cpu runs in a task queue below the foreground's**, at a latency goal of
   100 µs, in steps of a 64 KiB unit, as X9 found a shared executor needs. A Zen1 core rebuilds a
   4+2 chunk, verification included, at 1.3 GiB/s, so the cpu bounds no lab device. On a device as
   fast as the Optane the hold is still the foreground's tail.
3. **A rotational pool survives a second loss while it rebuilds.**
   - Its redundancy is at least two parity chunks or three copies, and the inventory wizard
     proposes 4+2.
   - 2+1 or two copies is refused unless accepted by name (S4's
     `rotational_pool_of_one_loss_is_refused`).
   - Its rebuild is spread over the pool's devices. At the pace a disk's foreground tolerates, 18 to
     25 MiB/s, a 16 TiB disk takes 8 to 11 days onto one destination and needs eight to eleven to
     share it for a day; 1 GbE caps a 4+2 rebuild at 28 MiB/s a destination host.
4. **A deep scrub's interval is how long a pass takes beside its device's foreground**, not a
   constant.
   - On a disk the scrub runs at whatever idle pacing gives, 44 to 59 MiB/s here, so 16 TiB in 3.3
     to 4.4 days, and a weekly cadence holds for these disks at these foregrounds.
   - On the 970 EVO a pass of 4 TiB takes three hours.
   - A scrub reads in pieces of 1 MiB.
   - A disk's foreground pays 1.1 to 1.3× its p99 while a scrub runs, at any pace above
     5 MiB/s, so M17 states a disk's objective from these figures rather than the 1.25× S15 drew
     before them.
5. **The parity check by summaries runs on a sample of deep scrubs**, a second pass over every
   unit that P1 found costs half the checksum on Zen1. Its sample is M17's.
6. **Q17, in part: a missed write's record keeps unit ranges up to the device's break-even**, about
   3 units of a chunk on a disk and 12 to 38 on an SSD, past which the chunk is rebuilt whole.

And beside those:

- **An SSD's rebuild objective is M16's to state.** No pace that rebuilt anything kept the
  970 EVO's foreground under 2×, and nothing kept the Optane's 86 µs read p99 under either line.
  What an SSD pays is a millisecond or two of tail for the hours a rebuild takes, and whether that
  is stated as an added latency, or bought back with a background executor, was not measured.
- **Moves** are a rebuild's shape with no decode, and take the same pacing.

## What X12 does not settle

- **An SSD's objective for a rebuild.** On the 970 EVO no pace that rebuilt anything kept its
  foreground under 2×, and on the Optane no pace of either kind kept a foreground of 86 µs under
  either line. Whether such a device is judged by an added latency in milliseconds, or its
  background's cpu runs on an executor apart from its slice, is M16's. Neither was measured.
- **The sample of the parity check**, and how a deep scrub's cadence is staggered: M17's, with
  these figures.
- **Q17's bound and snapshot.** X12 gives the record's granularity a break-even; the bound, the
  survival across a checkpoint and the reach to a new replica are M16's.
- **Moves.** A move reads and writes whole chunks as a rebuild does, and Q29 asks for its budget
  too. It is a rebuild's shape with no decode, and no side ran it.
- **A rebuild spread over many destinations.** The arithmetic assumes rates add across devices
  while the network allows. The lab has three hosts and one disk each, so nothing measured that.
- **The europa executor's lateness beside its disk**, 0.6 to 0.9 ms at the p99 with no
  background: seen and not traced.

## What it did not measure

- **A rebuild through the protocol**: no group, no commit of a label, no missed record, no driver.
  M16's event arms are where those are measured.
- **A light scrub.** X6 and X7 priced its walk.
- **Wider layouts across hosts**, or more than one disk a host.
- **kTLS on the rebuild's connections.** At 1 GbE the link is the bound (X11).
- **Disks with their write cache on**, which Q23 refuses, and shingled or SAS disks.
- **A device nearly full**, or one whose population is older than the run.
- **Moves and reclamation**, which read and write as a rebuild does.

## What it found

None of these is a defect of Shoal, so none is filed on
[Known Issues](../appendix/known-issues.md). Each is written down because M16, M17 or M19 meets
it, and the one that is an optimization not taken is on [Optimizations](../appendix/optimizations.md).

| Finding | Where |
| --- | --- |
| A whole-chunk write at one piece in flight reaches about 30 MiB/s on a disk with its cache off; sixteen in flight reach 60 | `rebuild`, section 2 |
| A Zen1 core checksums CRC-64/NVME at the rate it reads memory, so any second pass over a unit costs about half as much again: filed as [O96](../appendix/optimizations.md#o96-a-deep-scrub-reads-every-unit-twice-once-for-its-checksum-and-once-for-its-summary), a fold inside the checksum's own loop | `codec`, section 1 |
| europa's executor starts a foreground operation 0.6 to 0.9 ms late at the p99 beside its disk with nothing else running, where titan's and hyperion's start 0.06 to 0.09 ms late | `deep` and `rebuild`, every `none` side on europa's disk |
| titan's and hyperion's 970 EVO acknowledge a 4 KiB stage at 14.7 ms at the p99, carrying only the disk leg's journal | The disk legs' `stage_p99` |

## Related

- [X12](spikes.md#x12-recovery-and-scrub-rates) for what was planned.
- [X7's record](device-store-hdd.md) for the harness on a disk and the scrub figures this started
  from, and [X6's](device-store-ssd.md) for the harness itself.
- [S10](recovery.md#budgets) and [S11](scrub.md#schedule-and-budget) for the design these figures
  revise.
- [S18](contract.md#q28-and-q29-and-q17-in-part-recovery-and-scrub-rates-2026-10-09) for the
  decision.
- [X4](erasure-coding-crates.md) for the code and [X5](checksums.md) for the checksum.
- [X9](table-latency.md) for the latency goal a shared executor needs.
- [X11](streamed-bodies.md) for what 1 GbE carries.
- `shoal-spike/src/device/` for the harness and `shoal-spike/results/x12-lab.sh` for the run.
- `shoal-spike/results/x12/` for every round's records, and `x12-report.md` for the merged report.
