# X13. The benchmark's shape, measured

**Reported 2026-10-05.** This is the record of spike [X13](spikes.md#x13-the-benchmarks-shape),
what [F69](../features/driver-operation-kinds.md) left of Q30. It wrote the object dataset's two
shapes down as the types a driver would have, and measured:

- how fast one core makes seeded bytes, by five generators, and takes their CRC-64/NVME, on its
  own and on several cores at once;
- what one client core puts and gets through X11's server when the server discards the bytes or
  answers from memory, plaintext and under the product's kTLS;
- what it costs to check a read by making its bytes again;
- how fast one core reads a folder of real files, cold and hot, and digests it.

It ran over loopback on titan, hyperion and europa, from the `znver1` build every node runs and,
on europa, the native build the bench's admin program is there, and across 1 GbE from europa to
titan, four rounds each. It ends in a recommendation, which
[S18](contract.md#q30-the-object-dataset-and-seeded-bytes-2026-10-05) records and with it closes
Q30: **a stream makes its own bytes, inline, on the task that sends them; a description is
integers alone, and its bytes are SplitMix64 in counter mode; read-back makes the bytes again and
compares.**

No trigger fired. Four facts decide it:

- **One core makes bytes faster than a device takes them.** A Zen1 core put 1,837 to 1,860 MiB/s of
  made and checksummed 1 MiB frames, two and a half times the 970 EVO's 722, and europa's core
  6,892 MiB/s against the Optane's 2,501. That is T1. Four europa streams put 27,159 MiB/s, since
  each frame is made into a buffer that stays in cache.
- **Checking a read by making it again costs less than the read.** A Zen1 core got and regenerated
  3,304 MiB/s against the 970 EVO's 857, and europa 6,127 against the Optane's 2,553. That is T2.
- **The fastest generator with a published definition is also the plainest.** SplitMix64 in counter
  mode filled 1 MiB out of cache at 4.9 GiB/s on Zen1 and 23.3 on Zen4, the fastest of the four on
  every leg in every round and seven to nine times the devices: T3 holds. AES-128-CTR, expected to
  win, wins only into a hot buffer on Zen1, by 2 to 3% on a put.
- **Under kTLS the send, not the making, is three quarters of a Zen1 put's cpu**, and a kTLS put on
  titan ran under the 970 EVO's rate, near one device and swinging by half between rounds, as X11
  found.

**What was found on the way**: hashing a folder's files with SHA-256 on the reading thread costs a
cold scan a third on Zen1 and half on europa, small files read one at a time come at under half
either device, and europa's cores share about 19 GiB/s of memory for making bytes into a cold arena.
Planning X13 also found a defect in X11's harness, filed and fixed as
[item 213](../appendix/resolved/x11-setup-fifo.md) before X13 ran. What X13 did not settle is under
[What X13 does not settle](#what-x13-does-not-settle).

## The question

[Q30](contract.md#questions-to-answer) asks how the driver gains object operations, byte metrics
and an object dataset. [F69](../features/driver-operation-kinds.md) answered the first two by
generalizing the one driver, and S18 recorded that half
([Q30, in part](contract.md#q30-in-part-the-drivers-shape-2026-10-03)). What was left is X13's:

- **the object dataset's two shapes**: a folder of real files, a workload somebody has, and a
  description - how many objects, a distribution of sizes, a seed - a workload nobody has yet,
  repeatable to the byte ([S15](performance.md#what-the-driver-gains)); each written down against
  what a capture would have to say of it;
- **how fast one core makes seeded bytes** and checksums them, which decides whether a driver
  that makes an object's bytes is measuring the cluster or itself.

The spike's section named the result that would change the design in advance: one core cannot
make seeded bytes as fast as a pool takes them, so a driver needs several and the capture has to
prove it had them.

## How it was judged

The lines below were set before the harness existed and agreed with the user on 2026-10-05.
`shoal-spike driver report` judges them as written. Every figure is an interval over four rounds:
the lowest and the highest round, with the median between, and a trigger fires only where every
round is past its line, [the lab's rule](../performance/benchmarking.md#before-and-after-on-the-lab).
Each is judged over loopback, plaintext, on the build the driver would run on that host: native on
europa, where the bench's admin program is built for the host, and `znver1` on titan and hyperion.
The device rates are X6's, as X11 judged against them.

| Trigger | Fires when | If it fires |
| --- | --- | --- |
| **T1. One core cannot make bytes as fast as a device takes them** | One client core's put - the fastest published generator's fill and the CRC-64/NVME of every 1 MiB unit, in 1 MiB frames, to a server that discards - moves fewer MiB/s than the host's device writes: 2,501 on europa's Optane, 722 on the 970 EVOs | A stream's bytes are made on more than one core, and the capture records each driver thread's busy share |
| **T2. Regenerating a read to check it costs more than the read** | One core receiving a get, each frame checked by making it again with that generator and comparing, moves fewer MiB/s than the host's device reads: 2,553 and 857 | Read-back checks each unit against the checksum the driver kept when it wrote it (S16's ledger), and makes the bytes again only to say where a mismatch is |
| **T3. No generator with a published definition is fast enough** | The fastest of SplitMix64, xoshiro256++, ChaCha8 and AES-128-CTR fills 1 MiB units out of cache below the device's write rate | A description's bytes come from a stamped pattern, and its content repeats |

kTLS is reported beside T1 and T2 and not judged: X11 had already found one kTLS stream near or
below a device ([X11](streamed-bodies.md#1-one-connection-rate-and-cpu-by-frame)), and judging it
again would find X11's limit, not X13's.

**Statements made in advance**, reported and not judged:

- the fastest published definition is AES-128-CTR, from AES-NI;
- a Zen1 core makes and checksums 3 to 5 GiB/s;
- under kTLS the send, not the making, is 70% or more of a put's client cpu;
- a folder read cold runs at its device's rate, and SHA-256, F66's digest of a file, is what
  limits a folder's scan.

## What was run

### The harness

`shoal-spike driver`, a subcommand of the spike binary X2, X6 and X11 live in
(`shoal-spike/src/driver/`). Like every spike's code it is thrown away. It is the one spike here
that adds crates to the workspace's lockfile: `crc-fast` 1.10.0, pinned with only `std` as M13 will
pin it, and `spin` 0.10.1, which `crc-fast` needs on x86-64 beside the `spin` 0.9.8 already
there. Its generators reach `rand_chacha` and `aws-lc-rs`, and its folder `sha2`, all three
already in the lockfile.

**Five generators**, each seekable: it makes the bytes at any offset of any object without making
the ones before them, which checking a ranged read and a write in place both need.

| Generator | What it is | Definition |
| --- | --- | --- |
| `stamped` | One seeded buffer of 64 MiB copied, each 4 KiB block's first sixteen bytes stamped with the object and the block's offset | X13's own; everything but the stamps repeats from object to object. The floor |
| `splitmix` | SplitMix64 in counter mode: word `i` of object `o` is `mix(seed ^ o·γ + (i + 1)·γ)` | Vigna's SplitMix64, started where `shoal-loadgen`'s `Seeded::at(seed, o)` starts |
| `xoshiro` | xoshiro256++ in four lanes, each 4 KiB block seeded anew from the object's SplitMix64 words | Vigna's xoshiro256++ for each lane; the lanes and their seeding are X13's |
| `chacha8` | ChaCha with eight rounds through `rand_chacha` 0.3.1: the object is the stream, the offset the position | Bernstein's ChaCha8, with `rand_chacha`'s 64-bit counter and 64-bit stream |
| `aes-ctr` | AES-128 in counter mode through `aws-lc-rs`: the counter block is the object, then the offset's block of 16 bytes, both big endian; the keystream is the encryption of zeros | FIPS 197 and SP 800-38A |

**Correctness before speed**, every round, on every host and build, in the `check` section and as
unit tests:

- every published generator meets its published reference, built from the key or state the
  reference names and made as the driver makes it: SplitMix64 at seed 1234567 from Vigna's
  `splitmix64.c`, xoshiro256++ from the state 1, 2, 3, 4, ChaCha8 from
  draft-strombergson-chacha-test-vectors' first case, and four blocks of SP 800-38A F.5.1, so
  the counter's step is checked as well as the cipher;
- every generator seeks: a fill at any offset is that slice of one fill from zero, near offsets
  inside words, blocks and counters, and far ones past AES's 2^32 blocks, where its counter carries
  out of the low word, split at every kind of boundary;
- every generator's CRC-64/NVME of three fixed cells is recorded, so the report can say whether
  every host and build made the same bytes.

**The server** is X11's, glommio executors one to a core, in the same process over loopback or on
titan across the network. A put's data frames are read into a buffer and dropped, which is the
wire alone; a get's ranges are answered from a pattern in memory. X13 added one thing to it: a
pattern for each generator, object zero's first 64 MiB, which a connection's setup names by number,
so a get's bytes can be checked by making them again. Unset, the server is X11's byte for byte.

**The client** is X13's own: tokio on a thread pinned to one core, as a driver's worker runs, with
X11's frames and the product's kTLS through `shoal::client::tls::connect`.

- **A put** makes each 1 MiB frame's payload as it goes and writes it, until it is told to stop.
  The payload is a slice of the server's pattern, which makes nothing and is X11's baseline; or a
  generator's fill; or a fill and the unit's CRC-64/NVME, which is what a driver does before a unit
  goes on the wire ([S12](wire-and-client.md#ranged-frames)).
- **A get** keeps four ranges outstanding and checks each answer: not at all; by its CRC against a
  table of the pattern's units made at setup, which is what S16's ledger would hold; or by making
  the bytes again and comparing.
- **Counting.** Each stream's thread records its id, and its cpu is read from the kernel's
  schedstat for that thread, so a loop that rarely yields is counted as the kernel counts it. Each
  client core's busy share is read from `/proc/stat`, every executor's cpu from X11's stat frame,
  and the host's busy time beside them. *MiB/s at a busy core* is the rate divided by the client
  thread's busy share: what one fully busy driver core would move at the cost it paid a byte,
  which still reads when the server or kTLS, and not the client, set the rate.

**The sections.** Every stream cell warms up for 3 s and is measured for 5 s; every section 1 side
is timed three times for at least 300 ms, its sides interleaved, X5's way.

| Section | Cells | Sides |
| --- | --- | --- |
| **check** | each generator; the CRC; four example descriptions | the reference, the seek, the digests; the CRC's check value; a million objects described, expanded to paths and sizes, and one object's bytes read through the body source a driver would read |
| **1. Making bytes** | fill, fill with CRC, and verify, at 4 KiB, 64 KiB and 1 MiB, *cold* (units taken in turn from a 256 MiB arena) and *hot* (one unit over and over, which is a driver's frame buffer) | the five generators; a CRC alone and a copy beside them |
| **3a. Several cores, no wire** | fill with CRC, 1 MiB cold, on 1, 2 and 4 pinned cores at once, each with an arena of its own | the five generators |
| **2. Against a server that discards** | a put and a get of 1 MiB frames, plaintext and kTLS, one client core | put: the baseline, each generator's fill, and its fill with CRC; get: unchecked, by the ledger's CRC, and made again by each generator |
| **3b. The put from several client cores** | the put with CRC, plaintext, from 1 and 2 client cores, and 4 on europa, each its own stream to an executor of its own | the four published generators |
| **4. A folder of real files** | files of 64 KiB, 1 MiB and 64 MiB, 2 GiB of each, read in 1 MiB reads, cold (dropped from the page cache with `posix_fadvise`, the device's own counters read beside it) and hot | read alone; with each read's CRC; through SHA-256 a file at a time, as F66 digests a table's file |
| **5. Across the network** | section 2's cells, europa's client driving titan's server across 1 GbE | the same |

**Rounds.** Four, with every cell's sides in the opposite order in even rounds.
`results/x13-lab.sh` runs them and writes the facts every table is labelled with;
`shoal-spike driver report` merges the rounds and judges the triggers, and its output is
`shoal-spike/results/x13-report.md`. The script ran in two phases, titan and hyperion and then
europa and the network, and each phase's facts are `results/x13-facts-zen1.txt` and
`results/x13-facts-europa.txt`.

### Where, and on what

| Host | CPU | Build | Folder on | Server's executors | Client |
| --- | --- | --- | --- | --- | --- |
| titan | Ryzen Embedded V1756B (Zen1), 4 cores, 8 threads | `znver1` | the 970 EVO's XFS volume, `/xfs/x13` | cpus 2 and 4 | cpu 6, and 1 for a second stream; section 1 and the folder on 6 |
| hyperion | The same | The same | The same, another unit | The same | The same |
| europa | Ryzen 9 7945HX (Zen4), 16 cores, 32 threads | native (AVX-512, VAES) and `znver1` | the Optane 900P under XFS, `/optane/x13` | cpus 8, 9, 12 and 13 | cpus 10, 11, 14 and 15; section 1 and the folder on 10 |

- **Four legs over loopback** and one across the network: titan and hyperion at once, every
  section from the `znver1` build; europa alone, every section from its native build and the
  checks, section 1 and section 2 from the `znver1` one, the two builds' order alternating by round;
  then europa's native client driving titan's server across 1 GbE, section 2 only.
- **The triggers were judged on the build the driver would run**: native on europa, `znver1` on
  the Zen1 hosts. europa's `znver1` leg is reported beside them.
- **Governor `performance`** on every host for every run, and titan's and hyperion's `e2scrub_all`
  timer held; both put back afterwards. No shoal unit ran on any host.
- **One tree**, rustc 1.100.0-nightly (2026-09-04), kernel 7.0.0 (`-31` on europa, `-34` on titan
  and hyperion), the glommio fork at `f4643f7`. `crc-fast` dispatched to its VPCLMULQDQ kernel on
  europa from both builds and to PCLMULQDQ on the Zen1 hosts, as X5 found.
- **Every check passed in every round on every leg**: every published generator met its
  reference, every generator seeked, every digest was the same on all four legs, and no get ever
  read a byte it did not expect.
- europa is also the development host. Nothing was built on it while its rounds ran; the test
  suite ran while titan and hyperion did.

**Where the run departed from [the plan](spikes.md#x13-the-benchmarks-shape).**

- The plan named "a generator of seeded bytes"; five were measured, four with published
  definitions and a copy beside them, so the choice of definition could be made on the record.
- The plan named "a server that discards"; it is X11's, reused, with a pattern a generator makes
  for a get to be checked against.
- Three sections were added with the user before the run: several cores (3a with no wire, 3b with
  it), the folder, and the network leg.
- After the rounds the report gained a column saying how many rounds and legs each check covered,
  and the statement about kTLS was computed as it was stated, the send's share of the client's cpu,
  not kTLS's own. Neither moves a record.

## How to read the tables

Every figure is the median of four rounds, with the lowest and the highest round in brackets.
GiB/s and MiB/s are of payload. *cpu ms/GiB* is milliseconds of the client stream thread's cpu for
each GiB it moved, as the kernel's schedstat counts it; on loopback that includes the receiving
side's softirq work the kernel charges to the sender. *At a busy core* is the rate divided by that
thread's busy share.

## 1. Making bytes on one core

GiB/s for one pinned core. *Fill* makes a unit; *fill+crc* makes it and takes its CRC-64/NVME;
*verify* makes it again and compares it with bytes already made. *Cold* takes 1 MiB units in turn
from a 256 MiB arena; *hot* makes one unit over and over, which is a driver's frame buffer.

| 1 MiB | titan, cold / hot | europa native, cold / hot | europa `znver1`, cold / hot |
| --- | --- | --- | --- |
| **splitmix**, fill | **4.90** / 5.00 | **23.3** / **59.7** | **20.0** / **22.9** |
| splitmix, fill+crc | 3.54 / 3.63 | 17.7 / 33.6 | 15.1 / 17.6 |
| splitmix, verify | 3.68 / 4.33 | 23.3 / 31.0 | 14.2 / 18.0 |
| aes-ctr, fill | 4.27 / **6.71** | 8.54 / 11.6 | 8.53 / 11.6 |
| aes-ctr, fill+crc | 3.21 / 4.47 | 7.71 / 10.1 | 7.70 / 9.99 |
| xoshiro, fill | 3.45 / 4.98 | 15.5 / 23.4 | 8.70 / 10.4 |
| chacha8, fill | 2.79 / 2.78 | 6.91 / 7.07 | 6.44 / 6.52 |
| stamped, fill | 6.44 / 27.1 | 12.6 / 55.0 | 12.1 / 55.0 |
| a CRC alone | 11.5 / 13.2 | 40.5 / 76.1 | 40.3 / 76.3 |
| a copy alone | 6.72 / 29.7 | 10.2 / 62.5 | 10.1 / 62.6 |

hyperion repeated titan within 1% in every cell.

- **SplitMix64 is the fastest published generator out of cache, on every leg in every round**: 4.90
  GiB/s on Zen1, 23.3 on Zen4 from the native build and 20.0 from `znver1`. That is T3, which
  holds by seven times on Zen1 and nine on europa, and it is not the statement made in advance,
  which expected AES-128-CTR.
- **AES-128-CTR wins only into a hot buffer on Zen1**, 6.71 against 5.00 GiB/s, where AES-NI runs
  from cache; out of cache its two passes, zeroing and encrypting, lose to SplitMix64's one. On
  Zen4 it is a fifth of SplitMix64's rate hot, 11.6 against 59.7, which suggests aws-lc-rs's
  counter mode does not use VAES there; X13 did not look.
- **A Zen1 core makes and checksums 3.5 to 4.5 GiB/s**, within the 3 to 5 the statement expected.
  The CRC costs a Zen1 core about a third of SplitMix64's rate, since both are about one pass of a
  core's arithmetic; on Zen4 it costs a quarter.
- **The native build vectorizes the counter generators**: SplitMix64 and xoshiro256++ ran 1.2 to
  2.6 times as fast on europa from it, where AVX-512 has the 64-bit multiply and rotate their lanes
  need. AES and ChaCha8 dispatch at run time and ran alike from both builds.
- **Checking costs what making does**: *verify* ran within a quarter of *fill* for every published
  generator, since comparing a unit already in cache is cheap beside making it.
- **The stamped copy is the floor out of cache, not in it**: a copy from 64 MiB is memory's rate,
  6.4 GiB/s on Zen1, which is only a third over SplitMix64's; into a hot buffer it is five times as
  fast, and its bytes repeat.

Every cell at 4 KiB and 64 KiB is in the report; they say the same, each generator within about a
tenth of its 1 MiB rate at either size.

**A million objects described** expanded to paths and sizes at 3.1 to 4.0 million objects a second
on a Zen1 core and 7.1 to 10.5 million on europa, by shape: a description of a million objects is
its own plan in well under a second.

## 2. Against a server that discards

A put of 1 MiB frames from one client core over loopback, MiB/s, plaintext:

| Put, plaintext | titan | hyperion | europa native | europa `znver1` |
| --- | --- | --- | --- | --- |
| the baseline, nothing made | 3,578 [3,568–3,585] | 3,525 [3,506–3,530] | 6,412 [6,231–7,113] | 6,350 [6,219–7,075] |
| splitmix, fill+crc | 1,817 [1,802–1,821] | 1,789 [1,788–1,810] | **6,892** [6,272–7,487] | 5,830 [5,406–6,256] |
| aes-ctr, fill+crc | **1,860** [1,841–1,878] | **1,837** [1,824–1,837] | 4,925 [4,409–4,979] | 4,966 [4,429–4,999] |
| xoshiro, fill+crc | 1,697 [1,685–1,699] | 1,683 [1,680–1,690] | 6,296 [5,427–6,309] | 4,766 [4,258–4,788] |
| chacha8, fill+crc | 1,408 [1,392–1,414] | 1,404 [1,396–1,407] | 3,927 [3,741–3,949] | 3,619 [3,461–3,778] |
| the device's write rate, T1's line | 722 | 722 | 2,501 | 2,501 |

- **T1 holds on every judged leg, by 2.5 times on Zen1 and 2.8 on europa.** The client core was
  busy throughout on every side; the server's busiest executor was at 0.53 on Zen1, so the client
  set the rate, and at 0.86 on europa with SplitMix64, where the server had nearly caught up.
- **On europa making costs nothing a put notices**: the client spent 150 ms of cpu a GiB with
  SplitMix64's fill and CRC against 160 for the baseline, which makes nothing; both rates are the
  server's. On Zen1 making and checksumming doubled the client's cost a GiB, 564 against 286 ms.
- **AES-CTR is 2 to 3% faster than SplitMix64 on a Zen1 put**, the hot buffer's advantage, and
  SplitMix64 40% faster on europa's.

Under kTLS, MiB/s and what a fully busy client core would move:

| Put, kTLS | titan | hyperion | europa native |
| --- | --- | --- | --- |
| the baseline | 719 [656–1,015], 1,308 at a busy core | 944 [718–952], 1,293 | 2,729 [1,748–3,002], 2,906 |
| splitmix, fill+crc | 679 [580–968], 977 | 803 [625–967], 980 | 2,307 [1,828–2,807], 3,499 |
| aes-ctr, fill+crc | 615 [588–705], 997 | 999 [716–1,002], 1,002 | 1,906 [1,804–2,717], 2,940 |

- **kTLS puts the Zen1 rate near the device's, and swings by half between rounds.** titan's
  AES-CTR put was below 722 MiB/s in every round and hyperion's in one; neither end was saturated,
  the client 0.6 to 1.0 busy and the server's busiest executor 0.7, as X11 found and did not trace.
  A fully busy Zen1 core would have moved about 1,000 MiB/s.
- **Under kTLS the send is three quarters of a put's client cpu on Zen1**, 73 to 74% in every round
  with either SplitMix64 or AES-CTR, and nearly all of it on europa, where making cost nothing a
  plaintext put could see. That is the send's own cost, encryption included, against making and
  checksumming, and the statement made in advance, 70% or more, holds.

A get of 1 MiB frames, four outstanding, plaintext, MiB/s:

| Get, plaintext | titan | hyperion | europa native | europa `znver1` |
| --- | --- | --- | --- | --- |
| unchecked | 3,270 [3,256–3,285] | 3,217 [3,206–3,230] | 5,731 [5,706–5,772] | 5,733 [5,703–5,754] |
| by the ledger's CRC | 3,227 [3,223–3,232] | 3,170 [3,141–3,180] | 5,564 [5,527–5,605] | 5,550 [5,543–6,389] |
| made again by splitmix | 2,506 [2,422–2,567] | 2,551 [2,539–2,559] | 6,127 [5,432–6,826] | 5,624 [5,615–6,937] |
| made again by aes-ctr | 3,304 [3,279–3,328] | 3,257 [3,241–3,268] | 6,240 [6,228–6,269] | 6,278 [5,756–6,294] |
| the device's read rate, T2's line | 857 | 857 | 2,553 | 2,553 |

- **T2 holds on every judged leg**: 3.9 times the device on Zen1 with AES-CTR, the leg's fastest
  put (2.9 with SplitMix64), and 2.4 on europa with SplitMix64. Every published generator checked
  faster than its host's device reads, ChaCha8 by the least, 1.8 times on europa. No get read a
  byte it did not expect.
- **The server set every unchecked rate**: its executor was fully busy answering, and the client
  0.57 to 0.60. Checking by regenerating raised the client's cpu a GiB on Zen1 from 179 ms to 376
  with SplitMix64 and 293 with AES-CTR, and on europa from 106 to 111 with SplitMix64.
- **The ledger's CRC is the cheapest check**, 24 ms a GiB on Zen1 and 9 on europa over none, but it
  says only that a unit is the one written there, which a CRC kept from the write already said;
  making the bytes again says they are the object's bytes at that offset.

## 3. Several cores

**3a, with no wire**: fill+crc of 1 MiB units out of cache, every core taking units from an arena
of its own, GiB/s summed:

| Cores | titan, splitmix | titan, aes-ctr | europa native, splitmix | europa native, aes-ctr |
| --- | --- | --- | --- | --- |
| 1 | 3.55 | 3.21 | 17.8 | 7.69 |
| 2 | 6.33 | 5.98 | 18.9 | 14.9 |
| 4 | 9.53 | 8.33 | 18.8 | 20.3 |

- **Memory, not cores, is what making into a cold arena runs out of**: europa's cores shared about
  19 to 20 GiB/s, which one SplitMix64 core nearly filled and four AES-CTR cores did; titan's four
  reached 2.7 times one.

**3b, with the wire**: the put with fill+crc, plaintext, one stream a client core, MiB/s summed:

| Streams | titan, splitmix | titan, aes-ctr | europa native, splitmix | europa native, aes-ctr |
| --- | --- | --- | --- | --- |
| 1 | 1,802 | 1,848 | 7,408 | 4,935 |
| 2 | 3,128 | 2,909 | 14,028 | 9,793 |
| 4 | — | — | 27,159 | 19,042 |

- **A stream's own bytes scale with its cores**, because a frame is made into a buffer that stays
  in cache: four europa streams put 27,159 MiB/s, 3.7 times one, where making into a cold arena
  could not pass 19 GiB/s. Two on titan put 1.74 times one; the second ran on cpu 1, beside cpu 0's
  interrupts, which is all a four-core host has left beside a server of two executors.

## 4. A folder of real files

One core reading 2 GiB of files of a size in 1 MiB reads, MiB/s:

| Files | titan, cold: read / with CRC / with SHA-256 | titan, hot SHA-256 | europa, cold: read / with CRC / with SHA-256 | europa, hot SHA-256 |
| --- | --- | --- | --- | --- |
| 64 KiB | 393 / 382 / 313 | 1,240 | 1,063 / 1,049 / 739 | 1,765 |
| 1 MiB | 768 / 731 / 528 | 1,441 | 2,194 / 2,128 / 1,166 | 2,209 |
| 64 MiB | 860 / 860 / 601 | 1,468 | 2,606 / 2,605 / 1,315 | 2,273 |

Every cold read came from the device: its own counters read the folder's bytes once, every round.
hyperion repeated titan.

- **A folder read cold runs at its device's rate, from large files**: 860 MiB/s on the 970 EVO and
  2,606 on the Optane at 64 MiB, which X6 measured at 857 and 2,553. That half of the statement
  holds.
- **SHA-256 on the reading thread costs a scan a third on Zen1 and half on europa**, though alone
  it runs at 1,441 and 2,209 MiB/s: reading and hashing on one core add. A CRC on the same thread
  costs 0 to 5%. So the statement's other half holds where F66 digests a file as it reads it, and a
  folder's digest is taken on a thread beside the reads.
- **Small files read one at a time are bound by the device's latency**: 64 KiB files came at 393
  MiB/s on the 970 EVO and 1,063 on the Optane, under half the device, so a folder is read several
  files at once.

## 5. Across the network

europa's native client driving titan's server across 1 GbE, one client core, 1 MiB frames. Every
side ran at the link's rate, so what this leg measures is the driver's cpu at that rate:

| Across 1 GbE | MiB/s | europa's client cpu ms/GiB, plaintext | its busy share | kTLS cpu ms/GiB | its busy share |
| --- | --- | --- | --- | --- | --- |
| put, the baseline | 112.3 | 132.5 [130.1–135.4] | 1.5% | 446.2 [441.0–450.4] | 4.9% |
| put, splitmix fill+crc | 112.3 | 169.0 [168.1–169.5] | 1.9% | 404.3 [399.1–410.3] | 4.4% |
| put, aes-ctr fill+crc | 112.3 | 276.4 [274.1–278.5] | 3.0% | 501.5 [480.8–509.8] | 5.5% |
| get, unchecked | 112.4 | 305.7 [303.7–306.3] | 3.4% | 541.5 [540.5–543.4] | 5.9% |
| get, made again by splitmix | 112.2 | 340.6 [339.0–342.5] | 3.7% | 577.8 [571.7–579.7] | 6.3% |

- **A driver core that makes its own bytes feeds fifty links like the lab's.** At 112 MiB/s
  europa's client spent 1.9% of a core putting SplitMix64's bytes with their CRC, 4.4% under kTLS,
  and 3.7% getting and checking them. The bench drives the lab's three nodes from europa, so the
  driver is nowhere near its limit there.
- **The server spent far more than the driver**: titan's executors 1.7 s of cpu for every GiB a
  plaintext put brought them, 2.6 s under kTLS, which is X11's figure for a Zen1 server at the
  link's rate.
- **No get read a byte it did not expect**, plaintext or under kTLS, in any round.

<!-- RESULTS-PLACEHOLDER -->

## What would have changed the design

| Result named in advance | Found | So |
| --- | --- | --- |
| One core cannot make seeded bytes as fast as a pool takes them (T1) | **Does not fire.** One Zen1 core put 1,837 to 1,860 MiB/s of made and checksummed frames against the 970 EVO's 722; europa's put 6,892 against the Optane's 2,501 | A stream makes its own bytes, inline, on the task that sends them. The capture still keeps each driver thread's busy share, since a pool of several devices takes more than one core does |
| Regenerating a read to check it costs more than the read (T2) | **Does not fire.** A Zen1 core got and regenerated 3,257 to 3,304 MiB/s with AES-CTR, the legs' fastest put, against the 970 EVO's read rate of 857; europa 6,127 with SplitMix64 against 2,553 | Read-back makes each unit again and compares |
| No published definition is fast enough (T3) | **Does not fire.** SplitMix64 filled 1 MiB out of cache at 5,018 to 5,023 MiB/s on Zen1 and 23.3 GiB/s on europa | A description's bytes come from a published definition; the stamped copy is not used |

## The comparison

| Generator | Zen1, fill 1 MiB cold / hot, GiB/s | Zen1 put with CRC, MiB/s | Zen4 native, fill 1 MiB cold / hot | Definition | Verdict |
| --- | --- | --- | --- | --- | --- |
| **SplitMix64, counter mode (recommended)** | 4.90 / 5.00 | 1,817 | 23.3 / 59.7 | Vigna's; ten lines; what `shoal-loadgen` seeds every draw with | Taken: fastest out of cache everywhere, no crate, no cpu feature |
| AES-128-CTR, aws-lc-rs | 4.27 / 6.71 | 1,860 | 8.54 / 11.6 | FIPS 197 and SP 800-38A | Faster only into a hot buffer on Zen1; slower on Zen4; a crate and AES-NI under it |
| xoshiro256++, four lanes | 3.45 / 4.98 | 1,697 | 15.5 / 23.4 | Vigna's, with X13's lanes and seeding | Slower than SplitMix64 cold, and more definition |
| ChaCha8, `rand_chacha` | 2.79 / 2.78 | 1,408 | 6.91 / 7.07 | Bernstein's, with `rand_chacha`'s layout | The slowest |
| Stamped copy | 6.44 / 27.1 | 2,182 | 12.6 / 55.0 | X13's own | Rejected by T3's holding: its bytes repeat from object to object, and a published definition is fast enough |

## Recommendation

**The driver makes an object's bytes itself, on the stream that sends them, from a description of
integers whose bytes are SplitMix64 in counter mode**, recorded on
[S18](contract.md#q30-the-object-dataset-and-seeded-bytes-2026-10-05):

- **A stream makes its own bytes inline.** Each frame's payload is filled into the frame buffer and
  checksummed as the frame goes, on the task that writes it. No generator pool, and no bytes made
  ahead of the stream: one core is faster than any one lab device.
- **A description is integers alone.** Object count; a size distribution drawn without a float,
  which is fixed, uniform, doublings or a weighted table; a seed; and the generator by name and
  definition version, `splitmix64-ctr/1`. An object's size and path come from its index, and its
  bytes from its index and the offset, so any object, and any range of it, can be made alone.
- **SplitMix64 in counter mode** makes the bytes: word `i` of object `o` is
  `mix(seed ^ o·γ + (i + 1)·γ)`, little endian, the state `shoal-loadgen`'s `Seeded::at(seed, o)`
  starts from. M13 freezes its digests as literals, as it freezes X5's checksum vectors, and a
  second generator is a new name, never a change to this one.
- **Read-back makes each unit again and compares**, beside the CRC-64/NVME the client checks on the
  wire anyway.
- **A folder keeps F66's SHA-256 of each file**, taken on a thread beside the reads, and is read
  several files at once.

## What a capture gains

What a capture would have to say of each shape, so that two captures of it can be compared and one
of it reproduced:

| Shape | The capture keeps |
| --- | --- |
| A description | The description itself, whole, since it is a few lines; its digest, which `compare` joins on as it joins a table file's; the generator's name and version |
| A folder | Every file's path, size and SHA-256 and the folder's digest over them, as F66's scan keeps a table file's; the driver host's device and filesystem, since the folder is read from them |
| Either | Each driver thread's busy share every second, the bytes it made or read, and the MiB/s a fully busy core would reach at the cost it paid a byte; whether every acknowledged byte was read back and matched |

## What X13 does not settle

- **The object arms**: put, get, a ranged read, a write in place, append, stat and delete as
  operation kinds, each a sequence of frames, which F69's one query an operation cannot carry. M13.
- **How a run spreads streams over cores** for a pool of several devices. The rates here say how
  many devices one core feeds; the choice is M14's.
- **Why kTLS's rounds disagree**: titan's kTLS put ran at 588 MiB/s in some rounds and 705 in
  another with nothing else on the host, as X11 found and did not trace.

## What it did not measure

- **The product's client.** The stub writes X11's frames; the product's client archives a bundle,
  which costs what [S12](wire-and-client.md)'s object frames will not. M13's baseline measures it.
- **A link faster than 1 GbE.** Every rate across the network is the link's.
- **A driver on a host other than europa.** The bench's driver runs on europa; a Zen1 driver is
  measured over loopback only.
- **Compressibility.** Every published generator's bytes are incompressible for any practical
  compressor; no device here compresses, so it was not measured.

## What it found in the tree

None of these is a defect of the product, so none is filed on
[Known Issues](../appendix/known-issues.md).

| Finding | Where |
| --- | --- |
| X11's setup frame overwrote its first-in-first-out flag with the window, so X11's server wrote first in first out in every cell. Found reading the server this spike reuses; filed and fixed as item 213, with section 3's read cells run again | [Resolved #213](../appendix/resolved/x11-setup-fifo.md) |
| `crc-fast` needs `spin` 0.10 on x86-64 whatever its features, so it brings two crates into the lockfile, not one | `crc-fast` 1.10.0's manifest |
| `rand_chacha` 0.3's tests carry vectors for ChaCha20 only; ChaCha8 was checked against draft-strombergson-chacha-test-vectors | `rand_chacha/src/chacha.rs` |

## Related

- [X13](spikes.md#x13-the-benchmarks-shape) for what was planned;
  [S18](contract.md#q30-the-object-dataset-and-seeded-bytes-2026-10-05) for the decision;
  [Q30](contract.md#questions-to-answer).
- [F69](../features/driver-operation-kinds.md), which settled the driver's shape;
  [S15](performance.md), the benchmark these figures shape; [F66](../features/dataset-benchmarks.md)
  for the table dataset a folder follows.
- [X5's record](checksums.md) for the checksum every unit here is checksummed with;
  [X11's record](streamed-bodies.md) for the server and the frames;
  [X6's record](device-store-ssd.md) for the device rates the triggers were judged against.
- `shoal-spike/results/x13-report.md`, every figure merged across rounds;
  `shoal-spike/results/x13-*-r*.md`, each round's tables.

