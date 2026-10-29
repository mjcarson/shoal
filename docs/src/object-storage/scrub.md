# S11. Scrubbing and repair

## Context

A disk can return bytes it was never given and report nothing wrong. An object that is read
once a year would carry that damage until the day it was needed, and by then the other
chunks of the same stripe may have rotted too. R18 asks that objects be scrubbed: read on
purpose, on a schedule, so that damage is found while there is still enough redundancy to
mend it.

This page is the three places damage is caught, what counts as evidence, and why repairing a
stripe chunk here needs no vote.

## What exists today

Tables are scrubbed and repaired by [F44](../features/repair.md).

- **Every archive record carries a checksum verified on every read**: `[size][gxhash64][payload]`,
  written by `write_record` and checked by `read_record` and nowhere else.
- **A scrub is a log entry.** Every replica of a tablet group applies it at one index and
  takes a canonical digest of its rows there, so the copies are compared at a common
  committed boundary.
- **A strict majority of verified copies is the authority.** A copy that fails a checksum is
  quarantined on that evidence alone; among the rest, the majority's digest is trusted; a
  split installs nothing.
- **A scheduled scrub verifies and never installs**, and it is off by default:
  `cluster.repair.scrub_interval` is absent, because "the measurement that would justify a
  default is the background arm's" (`shoal-core/src/server/conf/cluster.rs:706-712`).
- **Nothing bounds a scrub's reads in bytes.** The settings are an interval, a timeout and
  how many repairs one shard drives at once (`cluster.rs:706-724`): "A repair reads a
  group's archives whole".

That machinery will scrub a bucket's two metadata tables as it scrubs any table, unchanged.
It cannot scrub a stripe chunk: a chunk is in no group's log, so there is no entry to apply
and no boundary to take, and the chunks of an erasure coded stripe differ from each other,
so there is no digest to compare.

Ceph's shape is two scrubs. "Light scrubbing checks the object size and attributes, and is
usually done daily. Deep scrubbing reads the data and uses checksums to ensure data
integrity, and is usually done weekly" (`doc/rados/configuration/osd-config-ref.rst` at
`v20.2.0`). The defaults behind "daily" and "weekly" are `osd_scrub_min_interval` of one
day and `osd_deep_scrub_interval` of seven, with three scrubs at once for an OSD and reads
of 512 KiB (`src/common/options/osd.yaml.in`). Each PG's next scrub is drawn around them: a
light one 1 to 1.5 days after the last, a deep one at 7 days with a deviation of 1.4
(`src/osd/scrubber/scrub_job.cc:115-118`, `:251-255`;
[X14](ceph-and-s3-sources.md#7-scrub-the-scheduler-and-what-a-deep-scrub-of-an-erasure-coded-pool-verifies)).

✅ **Built for replicated pools by [F89](../features/scrub-and-repair.md)** (M17, 2026-10-15). A
chunk that fails its own checksum is set aside in its slice's `quarantine/` and counted against its
device, a read's failure is reported by marking its position missed, and a rebuild never takes a
corrupt source. Each stripe group's leader drives a light and a deep scrub, one placement group at a
time from a cursor committed in the group's placement state, scheduled by its pool's policy, and
its pieces are served one at a time behind the foreground and recovery under the device's ceiling.
The design below is what was built, with the places it was built otherwise marked. ✅ Since
[F92](../features/reclamation-and-device-lifecycle.md#what-a-light-scrub-cannot-explain) (M20) a light
pass asks a row-less chunk's owner and removes what its entry names nowhere.

## The design

### Three places damage is caught

| Where | What it reads | What it finds | How often |
| --- | --- | --- | --- |
| **Every read** | The chunk units the read touches | A unit that fails its checksum | Whenever anything is read ([S6](device-store.md#reads)) |
| **Light scrub** | No data. Each slice lists what it holds: consumer, owner, stripe, position, label, length, to be compared with the rows | A chunk that is missing, a label that is stale, a chunk nothing names | Often. It is a directory listing and a comparison |
| **Deep scrub** | Every chunk unit of every chunk, read and verified on the slice that holds it | A unit that fails its checksum, in bytes nobody has asked for | Rarely. It is the whole device, read once |

A scrub is driven for each placement group by the leader of the tablet group that owns it,
as a repair is driven today, with a cursor committed as it goes so that a new leader resumes
where the last one stopped. The reading is done by each holder on its own slice. Only
listings, verdicts and summaries cross the network.

### What counts as evidence

Two authorities, and neither is a vote.

- **The row says which write a stripe chunk should hold.** A chunk whose label is older than
  the row's is **stale**: its slice missed a write. That is not damage. It is the ordinary
  business of [S10](recovery.md#how-a-slice-learns-what-it-missed), and a light scrub's
  job is to notice the ones the missed record lost track of.
- **The checksum says whether the bytes are that write's.** A chunk whose label is right
  and whose unit fails is **corrupt**. That is damage, and it is counted against the device.

A scrub that could not tell the two apart would report every device that had been down for
an hour as rotting, and [P15](contract.md#the-contract) requires that it can.

Because each stripe chunk carries its own evidence, a corrupt chunk is rebuilt from the
others without anyone being asked which copy is right. F44 needs a majority because two table
copies that differ are each internally sound and something must choose. Here a chunk that
fails its checksum is wrong by its own account, and `k` chunks that verify under the row's
labels are right by theirs.

### What a deep scrub proves, and what it does not

A deep scrub proves each stripe chunk is what was written to it. Under an erasure code that
leaves one gap: it does not prove that a stripe's parity chunks match its data chunks, unit
row by unit row. A write that staged parity computed wrongly, on a faulty core or by a defect
in the code, verifies perfectly on every slice and decodes to garbage.

Recomputing parity from the data would close the gap by moving every data chunk to one
place, which is a rebuild's cost paid on every pass. Ceph's design document for its Tentacle
release names the same problem, "A full consistency check would require large data transfers
between the shards so that the coding parities could be recalculated and compared with the
stored versions, in most cases this would be unacceptably slow", and a way round it that
works for any linear code: "if the contents of a chunk are XOR’d together to produce a
longitudinal summary value, then an encoding of the longitudinal summary values of each data
shard should produce the same longitudinal summary values as are stored by the coding parity
shards" (`doc/dev/osd_internals/erasure_coding/enhancements.rst`). It also names the cost:
"There is a risk that by XORing the contents of a chunk together that a set of corruptions
cancel each other out". **It was not built.** In `v20.2.0` a deep scrub of an overwritable
erasure coded pool reports a digest of zero for every shard and compares nothing between them
(`src/osd/ECBackendL.cc:1831-1835`, `src/osd/ECBackend.cc:1223-1224`). Only a pool that never
overwrites keeps a checksum a shard. [X14](ceph-and-s3-sources.md#7-scrub-the-scheduler-and-what-a-deep-scrub-of-an-erasure-coded-pool-verifies)
corrupted a parity shard on the lab with its checksums intact, and nothing found it. The check
below is this part's own.

The preferred direction is that check: each holder folds its chunk's units into one unit's
worth of summary, and the driver encodes the data summaries and compares them with the
parity summaries. It moves one unit a chunk and not the chunk. Whether it is done on every
deep scrub or on a sample is [Q28](contract.md#questions-to-answer). ✅ On a sample, since
[X12](recovery-scrub-rates.md#1-a-cores-cpu): the check holds, and a planted parity byte with its
checksum made to match was found every time a walk reached it, but a fold is a second pass over
every unit. In the checksum's own pass it added 0.49 of the checksum's cost on Zen1 and 0.71 on
Zen4, and folding to one 4 KiB block instead of a unit saved little. The check's linearity holds at
any summary length, since a code acts on every byte position alike. ~~How large the sample is, is
M17's.~~ ✅ A quarter of a pool's placement groups a deep pass, rotating, so every group's parity is
checked every fourth pass, decided with the user with F89
([the record](contract.md#m17-scrub-and-repair-2026-10-15)); the check is M18's to build. ✅ Built by
[F90](../features/erasure-coded-pools.md#the-scrubs-and-the-parity-check): each holder folds the units
it verifies into a 4 KiB summary, answered beside their checksums; once every chunk holding bytes
verified at the label its row names, the driver encodes the data chunks' summaries on an object
executor and compares them with the parity chunks'. A mismatch is counted (`parity_mismatch`) and
logged, and nothing marks or rebuilds it; `parity_every` in a pool's scrub policy sets the sample.

Under replication the check is simpler: the copies' checksum tables must be equal. ✅ As built,
each holder answers the checksum of each unit's bytes alone, unbound from its place, in the pass
that verifies it, and the driver compares the copies at one label.

### Quarantine and repair

A corrupt stripe chunk is set aside before anything else happens: it is no longer a chunk a
read is offered or a rebuild may use. Then it is rebuilt as any stale chunk is
([S10](recovery.md#rebuilding-a-stripe-chunk)), from `k` chunks that verify, the rebuilt chunk is
verified, and only then is the damaged copy removed. Until it is, the damaged copy is the
evidence.

✅ As built, a chunk is set aside by a rename into its slice's `quarantine/`, under its own path and
a count, and from then on is missing to every path; a whole chunk placed at its id is read back and
verified by the slice before the evidence is removed, and one that fails is set aside beside it.

**A rebuild on a checksum's evidence is automatic.** F44's scheduled scrub never installs,
because there an install replaces a copy by a rule. Here the chunk condemned itself and its
replacement is checked against the same row.

**Everything else stops with its evidence.** Fewer than `k` verified current chunks; a
parity check across a stripe's chunks that fails while every unit verifies; a chunk whose
label no row state explains. None of those is repaired by a rule. Each is reported by name
with what was found, and an operator's verb is what proceeds
([S14](operations.md#admin-operations)). ✅ As built, each is logged by name and counted in `pools`;
~~the verb, `ResolveStripe`, is M21's.~~ the verb, `ResolveStripe`, is built by
[F93](../features/operations-and-the-real-cluster.md#a-stripe-ahead-of-its-row): `lost` declares any
stripe lost, and `accept` settles a stripe ahead of its row. Naming which of two copies that each
verify to trust is [not built](../appendix/todos.md#what-f93-left-for-later).

A device whose chunks keep failing is failing. Its count of checksum failures is in its
status report, and past a threshold it is ~~drained as a device that asked to leave is~~ failed by
its node as an operator's order is carried out, and F88's retirement moves its groups off: ten
failures within ten minutes unless the device says otherwise, decided with the user with F89. ~~A
drain that keeps the device serving while its groups move needs `DrainDevice`, which is not built.~~
✅ Since [F92](../features/reclamation-and-device-lifecycle.md#a-devices-life) three failures in a day
drain it first, while it serves its copies, decided with the user; ten in ten minutes still fail it.

### Schedule and budget

- **A byte budget for each device**, shared with recovery ([S10](recovery.md#budgets)). A
  scrub that is not bounded in bytes is the F44 arrangement, and on a rotational disk it is
  the foreground's latency. Since [X12](recovery-scrub-rates.md) the budget is a ceiling, and
  the scrub takes the slice's idle time beneath it, a piece of 1 MiB at a time.
- **A cadence for each kind**, and a window of hours a scrub may run in. A deep scrub's
  interval is how long a pass takes beside its device's foreground, and is not a constant (X12).
- **Staggered by placement group**, so that a pool's scrubs do not all start together.
- **On by default.** A scrub that is off finds nothing, and R18 asks for the opposite of
  F44's default. What makes that affordable is the budget, and the default budget is
  [X12](spikes.md#x12-recovery-and-scrub-rates)'s to find. ✅ It found pacing, not a rate: below.

✅ As built by [F89](../features/scrub-and-repair.md), the cadence and the window are the pool's
policy, `scrub: {light, deep, window}`: a light pass daily, and a deep pass no sooner than a week
after the last began, or as it finished when it was slower, at any hour unless a window of UTC hours
is set. A scrub's request waits for its slice's foreground **and its recovery**, takes the slice's
one permit, and draws on the device's `recovery_ceiling`, the budget rebuilds and moves draw on. The
stagger is a tablet group's, not a placement group's: a group's first pass waits a share of its
interval fixed by its identity, never none, after its leader first sees it, and a pass then takes
the group's placement groups in key order. An operator's `scrub` begins a pass at once, whatever the
window.

The arithmetic that sets the scale: a 16 TiB disk read at 30 MiB/s takes six and a half
days. A weekly deep scrub of a large rotational disk is therefore most of that disk's idle
time, and a budget small enough to leave the foreground alone may not finish in a week.
Ceph's weekly default is a starting hypothesis here and nothing more. X12 replaced it with
the time a pass takes, below.

[X7](device-store-hdd.md#11-a-foreground-beside-a-scrub) put a budget to the lab's disks on
2026-10-06, with a deep scrub reading whole chunks beside a small foreground write and reads.
No budget of 10 MiB/s or more kept the write's p99 wholly within 1.25× of its p99 with no scrub
on any XFS leg. On europa's disk 10 MiB/s raised it 1.7 to 2.7 times, wholly, with the write
cache on or off. At 10 MiB/s a 14 TB disk takes fifteen and a half days to read anyway. So a
disk's deep scrub is paced by the arm's idle time, issuing its reads when the slice has no
foreground work outstanding, with the byte budget as its ceiling
([Q23](contract.md#q23-what-a-rotational-device-needs-2026-10-06)). How it paces, and the
ceiling a disk ships with, are X12's.

[X12](recovery-scrub-rates.md#5-a-deep-scrub-on-a-disk) put the pacing to every lab device on
2026-10-09, with real checksums and the parity check, the disks' cache off and the foreground's
journal on the SSD, and [S18](contract.md#q28-and-q29-and-q17-in-part-recovery-and-scrub-rates-2026-10-09) records Q28:

- **A deep scrub issues a 1 MiB piece only while its slice has no foreground operation in
  flight, one at a time.** On a disk that read 44 to 59 MiB/s, and the foreground's p99 paid 1.10
  to 1.34 times at the median. A fixed 5 or 10 MiB/s, a tenth of the rate, cost about the same,
  1.04 to 1.28; a fixed budget's cost grew with its rate, 1.46 to 2.18 at 40 MiB/s. On the
  970 EVO it read 385 MiB/s and cost nothing measurable, where a fixed 50 MiB/s doubled the read
  p99.
- **A disk ships with no ceiling below what idle pacing gives.** A ceiling of 20 MiB/s cost the
  foreground what none did, at a third of the rate. The ceiling keeps a scrub beneath recovery,
  not beneath the foreground.
- **A deep scrub's interval is how long a pass takes.** At 44 to 59 MiB/s a 16 TiB disk is read in
  3.3 to 4.4 days, so a week holds for these disks beside this foreground. A disk whose foreground
  leaves it less idle time is scrubbed less often, and says so. No pace above 5 MiB/s kept a
  disk's foreground wholly within 1.25× (S1 fired), so a disk's objective is M17's to state from
  these figures: at 5 MiB/s 16 TiB would take 39 days. ✅ Stated with F89: a disk's foreground p99
  within 1.5 times its own, and an SSD's within `max(1.25 × its own, its own + 1 ms)`
  ([the record](contract.md#m17-scrub-and-repair-2026-10-15)).
- **Pieces of 1 MiB**: 4 MiB pieces doubled the 970 EVO's read p99 even in its idle time, and on a
  disk 256 KiB and 4 MiB were both worse than 1 MiB.
- **The Optane is the exception.** Its foreground answers in 86 µs, and no pace kept it under
  either line: a piece in flight, or the scrub's cpu holding the executor, is several times that.

## Alternatives rejected

**Comparing stripe chunks across holders.** Under an erasure code they differ by design. Under
replication it moves every byte across the network to learn what two checksum tables say.

**A vote.** See [What counts as evidence](#what-counts-as-evidence).

**A scrub as an entry in the group's log**, as F44 does it. The boundary it buys is for
comparing copies of rows that are still changing. A chunk's label already says which write
it holds, so there is nothing to freeze.

**Full parity recomputation on every pass.** A rebuild's cost, paid weekly, for every
stripe.

**No scheduled scrub; verify on read only.** The bytes that rot unnoticed are the ones
nobody reads.

## What it costs

- **A deep scrub reads every byte of every device once a cycle**, and a checksum's worth of
  CPU for each. On an SSD pool that is device time; on a rotational one it is the arm.
- **A light scrub lists every placement group's directory on every holder** and compares it
  with rows and with the other holders' listings. It reads each chunk's header for its label,
  which [X6](device-store-ssd.md#5-listing-a-placement-group) measured: 37.5 s cold for a
  million chunks on the 970 EVO under XFS and 15.8 s on the Optane, the same a chunk at a
  hundred thousand. A 16 TiB device of 4 MiB chunks takes two and a half minutes, so the light
  scrub needs no index of its own. On ext4 a placement group of a million one-chunk objects
  took 154 s.
- **A summary a chunk** for the parity check, and an encode of `k` of them a stripe.
- **Space for a quarantined chunk** until its replacement is verified.

## What it breaks

- "A scrub is a log entry every replica applies at one index"
  ([F44](../features/repair.md#design-choices)): for tables, still. An object scrub has no
  entry.
- "A scheduled pass verifies and never installs": an object scrub rebuilds a chunk that
  failed its own checksum.
- "Scrubbing is off unless asked for."

## Invariants to uphold

- A stale stripe chunk and a corrupt one are reported as different things.
- A unit that fails its checksum is never returned, never merged into, never a source and
  never checksummed afresh.
- A corrupt chunk is quarantined before anything is rebuilt, and removed only after its
  replacement verifies.
- A chunk is rebuilt automatically only on its own checksum's evidence and only from `k`
  chunks that verify under the row's labels.
- Anything a checksum does not explain stops with its evidence.
- A scrub's reads are inside its device's byte budget.

## Prerequisites

[S1](prerequisites.md#required): the fixture's device faults, without which a flipped bit
cannot be arranged (✅ [F70](../features/storage-faults.md), which arms a torn write, a full
disk and a lost device but not yet a flipped bit), and a checksum with a frozen definition
(✅ CRC-64/NVME, chosen by [X5](checksums.md)). [S6](device-store.md) and
[S10](recovery.md).

## How it would be measured

[X12](spikes.md#x12-recovery-and-scrub-rates): how fast a device can be read and verified,
and what a foreground write's tail does while it is, at several byte budgets, on an SSD and
on a rotational disk. [X7](device-store-hdd.md#11-a-foreground-beside-a-scrub) took the
foreground's tail at budgets from 10 to 60 MiB/s on the lab's disks, with the write cache on and
off, and X12 starts from it. ✅ X12 measured it on 2026-10-09, on every lab device at four paces
([its record](recovery-scrub-rates.md)). In a running cluster, a background arm of [S15](performance.md) in the
shape of today's `macro/cluster/background/repair`: a scrub asked for a third of the way
through, the foreground's distribution before, during and after, and what the scrub read.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `flipped_bit_is_found_by_a_read_and_by_a_deep_scrub` | A unit damaged on disk is refused by a read of it, and found by a scrub when nothing reads it | M17 |
| `scrub_never_launders` | No path rewrites a unit under a new checksum without having verified the old one | M17 |
| `corrupt_chunk_is_never_a_source` | A rebuild offered a corrupt chunk among its sources does not use it, and fails by name if that leaves fewer than `k` | M17 |
| `stale_is_not_reported_as_corrupt` | A slice isolated through writes is reported stale and never counted as damaged | M17 |
| `parity_check_finds_wrong_parity_that_verifies` | Parity staged with a deliberate error, every unit valid, is found by a deep scrub and is not repaired by a rule | M18 |
| `scrub_resumes_from_its_cursor` | A leader killed in mid scrub is followed by one that continues, and no placement group is skipped | M17 |
| `scrub_stays_inside_its_byte_budget` | A device's scrub reads never exceed its budget over any ~~second~~ window, beyond the one second's worth its ceiling starts with ([F89](../features/scrub-and-repair.md)) | M17 |
| `scrub_waits_for_the_slices_idle_time` | No scrub read is issued while its slice has a foreground operation in flight, and at most one is in flight at once ([X12](recovery-scrub-rates.md#the-comparison)) | M17 |
| `unexplained_chunk_is_found_and_reclaimed` | A chunk no row names is found by a light scrub and removed only on a committed fact | M20 |

## Related

[S6](device-store.md) for the checksums; [S10](recovery.md) for rebuilding; [S8](erasure-coding.md)
for why parity needs its own check; [S14](operations.md) for the verbs;
[F44](../features/repair.md) for how tables do this and why objects cannot do the same;
[S17](prior-art.md#ceph) for Ceph's scrubs.
