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
of 512 KiB (`src/common/options/osd.yaml.in`).

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
cancel each other out".

The preferred direction is that check: each holder folds its chunk's units into one unit's
worth of summary, and the driver encodes the data summaries and compares them with the
parity summaries. It moves one unit a chunk and not the chunk. Whether it is done on every
deep scrub or on a sample is [Q28](contract.md#questions-to-answer).

Under replication the check is simpler: the copies' checksum tables must be equal.

### Quarantine and repair

A corrupt stripe chunk is set aside before anything else happens: it is no longer a chunk a
read is offered or a rebuild may use. Then it is rebuilt as any stale chunk is
([S10](recovery.md#rebuilding-a-stripe-chunk)), from `k` chunks that verify, the rebuilt chunk is
verified, and only then is the damaged copy removed. Until it is, the damaged copy is the
evidence.

**A rebuild on a checksum's evidence is automatic.** F44's scheduled scrub never installs,
because there an install replaces a copy by a rule. Here the chunk condemned itself and its
replacement is checked against the same row.

**Everything else stops with its evidence.** Fewer than `k` verified current chunks; a
parity check across a stripe's chunks that fails while every unit verifies; a chunk whose
label no row state explains. None of those is repaired by a rule. Each is reported by name
with what was found, and an operator's verb is what proceeds
([S14](operations.md#admin-operations)).

A device whose chunks keep failing is failing. Its count of checksum failures is in its
status report, and past a threshold it is drained as a device that asked to leave is.

### Schedule and budget

- **A byte budget for each device**, shared with recovery ([S10](recovery.md#budgets)). A
  scrub that is not bounded in bytes is the F44 arrangement, and on a rotational disk it is
  the foreground's latency.
- **A cadence for each kind**, and a window of hours a scrub may run in.
- **Staggered by placement group**, so that a pool's scrubs do not all start together.
- **On by default.** A scrub that is off finds nothing, and R18 asks for the opposite of
  F44's default. What makes that affordable is the budget, and the default budget is
  [X12](spikes.md#x12-recovery-and-scrub-rates)'s to find.

The arithmetic that sets the scale: a 16 TiB disk read at 30 MiB/s takes six and a half
days. A weekly deep scrub of a large rotational disk is therefore most of that disk's idle
time, and a budget small enough to leave the foreground alone may not finish in a week.
Ceph's weekly default is a starting hypothesis here and nothing more.

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
  with rows and with the other holders' listings.
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
on a rotational disk. In a running cluster, a background arm of [S15](performance.md) in the
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
| `scrub_stays_inside_its_byte_budget` | A device's scrub reads never exceed its budget over any second | M17 |
| `unexplained_chunk_is_found_and_reclaimed` | A chunk no row names is found by a light scrub and removed only on a committed fact | M20 |

## Related

[S6](device-store.md) for the checksums; [S10](recovery.md) for rebuilding; [S8](erasure-coding.md)
for why parity needs its own check; [S14](operations.md) for the verbs;
[F44](../features/repair.md) for how tables do this and why objects cannot do the same;
[S17](prior-art.md#ceph) for Ceph's scrubs.
