# F90 — M18, erasure coded pools

## Context

[M18](../object-storage/milestones.md#m18-erasure-coding) is the object store's eighth gate. After
[F89](scrub-and-repair.md) a pool's redundancy could be written `{erasure: {data: 4, parity: 2}}`,
was validated and placed, and every write to it was refused `Unsupported`: everything downstream of
the placement assumed `k = 1`, a stripe chunk that is the whole stripe and holders that hold the same
bytes. R11 asks for an erasure code beside copies, so that a pool can survive `m` losses for
`m / k` of its bytes again instead of `m` whole copies. This gate builds
[S8](../object-storage/erasure-coding.md) in its three steps - whole stripes, reconstruct-write and
parity delta - with the third row of [the acknowledgement rule](../object-storage/write-path.md#the-acknowledgement-rule),
degraded reads ([S9](../object-storage/read-path.md#degraded-reads)), rebuilds and moves by decode
([S10](../object-storage/recovery.md)) and the deep scrub's check of parity by summaries
([S11](../object-storage/scrub.md#what-a-deep-scrub-proves-and-what-it-does-not)).

The code and the crate were decided by [X4](../object-storage/erasure-coding-crates.md) on
2026-10-03: Reed-Solomon over GF(2^8) on ISA-L's Cauchy matrix through `rusty_erasure`, with plain
XOR at one parity chunk. What stood open was S18's Q20 geometry, and two choices the pages left to
this gate. Four decisions were taken with the user on 2026-10-15, before anything was built:

- **The layout is contiguous**: data chunk `j` holds bytes `[j·C, (j+1)·C)` of its stripe, and unit
  row `u` is unit `u` of every chunk. It is S8's diagram, the layout X1's model checked, and what
  `PoolSpec::chunk_bytes` already computed. A range inside a chunk is read from one slice; the cost
  is that a put holds a whole stripe before it can encode its parity.
- **A 4 MiB stripe and 64 KiB units for every pool**, the provisional defaults decided: a 4+2
  pool's chunks are 1 MiB, X6's floor on a device that flushes, and a 2+1's are 2 MiB; 64 KiB is
  X9's step and where X4's encode reaches its rate on both of the lab's microarchitectures. A
  rotational pool's own are M19's: ✅ `k` chunks of 4 MiB, decided with [F91](rotational-devices.md).
- **A write of part of a stripe makes its parity by delta when it touches fewer data chunks than it
  leaves** (`d < k − d`), and by reconstruct-write otherwise: the delta reads the `d` old ranges,
  and each parity holder reads its own parity where it lies, while reconstruct-write reads the
  `k − d` untouched units of the rows it reaches. One data chunk of 4+2 is a delta; of 2+1, a
  reconstruct-write.
- **No erasure coded write rides its commit.** X1's model checks the small write in its commit for
  a replicated stripe alone; an erasure coded one has parity to bring along, which no model has
  checked. Every erasure coded write is staged and `small_writes` stays refused on such a pool
  ([todo](../appendix/todos.md#an-erasure-coded-small-write-in-its-commit)).

The other decisions were mine, and are under **Design choices** with their reasons.

## What it does

### Configuration

A pool names its code; nothing else is new. Wire version 12 is the erasure wire: a cluster that has
not activated it refuses every write to an erasure coded pool `NotReady`, by name, and `shoaladm`
bootstraps at the version its build speaks, which is 12 now. A standalone node writes one from its
start.

```yaml
storage:
  pools:
    wide:
      class: ssd
      redundancy: {erasure: {data: 4, parity: 2}}
      failure_domain: device        # or host, rack, zone
      stripe: 4MiB                  # every field below optional
      unit: 64KiB
      f: 1
      scrub: {deep: 7d, parity_every: 4}
```

A pool is refused at its check when its stripe has one data chunk (that is copies: it says so and
names `{replicas: N}`), when it is wider than 32 chunks (a commit names the positions it changed in
a 32-bit mask), or when its stripe is not `k` chunks of whole units: a 4 MiB stripe at 3+2 is
refused, naming 3 MiB, the largest stripe of `k` chunks a power of two of units that fits.

### The code and the crate

`shoal-core/src/server/objects/codec.rs` is every computation an object path makes with the code:
encode, decode, a parity holder's fold, and a chunk's summary. `matrix_of(k, m)` is the one place
that says what the code is: identity over a row of ones at one parity chunk, so that the parity is
the data's XOR and not Cauchy's single row, and ISA-L's Cauchy matrix past it. Each layout's coder
is made once a thread on the best kernels the cpu has, chosen at run time by the crate (GFNI on
europa from a `znver1` build, AVX2 on the Zen1 hosts), and so is each parity row's own coder for a
holder's fold and each loss pattern's decode plan. One parity chunk is encoded by the crate's XOR
kernel, tested equal to the coder.

`rusty_erasure`, `rusty_erasure-core` and `rusty_erasure-accel` are dependencies of `shoal-core`,
each pinned `=0.4.1`: the facade asks for its sub-crates at `^0.4.1`, and 0.4.2 of both is
published. Their `unsafe` was read before they were added, as S18 required: every block a build for
x86-64 compiles is in `rusty_erasure-accel`'s `x86.rs` - the SSSE3, AVX2 and GFNI kernels for
encode, a single multiply-add, a fused update, and the RAID XOR and P+Q - and each is reached only
through a safe wrapper that asserts every slice's length and the tables' before any pointer
arithmetic, from a kernel set handed out only after its cpu features were detected; `core` and the
facade are `forbid(unsafe_code)`. Nothing in the read needed a change. What it means for Shoal is
that a length mismatch panics in a wrapper instead of misreading memory, so the codec checks every
shape before it calls the crate.

Every computation runs on an object executor, never on a table shard (X9): a put's pipeline is
already there, a parity holder's fold runs on its slice's owner, and a coordinating shard hands an
encode or a decode to `ObjectStore::compute`. Every call takes at most 64 KiB of input before its
task may yield, the step X9 set. Each executor counts its node's calls - bytes encoded, decoded and
folded, the nanoseconds the calls took without the yields, and bytes summarised - and the counts
reach `NodeStats::codec`, `shoaladm stats --json` and the fixture's `OBJECTS`.

### A put

A put to an erasure coded pool runs in the same pipeline on an object executor as a replicated one
(`objects/put.rs`, `pipeline_erasure`). It holds each stripe's bytes as they arrive, sends each data
chunk's pieces as soon as they fill, never across a chunk's end, and once the stripe is whole encodes
its parity in steps and sends it. A stripe shorter than its data chunks stores only the chunks its
bytes reach and the parity, each as long as its share: a stripe of 20,000 bytes at 4+2 is three
chunks of 20,000, and data chunks one to three are never stored. A position whose chunk holds no
bytes counts as held without a piece, by its put and by every write after it, since it is current by
holding nothing. Every piece names the stripe's length, which each chunk's header records in bytes
that were reserved: a chunk's own length is its share of the stripe, and a stripe its put wrote has
no row to say the rest.

### A read

A range of a stripe is read from the data chunks it falls in, each for exactly its part, at once,
and decodes nothing while they answer (`shard/objects/erasure.rs`, `read_erasure`). A part whose
holder is stale, down, or answers `Corrupt` is decoded from `k` other positions over exactly the same
range of their chunks - the parts already read first, then positions holding no bytes there, then the
rest, a holder on this node first - and no more. Only bytes at the label the row names are taken
(`take`): a holder ahead of the row reads the row again, and one at any other label is no source, so
no decode ever mixes two writes (P10). A corrupt holder is reported, its position marked missed as
F89 does. With fewer than `k` positions answering, that stripe fails by name and the object's other
stripes read as before. A part past a position's share of the stripe is zeros, asked of nobody.

### A write in place

An erasure coded stripe's write reads what it needs, stages what it changes under its label,
confirms every chunk it leaves alone, commits once `k + f` positions hold the stripe after it, and
applies (`write_erasure`). A stripe with no row gets one first, a no-op of its put's row, so no stage
is ever left on a stripe whose length one chunk cannot say. `plan_write` decides how:

- **a whole stripe** where the write covers every byte the stripe will hold, where a floor or a hole
  leaves old bytes a partial write could bring back (P13), or where the write starts past the
  stripe's end: the bytes a reader sees are read, decoded where they must be, the rest are zeros,
  the write is laid over, and the stripe is encoded once and every chunk that holds bytes staged
  whole. A position that holds none after it is left as it was; what its slice keeps is M20's
  (✅ [F92](reclamation-and-device-lifecycle.md#what-a-light-scrub-cannot-explain), a light scrub's to
  find);
- **parity delta** where the write touches fewer data chunks than it leaves and every one it touches
  is current: the touched chunks' old units are read, each touched data chunk is staged by the units
  it changes (or whole, if it held nothing or the write covers it), and each parity holder is sent
  the change, old XOR new, as `StageParity`. The holder reads its own parity at the label it
  expects, each unit verified before it is merged into (P15), folds `c · change` into it with its
  row's coder, and stages the **new values** as any stage of units is staged - never the change, so
  a record replayed is the same parity (X1's `parity_staged_as_a_patch`);
- **reconstruct-write** otherwise: every data chunk's units of the rows the write reaches are read,
  decoded where they cannot be, the touched ones laid over, the rows encoded again, and the new
  parity staged by units over the label the row names.

Every untouched position holding bytes is asked to confirm the label the row names, and counts only
on its holder's word in the write's round (Q16); a position holding none counts by holding none; a
moving position counts only where its old slice and its arrival both hold it. Fewer than `k + f`
and every stage is discarded, and the write is refused `NotReady` by name - or tried again if the
row moved meanwhile. A stage a holder refuses, a parity holder at another label among them, leaves
that position missed: a reconstruct-write would be refused by the same holder, so there is nothing to
fall back to. What a commit changed is the whole chunk at the positions it staged and no other, the
model's rule, so a stale position the write did not touch keeps its entry in the missed record.

### Recovery and moves

An erasure coded position's missed record is always whole, as the model's is, and its rebuild decodes
the whole chunk from `k` current chunks that verify, a piece of at most 1 MiB at a time, and writes
it in place at the row's label (`recover.rs`, `decode_whole`). A source that answers `Corrupt` is
marked in the commit that makes the target current and the next one taken; with fewer than `k` that
verify, the stripe is lost by name, as F89's is. A position holding no bytes is committed current
with nothing written. A move copies each arrival's chunk from its own position's old holder, since
another position's chunk is not its bytes, and decodes it from `k` others when that holder gives
nothing. A stripe with no row is found by its old holders' listings, which now carry the stripe's
length each chunk's header recorded. `decide`, which settles a stage its stager left, refuses to
guess an erasure coded stripe's length from one chunk; it never has to, since a writer makes the row
before it stages.

### The scrubs and the parity check

A light scrub of an erasure coded stripe expects no chunk at a position whose share of the stripe is
no bytes, and counts a chunk it finds there - one a cut left - as reclaimable. A deep scrub verifies
each chunk where it lies over its own share, and does not compare the chunks' checksum tables, which
for a code are each their own. On a quarter of a pool's placement groups a deep pass, rotating
(`ScrubPolicy::checks_parity`, `parity_every` four unless set), it also checks parity by summaries:
each holder folds the units it verifies into a 4 KiB summary, the XOR of the chunk's 4 KiB blocks
where they lie, answered beside its checksums (`VerifySummary`, `Summarised`); once every position
holding bytes verified at the label its row names, the driver encodes the data chunks' summaries on
an object executor and compares them with the parity chunks'. The code is linear and acts on every
byte position alike, so the data's summaries encode to the parity's exactly when the parity matches
the data, up to changes that cancel in the XOR. A mismatch is counted (`parity_mismatch`, beside
`parity_checked`) and logged at ERROR, and nothing is marked, set aside or rebuilt: data and parity
that each verify and disagree cannot say which chunk is wrong, and an operator's word does, ~~at M21~~:
since [F93](operations-and-the-real-cluster.md#a-stripe-ahead-of-its-row) `ResolveStripe` declares
such a stripe lost, and nothing yet names the data or the parity as the one to trust
([todo](../appendix/todos.md#what-f93-left-for-later)).

### The wire

Wire 12 adds two requests to the object lane and an answer: `StageParity` (kind 11), a parity
holder's stage of a change, and `VerifySummary` (kind 12), a verification answered `Summarised`
(answer 7) with its summary. A piece and a listing's entry carry the stripe's length in four bytes
that were reserved, zero where not recorded, and a chunk's header in eight. No older reader is kept,
as with F89: a cluster on 11 that cannot be upgraded is bootstrapped again.

### What an operator sees

`shoaladm pools` names each pool's layout - `4+2`, `3 replicas` - after its class, and its scrub
line ends with `parity checked N mismatched M` once a parity check has run. Each member's codec
figures are in `shoaladm stats --json` under `codec`, with the kernel set its cpu runs.

### The fixture

`OBJECTS` answers the node's codec figures and each slice's read bytes. The erasure coded rows read
chunk files by position across every node's devices (`erasure_chunks_of`), check every stripe's
parity against a fresh encode of its data (`parity_holds`), and rewrite a unit with its checksum
bound to the wrong bytes (`rewrite_unit_verified`), a chunk that verifies and is wrong.

## Design choices

**The stripe's length in a chunk's header.** A rowless stripe's row is built from a length, and three
paths built it from one chunk's: `decide`, a light scrub and a move. For a copy that is the stripe's;
for a code it is a share, and a 4+2 stripe of 3 MiB and one of 4 MiB have the same first chunk. A row
made from the wrong length makes every position past it empty, and every byte there read as zeros,
silently. The header, a piece and a listing each had reserved bytes, so the length rides there and a
replicated header is the bytes it always was.

**A whole-stripe rewrite under a floor.** Zeroing the units a floor hides changes data, so parity,
and a slice never shrinks a header's length. A partial write over a floor would have to stage zeros
on every data chunk the floor reaches and parity beside them; reading what a reader sees and encoding
the stripe once is simpler and is what a truncate followed by a write costs a copy too.

**The touched positions' mask in the commit.** The model's derive acts on the positions a write
touched; the product's acted on every position its commit named, and the conformance test passed
`every` for an erasure coded write. The two agreed only because no saved schedule had an untouched
stale position past the record's bound; past it, each write would have counted a drop, and a backfill
waits for drops to stop.

**A refused parity stage is a missed position**, not a fallback. S8 said a parity holder at another
label makes the write fall back or fail; reconstruct-write expects the same label of the same holder,
so the write is counted as S7 counts any other stage refused.

**Every arithmetic step on an object executor.** A coordinator's decode of a range and its encode of a
stripe are object work, which X9 found costs a table's p99 two to four times on its shard.

**Counting a decode's input.** The codec counts the bytes a call takes in, so an encode's rate is its
data's and a decode's is what it read to make its targets; a degraded read of a range decodes from
`k` times the range.

## Alternatives rejected

**Round-robin units**, Ceph's layout: a long read fans over every holder and a put encodes every `k`
units as they arrive. Rejected with the user: a range crossing units touches several holders, a
rotational disk wants long runs, and X1's model checked the contiguous layout.

**A fixed chunk of 1 MiB or 4 MiB, the stripe `k` of them.** Rejected with the user: an erasure coded
position's rebuild is of a whole chunk, so a larger chunk is a dearer rebuild of a small missed write,
and M19 sets a disk's own (✅ [F91](rotational-devices.md): 4 MiB chunks or more).

**Ceph's rule for delta**, `d + m < k − d`, counting the parity reads: at one data chunk it is
reconstruct-write at both 2+1 and 4+2, and the parity reads are local to each parity holder, inside
the stage's round, where the writer's reads are a round of their own.

**The coordinator folds the change and stages the result.** It reads `m` parity ranges across the
network the holders read where they lie, and X1's model checked the holder's fold.

**Guessing a rowless erasure coded stripe's length from the chunks listed.** The largest share
listed is a lower bound and nothing more; an answer that is wrong loses bytes silently.

**A summary as long as a unit.** X12 found a 4 KiB block costs about what a unit does to fold, and it
is sixteen times less to carry and to encode.

## Limitations

- ~~**A short chunk still takes its slice's whole file.** A whole chunk is written into a file of the
  slice's pool written ahead at the chunk's full size, so a stripe of 20,000 bytes at 4+2 stores three
  files of 1 MiB and a block; no padding is stored in a chunk's length or its checksums, but the
  device spends the file. A replicated short stripe does the same
  ([todo](../appendix/todos.md#a-short-chunk-in-a-file-of-its-own-length)).~~ Lifted by
  [F96](chunk-file-classes.md): a chunk's file is the class of units it holds, so a stripe of 20,000
  bytes at 4+2 stores three files of one 64 KiB unit and a block, and a write that takes a chunk past
  its class rewrites the stripe whole (`plan_write`).
- **An erasure coded position is rebuilt whole**, however little of it a write missed, as the model
  has it ([O115](../appendix/optimizations.md#o115-an-erasure-coded-position-is-rebuilt-whole-for-one-missed-unit)).
- **A rebuild decodes one position at a time.** Two positions of one stripe missed are each read `k`
  times over ([todo](../appendix/todos.md#rebuild-every-stale-position-of-a-stripe-in-one-decode)).
- **No small write in its commit** on an erasure coded pool, by the user's decision.
- **At 2+1 by host on the lab, no write is taken with a host down** at `f = 1`: `k + f` is the whole
  width. Wider layouts run under a device failure domain or in the fixture, as M18 said.
- **A write's stall beside moves**, X1's open item, is a progress bound and no clause: the search of
  the group at 2+1 and 4+2 found one and four stalls in 24,000 runs each with the product's settings,
  every one a failed device and its position's move in flight, the writes refused until the move
  switched, and committed after ([X1](../object-storage/stripe-model.md#what-it-does-not-settle)).
- **A pool not yet writable below wire 12 says so on a write**, and not in its readiness.
- ~~**A failed device keeps taking puts in a placement group that never held a stripe**, which places at
  its pool's pin, where the device still stands, and nothing moves it off. A replicated put counts the
  position missed; at 2+1, whose `k + f` is its width, the put is refused `NotReady`: nine of ten
  runs of `stripes` on the lab after an operator failed a device
  ([item 250](../appendix/resolved/empty-groups-move-without-a-slot.md)).~~ ✅ Since [Resolved #250, the rest](../appendix/resolved/empty-groups-move-without-a-slot.md)
  such a group moves off the failed device at once with no slot, so only a put inside its first
  driver's pass meets it, and is tried again.
- **An object shorter than a chunk is read from one host.** Its bytes are data chunk zero's, and the
  rule's lowest score, which a heavier device draws more often, takes position zero: on the lab
  every read of a 1 MiB object at 2+2 and 3+2 came off europa's devices
  ([O116](../appendix/optimizations.md#o116-an-erasure-coded-stripes-first-data-chunk-favours-the-heaviest-device)).

## Invariants to uphold

- **`matrix_of` never changes.** The code a pool was written with is the code it is read with for as
  long as it lives; `encoding_is_stable_across_builds` holds every layout's digest, and the first
  eight hex digits of each are X4's for ISA-L's C library.
- **Only bytes at the label the row names reach a decoder or a reader** (P10, `take`).
- **A decode takes exactly `k` sources of one range.**
- **A parity holder stages new values, never the change**, and refuses a base it does not hold.
- **A parity stage judges its base under the chunk's lock**, as every stage and read does. An apply
  rewrites the chunk's header and then drops its record under the write lock, and the base check
  reads the header, awaits, then looks for the record: run outside the lock it met neither between
  the two and refused `CannotMake` though the slice held the label it expected, which a write then
  named missed. Found under a stress of the arithmetic test, eight copies at once on one host, where
  it refused one stage in a few hundred; none in the same stress since.
- **A partial write never makes a stale chunk current**: a chunk changed in part is staged over the
  row's label; only a whole chunk is staged with no base, and a position holding no bytes is staged
  whole when a write gives it some.
- **A row is never built from a chunk's length alone for an erasure coded stripe**: a writer makes
  the row before it stages, and a rowless stripe's length is the one its chunks' headers recorded.
- **A commit changes the whole chunk at the positions it staged, and no other.**
- **Every arithmetic step runs on an object executor, in steps of at most 64 KiB of input.**
- **A parity mismatch is never repaired by a rule.**

## Performance

### In the fixture

Every erasure coded fixture row runs in seconds to two and a half minutes on europa beside the suite's
others, a move of every placement group of a failed device by decode included. The parity check of a
2+1 pool's every group runs in a deep pass the fixture waits up to three minutes for.

### On the lab

Taken at `8032b68` from a clean worktree (`target/lab/f90/bench.sh`), on side clusters copied from
`tmdb_cluster.yaml` that differ only in their pool, `--stop-unit shoal-tmdb`: europa (Ryzen 9
7945HX, Zen4), titan and hyperion (Ryzen Embedded V1756B, Zen1), the performance governor, 1 GbE.
Each host has two devices: europa's Optane and an XFS image on it through a loop device, titan's and
hyperion's two XFS volumes on a 970 EVO. Five pools: `r3`, three copies by host (the lab's own, its
small writes riding their commits); `ec21`, 2+1 by host, the one code the three hosts can hold by
host; and 2+2, 3+2 (a 3 MiB stripe) and 4+2 by device over the six devices. The driver runs on europa
over 64 objects (`shoal-spike/results/m16-objects`), two in flight. Each arm ran three times, sixty
seconds measured, ninety for an event arm, and each cell is the median of its runs with their range.
The node and the loader are the `znver1` build every node runs. The captures are under
`target/lab/f90/wt/target/shoaladm-bench/`; `target/lab/f90/summarise.py` reads them, and each member's
codec figures were sampled from `stats --json` every ten seconds beside them.

**The code on Zen1.** The six `codec` tests, `encoding_is_stable_across_builds` among them, passed on
titan and hyperion from a `znver1` build (`avx2`, no GFNI) as on europa (`avx2_gfni`): every layout's
parity is the same bytes on both microarchitectures.

**Whole objects.** A code moves fewer bytes than three copies, and on 1 GbE that is the put's rate:

| Pool | put 1 MiB | put 4 MiB | get 1 MiB | get 4 MiB |
| --- | --- | --- | --- | --- |
| `r3` | 24 [23–24] /s, p50 88 ms | 14 [14–14] /s, p50 142 ms | 136 [125–177] /s | 65 [42–74] /s |
| 2+1 by host | 38 [37–39] /s, p50 48 ms | 19 [19–20] /s, p50 102 ms | 168 [96–180] /s | 29 [25–34] /s |
| 4+2 by device | 30 [28–30] /s, p50 60 ms | 17 [17–19] /s, p50 106 ms | 110 [105–122] /s | 30 [29–32] /s |

A get's rate turns on whether the chunk it reads is on europa, the driver's host: a 1 MiB get at
2+1 had a p50 of 4 to 25 ms by run. A 4 MiB get of a code gathers `k` chunks where a copy is read
whole from one holder, and ran at half of `r3`'s.

**The code inside a put.** Each node's figures over the layouts' runs, input bytes over the time the
code took:

| | europa (`avx2_gfni`) | titan, hyperion (`avx2`) |
| --- | --- | --- |
| encode, 2+1 (XOR) | 58.9 GiB/s | 8.1 GiB/s |
| encode, 4+2 | 58.6 GiB/s | 5.6 GiB/s |
| decode, 4+2 (degraded reads) | 40.9 GiB/s | 8.3 GiB/s (hyperion) |
| fold, 4+2 (parity deltas) | 47.8 GiB/s | 8.6, 8.8 GiB/s |

A Zen1 core encodes 4+2 inside a put at 5.6 GiB/s, below [X4](../object-storage/erasure-coding-crates.md)'s
7.6 GiB/s for the same crate on titan from bytes out of cache and 9.9 from bytes in it; the steps of
64 KiB, each timed and followed by a yield, are the difference X4's loop does not pay, and it is not
taken apart further here. Either is about fifty times what a 1 GbE link brings a node.

**Writes in place**, into a 1 MiB object, 4 KiB and 32 KiB a write:

| Pool | 4 KiB | 32 KiB |
| --- | --- | --- |
| `r3` (in its commit) | 71 [71–81] /s, p50 21 ms, p99 116 ms | 64 [63–73] /s, p50 27 ms, p99 101 ms |
| 2+1 by host (reconstruct-write) | 132 [108–147] /s, p50 14 ms, p99 44 ms | 119 [116–126] /s, p50 14 ms, p99 55 ms |
| 4+2 by device (parity delta) | 106 [92–116] /s, p50 17 ms, p99 52 ms | 85 [80–97] /s, p50 18 ms, p99 65 ms |

Neither code is slower than the copies here, but the comparison flatters it: a 1 MiB object fills
only data chunk zero, so 2+1's reconstruct-write reads nothing remote (its other data chunk is
empty) and 4+2's delta reads one range; a whole stripe's partial write would show the read round.
Every 4 KiB delta folds a whole 64 KiB unit at each parity holder, 2.2 GB folded a Zen1 host over
the 4+2 runs.

**Degraded reads**, 1 MiB objects, titan killed a third of the way in and started again two thirds
in. Every kill stopped every read: none completed for six to eight seconds after it, and those in
flight at it took 8.1 to 9.2 s. That is the tablet groups titan led failing over, as
[F88](recovery-and-moves.md#a-node-killed-and-returned-inside-its-grace) measured on a replicated pool,
and no decode: two in flight both wait on a group titan led. The windows before the kill and after
it, the pause included:

| Pool | before | after |
| --- | --- | --- |
| 2+1 by host | 146 [136–160] /s, p99 38 ms | 124 [124–144] /s, p99 38 ms |
| 2+2 by device | 136 [109–196] /s, p99 36 ms | 180 [114–253] /s, p99 32 ms |
| 3+2 by device | 125 [120–128] /s, p99 38 ms | 119 [109–133] /s, p99 36 ms |
| 4+2 by device | 116 [93–118] /s, p99 43 ms | 89 [87–95] /s, p99 101 ms |

Only 4+2 and 2+1 decoded: 1,672 MiB on europa and 972 MiB on hyperion at 4+2, its p99 two and a half
times higher, and 246 MiB on europa at 2+1. At 2+2 and 3+2 nothing decoded, because nothing needed
to: every read of each one's first "none" run came off europa's devices, 12 and 11 GiB, and titan's and hyperion's read nothing, so data chunk zero
of all 64 objects was on europa ([O116](../appendix/optimizations.md#o116-an-erasure-coded-stripes-first-data-chunk-favours-the-heaviest-device)).

**Operations, on a 2+1 cluster of its own** (`target/lab/f90/ops.sh`) with every placement group's
parity checked a deep pass. `stripes` put twelve posters of 20,000 bytes to 32 MiB through every
member, wrote them in place and read them whole through every member at `One` and `Quorum`.

- *A deep scrub, clean*: 114 chunks verified, 193 MiB, in three seconds; parity checked on 42
  stripes, none mismatched, and nothing stale, corrupt or disagreeing. `pools` names the pool
  `fast ssd 2+1`.
- *Parity that verifies and is wrong*: `f90-plant` rewrote unit 3 of a parity chunk on titan's `/xfs`
  with its checksum bound to the new bytes. The next deep pass counted `parity checked 84
  mismatched 1` and nothing corrupt; europa, which leads the stripe's group, logged the stripe and its
  parity row at ERROR; the chunk's md5 was the same after as before, nothing set aside.
- *A device failed by an operator*: hyperion's `/xfs`, holding 26 chunks, failed with `fail-device`;
  the pool's placement groups moved to the new generation, 2,690 switched in seventy seconds and
  none left stale, and the positions it held were made again by decode: 25, 32 and 38 MB decoded on hyperion, titan and
  europa. A deep pass after verified 342 chunks and found nothing new.
- *Puts after the failure* were refused now and then, and after a second device's failure nine runs
  of ten: a placement group that never held a stripe still places on the failed device, and at 2+1
  one refused stage refuses the put ([item 250](../appendix/resolved/empty-groups-move-without-a-slot.md)).
  Why was found only once a failed stream said so at WARN (`0c3c40e`).

**The tmdb cluster** runs this change (`0c3c40e`): `upgrade --activate` moved it from wire 11 to 12
in place in 24 seconds, one node at a time, the leader last, its pool replicated as before and named
`3 replicas` by `pools`. Every movie, release row and keyword partition then read back at `Quorum`
matching the csv (1,187,691 movies, 1,188,009 release rows and 58,418 keyword partitions), `walk`
found every page held to its rules and every member matching, and `stripes` put nine posters through
every member, wrote them in place and read them back whole. Every host was put back on its resting
governor.

## Tests

| Test | What breaks if it is reverted |
| --- | --- |
| `codec` `every_loss_pattern_decodes` | A loss pattern of a layout from 2 to 12 data chunks and 1 to 4 parity chunks, 8,536 in all, no longer decodes to its chunks |
| `codec` `encoding_is_stable_across_builds` | The crate, the matrix or a kernel set writes other parity than X4 recorded for ISA-L |
| `codec` `one_parity_chunk_is_the_xor_of_the_data` | One parity chunk is no longer the data's XOR, by the kernel or the matrix |
| `codec` `a_fold_sequence_equals_a_fresh_encode` | A holder's fold of a change leaves other parity than a fresh encode |
| `codec` `summaries_check_parity_against_data` | A summary built in pieces differs from one built whole, or a wrong parity byte goes unseen |
| `codec` `copies_are_not_a_code` | A layout of one data chunk or no parity is taken as a code |
| `erasure` `decode_never_mixes_labels` | Bytes at a label the row does not name reach a decoder (P10) |
| `erasure` `a_whole_stripe_is_encoded_once`, `a_partial_write_takes_the_fewer_remote_reads`, `an_empty_chunk_is_held_and_filled_whole` | A write is planned the wrong way: under a floor, by the wrong partial method, or over a chunk that held nothing |
| `chunk` `a_header_records_its_stripes_length` | A header's stripe length is lost, or moves another byte of the frozen format |
| `pools` `a_pools_facts_are_checked` | A pool of one data chunk, past 32 chunks, or of a stripe not `k` chunks of whole units is taken |
| `scrub` `the_parity_sample_rotates` | A group's parity is checked other than once every four passes |
| `device_store` `a_parity_stage_folds_its_change_and_refuses_another_base` | A parity holder stages the change, folds it twice, or folds it over another base |
| `objects` `an_erasure_coded_object_round_trips` | A put, a range, a delta, a reconstruct-write, an append into an empty chunk or a cut reads back wrong, or leaves parity unlike its data |
| `objects` `short_stripe_stores_no_padding` | A short stripe stores a chunk for a data position its bytes do not reach, or one longer than its share |
| `objects` `range_read_touches_only_the_chunks_it_needs` | A healthy range reads another slice, more than its length, or decodes |
| `objects` `unreadable_stripe_fails_alone` | A stripe short of `k` reads garbage, fails without its name, or fails the object's other stripes |
| `objects` `partial_overwrite_equals_a_fresh_encode` | Writes in place at 4+2 and at 3+1 leave parity other than a fresh encode. Its node waits two minutes for a holder, not ten seconds: a stage refused or answered past the deadline is named missed, rightly, and a standalone node rebuilds nothing, so the test reads that chunk stale. The workspace run met that once, under heavy I/O, by a cause not established: the parity stage's race above, a late flush, or [item 249](../appendix/resolved/an-executor-parked-with-work.md), an object executor that went to sleep with its tasks queued, since fixed in the glommio fork |
| `objects` `parity_stage_refuses_a_base_it_does_not_hold` | A parity holder at an older label takes a change, or a write short of `k + f` is acknowledged |
| `cluster_fixture` `an_erasure_coded_object_round_trips_across_nodes` | A 2+1 object put and written through different nodes reads back wrong on any node |
| `cluster_fixture` `degraded_read_decodes_only_what_it_needs` | A degraded read decodes or reads more than its range |
| `cluster_fixture` `erasure_coded_ack_requires_k_plus_f_current` | A write is acknowledged with fewer than `k + f` positions current, or its bytes do not survive its device (P11) |
| `cluster_fixture` `parity_check_finds_wrong_parity_that_verifies` | Parity that verifies and is wrong goes unseen, or is changed by a rule |
| `cluster_fixture` `an_erasure_coded_position_rebuilds_by_decode` | A position a write missed is not rebuilt by decode, or its parity is wrong after |
| `cluster_fixture` `an_erasure_coded_move_decodes_what_its_old_holder_lost` | A move copies another position's chunk, or cannot move a position whose old holder is gone |
| `cluster_fixture` `an_erasure_coded_write_is_atomic_at_every_crash_point` | A delta write at a crash point leaves bytes of two states, or a history no single copy could have produced |
| `stripe_model_conformance` `stripe_rows_follow_the_model`, `the_missed_record_follows_the_model` | An erasure coded commit's changed positions differ from the model's |
| `peer::chunk` `every_request_round_trips`, `every_answer_round_trips` | A parity stage, a summarised verification or a stripe's length does not survive the lane |

## Related

[M18](../object-storage/milestones.md#m18-erasure-coding) for the gate;
[S8](../object-storage/erasure-coding.md) for the design;
[X4](../object-storage/erasure-coding-crates.md) for the code and the crate;
[X1](../object-storage/stripe-model.md) for the model the write path keeps to;
[S18's record](../object-storage/contract.md#m18-erasure-coding-2026-10-15) for the decisions;
[F86](replicated-pools.md), [F88](recovery-and-moves.md) and [F89](scrub-and-repair.md) for the paths
this gate extends.
