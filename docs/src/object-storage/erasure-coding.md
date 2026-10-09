# S8. Erasure coding

## Context

A pool's redundancy is copies or an erasure code (R11). Copies need nothing from this page:
a replicated stripe's chunks are identical copies, and every rule of [S7](write-path.md) applies
with `k = 1`. An erasure code is where two of the decisions already taken become expensive.
Objects are **seekable**, so a read of a few bytes should not cost a decode; and they are
**mutable in place**, so a write of a few bytes should not cost a stripe.

Both costs are set by properties of the code and by the geometry it is laid out in, more than
by how fast any one crate multiplies. That is why
[X4](spikes.md#x4-erasure-coding-crates-performance-and-tradeoffs) compares families of code
and not only Reed-Solomon crates, and why this page states the properties before it names a
candidate.

## What exists today

~~Nothing.~~ No erasure code in the engine, no Galois field arithmetic and no such crate in
the workspace's `Cargo.lock`: M18 adds one. What there is since 2026-10-03 is
[X4's record](erasure-coding-crates.md), with the crate chosen, every candidate's properties and
its speed on the lab, measured by a harness outside the workspace (`shoal-spike-erasure/`). What
an implementation would have to live with is all elsewhere in the tree:

- **One executor a core, and no thread pool.** A shard runs everything on its own thread
  ([Thread per Core](../architecture/thread-per-core.md)). A crate that hands work to a
  thread pool of its own is working against that.
- **Nodes are built for the oldest CPU in the deployment.** The workspace builds for
  `target-cpu=native` (`.cargo/config.toml`), and a native build from europa "dies of SIGILL
  on the Zen1 hosts", so the lab's nodes are built `znver1` (`CLAUDE.md`). A crate that picks
  its SIMD at compile time gets Zen1's instructions everywhere; one that picks at run time
  can use what europa has and titan lacks. X4 measured what that is worth: the crate chosen
  finds GFNI from a `znver1` build and encodes 4+2 in cache at 97 GiB/s on europa, against 50
  when held to AVX2 ([by target](erasure-coding-crates.md#by-target)).
- **Direct I/O wants aligned buffers** that the caller owns (`alloc_dma_buffer`,
  `glommio/src/io/dma_file.rs:269`). A crate that returns a fresh `Vec` for each stripe chunk
  costs a copy into one.

## The design

### Geometry

```
one stripe, 4+2: six stripe chunks, each a run of chunk units

               unit 0   unit 1   unit 2  ...  unit N-1
chunk 0 data  │  u   │   u    │   u    │ ... │   u    │   bytes [0, S/4) of the stripe
chunk 1 data  │  u   │   u    │   u    │ ... │   u    │   bytes [S/4, S/2)
chunk 2 data  │  u   │   u    │   u    │ ... │   u    │
chunk 3 data  │  u   │   u    │   u    │ ... │   u    │
chunk 4 parity│  p   │   p    │   p    │ ... │   p    │
chunk 5 parity│  p   │   p    │   p    │ ... │   p    │
                 ▲
                 └── unit row 0: unit 0 of every chunk, encoded together; any 4 of the 6 decode it

replicated, replicas: 3: three stripe chunks, each a whole copy of the stripe, and no unit rows
```

A stripe of `S` bytes is `k` **data chunks** and `m` **parity chunks**: its bytes are cut
into the data chunks, and the parity chunks are computed from them. Each stripe chunk is a
run of **chunk units** of one size. The `i`-th unit of every chunk forms one **unit row**:
the units that are encoded together, and what a decode needs `k` of. Coding theory calls a
unit row a codeword, and Ceph's erasure coding calls it a stripe, which here is the whole
thing. The diagram is the stripe half of [the overview's](overview.md#the-hierarchy), which
shows where each chunk lands.

Two sizes are chosen, and both are [Q20](contract.md#questions-to-answer)'s:

| Size | Smaller | Larger |
| --- | --- | --- |
| **Stripe**, a pool's setting | More stripes an object, so more rows once written in place, and more placement spread | Larger chunks: fewer files, longer sequential runs on a disk, a dearer rebuild of one |
| **Chunk unit** | A small read verifies less, a small write stages less. More checksums, and a short unit row is slow to encode | A small read reads more to verify. Fewer checksums, and encoding runs at its best |

Ceph's stripe unit is what this page calls a chunk unit. Its default is 4 KiB, and its own
documentation recommends raising it: "For the
majority of I/O workloads it is recommended to increase the stripe unit to at least 16K when
using optimizations", and up to 256 KiB for loads that mostly read
(`doc/rados/operations/erasure-code.rst` at `v20.2.0`). That is advice only: the default is
`osd_pool_erasure_code_stripe_unit`, 4 KiB, for every pool, optimized or not, and it is fixed
when the pool is made (`src/common/options/mon.yaml.in:16-27`, `src/mon/OSDMonitor.cc:7836-7844`;
[X14](ceph-and-s3-sources.md#3-partial-writes-and-the-shard-versions)).

**How data is laid across the data chunks** is a third choice. Above, each data chunk holds
one contiguous quarter of the stripe, so a small read touches one holder and a long one
reads each holder in one run. Ceph deals units round-robin, so a long read fans over every
holder at once: unit `i` of a stripe is data shard `i` (`src/osd/ECUtil.h:632-643` at
`v20.2.0`), and a short read reads only the shards that hold it (`osd_ec_partial_reads`, on by
default). The first suits a seek and a rotational disk; the second parallelises a
stream. It is left to ~~X4 and~~ the read arms of [S15](performance.md): X4 times a code over
whole unit rows, and either way of dealing bytes encodes the same rows, so it could not tell
them apart.

### What the code has to be

| Property | What it buys | What its absence costs |
| --- | --- | --- |
| **Systematic**: the data chunks hold the data as written | A healthy read decodes nothing and touches only the stripe chunks its range falls in | Every read fetches `k` chunks and decodes |
| **Decodable from any `k` chunks** | [P11](contract.md#the-contract) can be stated: `k + f` current chunks survive any `f` losses | A set of `k` chunks may fail to decode, and durability is a probability |
| **An update form**: new parity from old parity and the change | A small write touches the data chunk it lands in and the parity chunks, `1 + m` | Every small write rewrites every chunk, `k + m`, or re-reads the stripe |

A Reed-Solomon code laid out systematically has all three. The preferred direction is a
code that does. It is a preference and not a selection, because the user asked for the
tradeoffs to be measured, and one candidate is chosen specifically for not having them:

**Random linear network coding**, the `rlnc` crate. As its source stands at `0.8.7`, every
coded chunk is a random combination of the originals with its coefficients attached; the
caller supplies an RNG and cannot supply the coefficients, so there is no systematic chunk
to ask for; and a decoder is fed chunks until it has `k` that are independent, refusing one
that is not as `PieceNotUseful` ([S18](contract.md#decision-record)). What it offers that
the others do not is **recoding**: a new coded chunk made from coded chunks, with no decode.
Whether that makes a rebuild cheaper when a slice holds one chunk of a stripe, which is
this design's layout, is a thing to measure and not to assume.

### A partial overwrite

A write that changes part of a stripe touches `d` data chunks and all `m` parity chunks.
There are two ways to produce the new parity.

| | Parity delta | Reconstruct-write |
| --- | --- | --- |
| Reads | The old bytes of the range, from each touched data chunk; each parity holder reads its own old parity | The untouched data units of the affected unit rows, from their holders |
| Computes | The change, old XOR new; each parity holder folds the change into its parity | New parity, encoded afresh from all `k` data units |
| Best when | Few data chunks are touched | Most of the unit row is being written anyway |
| Needs | An update form in the code, and every touched chunk current | Any `k` current chunks |

Ceph's design for its Tentacle release describes the first in these words: it "is
implemented by reading the old data that is about to be overwritten and XORing it with the
new data to create a delta. The coding parities can then be read, updated to apply the delta
and re-written. With M=2 (RAID6) this can result in just 3 read and 3 writes to perform an
overwrite of less than one chunk." And of which codes can: "Not all erasure codings and
erasure coding libraries support the capability of performing delta updates, however those
implemented using XOR and/or GF arithmetic should"
(`doc/dev/osd_internals/erasure_coding/enhancements.rst` at `v20.2.0`, a design document).

Four rules make either way safe under [S7](write-path.md):

- **A parity holder stages new parity, never the change.** The change is what travels; what
  is staged is the result ([S6](device-store.md#staging-two-cases)).
- **A stage names the label it expects.** A parity holder whose parity is not at the label
  the writer read refuses, and the write falls back to reconstruct-write or fails.
- **A unit is verified before it is merged into.** A write of part of a unit reads the old
  unit; if it fails its checksum, the write stops and the chunk is treated as corrupt.
  Merging and checksumming afresh would launder it ([P15](contract.md#the-contract)).
- **A partial write never makes a stale chunk current.** Only a write of the whole chunk
  does.

**The untouched chunks are not touched.** A write to one data chunk of a 4+2 stripe stages
on three slices and leaves three alone, and the `StripeMeta` row's label for each untouched
chunk stays as it was. That is why the row keeps a label for every chunk and not one for the
stripe.
Ceph reached the same place from the other side: its design keeps "a vector of version
numbers" for each object and requires that "the log entry needs to be modified to include
the set of shards that are being updated" (same document; a shard there is a chunk's holder
here, a slice). There the vector lives with the holders and peering reconciles it; here it is one
replicated row. The code shipped it as two records
([X14](ceph-and-s3-sources.md#3-partial-writes-and-the-shard-versions)):

- each log entry names the shards it wrote (`written_shards`, `src/osd/osd_types.h:4510`);
- the object's info keeps the version each untouched data shard was left at (`shard_versions`,
  `:6263`), on the shards that can become primary: data shard 0 and the parity.

A per-PG summary of which writes each shard skipped is merged at peering
(`partial_writes_last_complete`, `:3061-3062`; `src/osd/PeeringState.cc:366-435`).

### The write hole

Parity that does not match its data is the failure erasure coding is known for. It can open
in two places.

- **Across chunks**, when a write reaches some of a stripe's chunks and not others. It is
  closed by staging every touched chunk before the commit, by the commit being the only
  thing that makes any of them current, and by labels: a decode takes only chunks whose
  labels the row names together ([P10](contract.md#the-contract)).
- **Inside one chunk**, when an apply in place is cut short. It is closed by the staged
  copy outliving the apply and by the unit's checksum ([S6](device-store.md)).

### Whole stripes, and short ones

A write of a whole stripe encodes once and stages every stripe chunk as a whole chunk,
written beside the old and renamed. It is the cheap case, and it heals: any chunk it writes
is current afterwards, whatever its slice had missed.

The last stripe of an object is usually short. Its data chunks hold what there is, a parity
chunk is as long as the longest data chunk, and what is absent is zeros that are never
stored. A small object in a 4+2 pool therefore costs three times its size and not six
chunks of padding. Ceph's Tentacle release "eliminates padding which can save capacity" for
the same reason (`doc/rados/operations/erasure-code.rst`). Its optimized pools store each data
shard only as long as its bytes, rounded to 4 KiB, and parity as long as shard 0
(`src/osd/ECUtil.h:614-629` at `v20.2.0`). Its legacy pools pad every shard to a whole stripe.

### Decoding and rebuilding

A degraded read decodes only the units its range needs, from any `k` current stripe chunks.
A rebuild of a lost chunk reads `k`, computes the one, and writes it whole
([S10](recovery.md)). Both are bounded by a core's decode rate and by the slowest of `k`
devices.

### One parity chunk is XOR

With `m = 1` the parity is the XOR of the data, with no field arithmetic at all. That
matters here because three hosts under a host failure domain allow only 2+1, so the first
erasure coded pool the lab can run across hosts needs no crate, and gives the speed ~~nothing
else will beat~~ of the memory: X4 found it the fastest code at one parity chunk on Zen1 (14.5
to 16.9 GiB/s at 4+1 to 10+1 out of cache, against 11.8 to 12.5 for the field code), and on Zen4
level with the field code, both at the rate memory feeds one core
([one parity chunk](erasure-coding-crates.md#one-parity-chunk)).

### Which crate

~~Not chosen.~~ **`rusty_erasure` 0.4.1**, a Rust port of ISA-L's erasure code, on ISA-L's
Cauchy matrix, chosen by [X4](erasure-coding-crates.md) and recorded on
[S18](contract.md#q20-in-part-the-code-and-the-crate-2026-10-03). It has all three properties
above, an update in its public API, writes into the caller's buffers, picks its SIMD at run
time, and was the fastest candidate at every operation on both of the lab's
microarchitectures. Its parity is byte for byte ISA-L's, so what it writes to a device is
defined by ISA-L's matrix and not by this crate's release. The candidates were pinned on
[S18](contract.md#decision-record), and three things were learnt by reading their sources that
their READMEs did not say:

- `reed-solomon-erasure`'s shard-by-shard encoding must run "in strict sequential order", so
  it is an incremental encode and not an update of one shard;
- the `isa-l` crate binds no incremental update, while `rusty_erasure`, a port of the same
  library, has `ec_encode_data_update`;
- `raptorq` sizes a packet with a `u16`, so its symbols stop at 64 KiB.

X4 learnt more by running them, every one on its page: RaptorQ and random linear network
coding both fail some sets of k chunks (4 of 1,001 patterns of 10+4, and 0.39% of k-sets), the
`isa-l` crate cannot build the ISA-L it bundles with any `pkg-config` released since 2022, and
`rlnc`'s recoder reaches undefined behaviour on a short buffer.

## Alternatives rejected

**A code that is not systematic, as the default.** It makes every read a decode. It is
measured, as the user asked, and would have to be several times faster to pay for that.

**Encoding whole objects.** A decode would need the object, a write would rewrite it, and an
object of any size would have to fit `k + m` devices.

**Replicate first and encode later**, as warm storage tiers do. It gives a fast write and a
cheap rest, at the price of two layouts for one object and a migration between them. It is
tiering, which [the overview](overview.md#what-this-part-is-not) leaves out.

**Codes that rebuild from fewer chunks**, locally repairable or regenerating. They trade
more parity for a cheaper rebuild. Nothing here precludes one later, since a pool's
redundancy is a pool's setting.

**The client encodes.** It saves a node's CPU and a network crossing and needs the client
to reach every holder, which is [D7](../direction/shard-aware-routing.md).

## What it costs

- **CPU on every write** and on every degraded read and rebuild. ~~What a Zen1 core encodes in
  a second is not known, and is X4's first number.~~ A Zen1 core encodes 4+2 at 7.6 GiB/s of
  data with its bytes out of cache and 9.9 in it; a Zen4 core at 20 and 97
  ([X4](erasure-coding-crates.md#by-target)). A degraded read of 4+2 with two chunks lost costs
  the same, and a rebuild of one 64 KiB chunk takes about 20 µs on titan out of cache.
- **A partial write is reads and then writes.** With one data chunk touched and `m = 2`:
  three reads and three stages, where a replicated write of the same bytes is three stages
  and no read.
- **Space**: `(k + m) / k`, a header a stripe chunk, a checksum a unit.
- **A rebuild reads `k` chunks to make one**, which on the lab's network is where its time
  goes ([X12](spikes.md#x12-recovery-and-scrub-rates)). ✅ X12 measured it: across 1 GbE a 2+1 and a
  4+2 rebuild reached 0.95 and 0.96 of 112 MiB/s ÷ k, and on one device a decode cost what its
  reads cost, the device's rate ÷ (k + 1) ([X12](recovery-scrub-rates.md#7-a-rebuild-across-hosts)).

## What it breaks

- "Redundancy is copies of the same bytes": the chunks of a stripe differ, so comparing two
  of them proves nothing, which changes what a scrub can do ([S11](scrub.md)).
- "The workspace has one dependency whose output is persisted", gxhash: an erasure code is a
  second, and since [X5](checksums.md) the chunk unit's checksum, CRC-64/NVME, a third.
  That one is pinned by its definition, six parameters and a check value, and not by its
  crate. Its output is on disk for as long as a pool lives, so a crate whose encoding
  changed between releases would be a format break. X4's answer is a definition outside the
  crate: the chosen crate's parity equals ISA-L's C library's at every layout measured, and its
  digests were the same on Zen1 and Zen4 and from three builds. The crate is pinned exactly at
  M18, as gxhash is, and a change of it is judged against ISA-L's bytes.

## Invariants to uphold

- A unit row is encoded, decoded and rebuilt only from units whose chunks' labels the
  `StripeMeta` row names together.
- A stripe chunk's position fixes its role, data or parity, for the life of the stripe.
- A parity holder stages new parity. A change is never staged.
- A stage of part of a chunk is refused by a holder not at the label it expects.
- A unit that fails its checksum is never merged into and never a source.
- No call into the code runs past the executor's latency budget without yielding
  ([S13](isolation.md)).
- The encoding a pool was written with is the encoding it is read with, for ever.

## Prerequisites

~~An erasure coding crate, chosen by X4~~ ✅ An erasure coding crate: `rusty_erasure`, chosen by
[X4](erasure-coding-crates.md) ([S1](prerequisites.md#dependencies-to-choose)).
[S6](device-store.md) and [S7](write-path.md), which this page only adds a kind of chunk to.

## How it would be measured

[X4](spikes.md#x4-erasure-coding-crates-performance-and-tradeoffs) records, for every
candidate, the properties in the table above and its speed a core: encode, decode at one to
`m` losses, update and recode, at 2+1 through 10+4 and units from 4 KiB to 1 MiB, on titan
and on europa, from a `znver1` build. Correctness comes first: every loss pattern of the
small layouts decodes, and for a random code the fraction of sets of `k` that fail is
counted. **It has**, on 2026-10-03: [its record](erasure-coding-crates.md).

In a running cluster the cost shows in three arms of [S15](performance.md): a write of part
of a stripe against the same write to a replicated pool, a read with a holder down against
one with none, and a rebuild beside a foreground load.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `every_loss_pattern_decodes` | For each supported k+m up to a bound, every set of `m` lost chunks is recovered | M18 |
| `decode_never_mixes_labels` | A decode offered chunks under two labels uses one label's or fails by name | M18 |
| `partial_overwrite_equals_a_fresh_encode` | After any sequence of partial writes, each parity chunk equals what encoding the data chunks afresh gives | M18 |
| `parity_stage_refuses_a_base_it_does_not_hold` | A parity holder at another label refuses the stage, and the write falls back or fails | M18 |
| `short_stripe_stores_no_padding` | A stripe shorter than a unit stores `1 + m` chunks of its own length | M18 |
| `encoding_is_stable_across_builds` | Fixed inputs encode to fixed bytes, pinned by literal, on Zen1 and Zen4 builds alike | M18 |
| `erasure_coded_ack_requires_k_plus_f_current` | A write touching one data chunk of a k+m stripe is acknowledged only when `k + f` chunks are current after it, and is read back with the touched device lost | M18 |

## Related

[S7](write-path.md) for the protocol a partial overwrite runs under; [S6](device-store.md)
for staging; [S9](read-path.md) for degraded reads; [S10](recovery.md) for rebuilds;
[S11](scrub.md) for checking parity; [S18](contract.md#decision-record) for the candidates;
[S17](prior-art.md#ceph) for Ceph's erasure coding.
