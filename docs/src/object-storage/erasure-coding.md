# S8. Erasure coding

## Context

A pool's redundancy is copies or an erasure code (R11). Copies need nothing from this page:
a replicated stripe's pieces are identical, and every rule of [S7](write-path.md) applies
with `k = 1`. An erasure code is where two of the decisions already taken become expensive.
Objects are **seekable**, so a read of a few bytes should not cost a decode; and they are
**mutable in place**, so a write of a few bytes should not cost a stripe.

Both costs are set by properties of the code and by the geometry it is laid out in, more than
by how fast any one crate multiplies. That is why
[X4](spikes.md#x4-erasure-coding-crates-performance-and-tradeoffs) compares families of code
and not only Reed-Solomon crates, and why this page states the properties before it names a
candidate.

## What exists today

Nothing. There is no erasure code, no Galois field arithmetic and no such crate in
`Cargo.lock`. What an implementation would have to live with is all elsewhere in the tree:

- **One executor a core, and no thread pool.** A shard runs everything on its own thread
  ([Thread per Core](../architecture/thread-per-core.md)). A crate that hands work to a
  thread pool of its own is working against that.
- **Nodes are built for the oldest CPU in the deployment.** The workspace builds for
  `target-cpu=native` (`.cargo/config.toml`), and a native build from europa "dies of SIGILL
  on the Zen1 hosts", so the lab's nodes are built `znver1` (`CLAUDE.md`). A crate that picks
  its SIMD at compile time gets Zen1's instructions everywhere; one that picks at run time
  can use what europa has and titan lacks.
- **Direct I/O wants aligned buffers** that the caller owns (`alloc_dma_buffer`,
  `glommio/src/io/dma_file.rs:269`). A crate that returns a fresh `Vec` for each piece costs
  a copy into one.

## The design

### Geometry

```
one stripe of S bytes, 4+2, stripe unit u

data piece 0   │ u │ u │ u │ ... │     bytes [0, S/4) of the stripe
data piece 1   │ u │ u │ u │ ... │     bytes [S/4, S/2)
data piece 2   │ u │ u │ u │ ... │
data piece 3   │ u │ u │ u │ ... │
parity piece 0 │ p │ p │ p │ ... │
parity piece 1 │ p │ p │ p │ ... │
                 ▲
                 └── one codeword: the i-th unit of every piece, encoded together
```

A stripe's bytes are cut into `k` data pieces, and `m` parity pieces are computed from them.
Each piece is a run of stripe units. The `i`-th unit of every piece form one **codeword**:
they are what is encoded together, and what a decode needs `k` of. (Ceph's erasure coding
calls that a stripe; here a stripe is the larger thing.)

Two sizes are chosen, and both are [Q20](contract.md#questions-to-answer)'s:

| Size | Smaller | Larger |
| --- | --- | --- |
| **Stripe**, a pool's setting | More stripes an object, so more rows once written in place, and more placement spread | Larger pieces: fewer files, longer sequential runs on a disk, a dearer rebuild of one |
| **Stripe unit** | A small read verifies less, a small write stages less. More checksums, and a short codeword is slow to encode | A small read reads more to verify. Fewer checksums, and encoding runs at its best |

Ceph's default unit is 4 KiB, and its own documentation recommends raising it: "For the
majority of I/O workloads it is recommended to increase the stripe unit to at least 16K when
using optimizations", and up to 256 KiB for loads that mostly read
(`doc/rados/operations/erasure-code.rst` at `v20.2.0`).

**How data is laid across the data pieces** is a third choice. Above, each data piece holds
one contiguous quarter of the stripe, so a small read touches one device and a long one
reads each device in one run. Ceph deals units round-robin, so a long read fans over every
device at once. The first suits a seek and a rotational disk; the second parallelises a
stream. It is left to X4 and the read arms of [S15](performance.md).

### What the code has to be

| Property | What it buys | What its absence costs |
| --- | --- | --- |
| **Systematic**: the data pieces hold the data as written | A healthy read decodes nothing and touches only the pieces its range falls in | Every read fetches `k` pieces and decodes |
| **Decodable from any `k` pieces** | [P11](contract.md#the-contract) can be stated: `k + f` current pieces survive any `f` losses | A set of `k` pieces may fail to decode, and durability is a probability |
| **An update form**: new parity from old parity and the change | A small write touches the data piece it lands in and the parity pieces, `1 + m` | Every small write rewrites every piece, `k + m`, or re-reads the stripe |

A Reed-Solomon code laid out systematically has all three. The preferred direction is a
code that does. It is a preference and not a selection, because the user asked for the
tradeoffs to be measured, and one candidate is chosen specifically for not having them:

**Random linear network coding**, the `rlnc` crate. As its source stands at `0.8.7`, every
coded piece is a random combination of the originals with its coefficients attached; the
caller supplies an RNG and cannot supply the coefficients, so there is no systematic piece
to ask for; and a decoder is fed pieces until it has `k` that are independent, refusing one
that is not as `PieceNotUseful` ([S18](contract.md#decision-record)). What it offers that
the others do not is **recoding**: a new coded piece made from coded pieces, with no decode.
Whether that makes a rebuild cheaper when a device holds one piece of a stripe, which is
this design's layout, is a thing to measure and not to assume.

### A partial overwrite

A write that changes part of a stripe touches `d` data pieces and all `m` parity pieces.
There are two ways to produce the new parity.

| | Parity delta | Reconstruct-write |
| --- | --- | --- |
| Reads | The old bytes of the range, from each touched data piece; each parity holder reads its own old parity | The untouched data units of the affected codewords, from their holders |
| Computes | The change, old XOR new; each parity holder folds the change into its parity | New parity, encoded afresh from all `k` data units |
| Best when | Few data pieces are touched | Most of the codeword is being written anyway |
| Needs | An update form in the code, and every touched piece current | Any `k` current pieces |

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
  unit; if it fails its checksum, the write stops and the piece is treated as corrupt.
  Merging and checksumming afresh would launder it ([P15](contract.md#the-contract)).
- **A partial write never makes a stale piece current.** Only a write of the whole piece
  does.

**The untouched pieces are not touched.** A write to one data piece of a 4+2 stripe stages
on three devices and leaves three alone, and the row's label for each untouched piece stays
as it was. That is why the row keeps a label for every piece and not one for the stripe.
Ceph reached the same place from the other side: its design keeps "a vector of version
numbers" for each object and requires that "the log entry needs to be modified to include
the set of shards that are being updated" (same document; a shard there is a piece's holder
here). There the vector lives with the holders and peering reconciles it; here it is one
replicated row.

### The write hole

Parity that does not match its data is the failure erasure coding is known for. It can open
in two places.

- **Across pieces**, when a write reaches some of a codeword's pieces and not others. It is
  closed by staging every touched piece before the commit, by the commit being the only
  thing that makes any of them current, and by labels: a decode takes only pieces whose
  labels the row names together ([P10](contract.md#the-contract)).
- **Inside one piece**, when an apply in place is cut short. It is closed by the staged
  copy outliving the apply and by the unit's checksum ([S6](device-store.md)).

### Whole stripes, and short ones

A write of a whole stripe encodes once and stages every piece as a whole piece, written
beside the old and renamed. It is the cheap case, and it heals: any piece it writes is
current afterwards, whatever that device had missed.

The last stripe of an object is usually short. Its data pieces hold what there is, a parity
piece is as long as the longest data piece, and what is absent is zeros that are never
stored. A small object in a 4+2 pool therefore costs three times its size and not six
pieces of padding. Ceph's Tentacle release "eliminates padding which can save capacity" for
the same reason (`doc/rados/operations/erasure-code.rst`).

### Decoding and rebuilding

A degraded read decodes only the units its range needs, from any `k` current pieces. A
rebuild of a lost piece reads `k`, computes the one, and writes it whole
([S10](recovery.md)). Both are bounded by a core's decode rate and by the slowest of `k`
devices.

### One parity piece is XOR

With `m = 1` the parity is the XOR of the data, with no field arithmetic at all. That
matters here because three hosts under a host failure domain allow only 2+1, so the first
erasure coded pool the lab can run across hosts needs no crate, and gives the speed nothing
else will beat.

### Which crate

Not chosen. The candidates are pinned on [S18](contract.md#decision-record), and three
things were learnt by reading their sources that their READMEs did not say:

- `reed-solomon-erasure`'s shard-by-shard encoding must run "in strict sequential order", so
  it is an incremental encode and not an update of one shard;
- the `isa-l` crate binds no incremental update, while `rusty_erasure`, a port of the same
  library, has `ec_encode_data_update`;
- `raptorq` sizes a packet with a `u16`, so its symbols stop at 64 KiB.

## Alternatives rejected

**A code that is not systematic, as the default.** It makes every read a decode. It is
measured, as the user asked, and would have to be several times faster to pay for that.

**Encoding whole objects.** A decode would need the object, a write would rewrite it, and an
object of any size would have to fit `k + m` devices.

**Replicate first and encode later**, as warm storage tiers do. It gives a fast write and a
cheap rest, at the price of two layouts for one object and a migration between them. It is
tiering, which [the overview](overview.md#what-this-part-is-not) leaves out.

**Codes that rebuild from fewer pieces**, locally repairable or regenerating. They trade
more parity for a cheaper rebuild. Nothing here precludes one later, since a pool's
redundancy is a pool's setting.

**The client encodes.** It saves a node's CPU and a network crossing and needs the client
to reach every holder, which is [D7](../direction/shard-aware-routing.md).

## What it costs

- **CPU on every write** and on every degraded read and rebuild. What a Zen1 core encodes in
  a second is not known, and is X4's first number.
- **A partial write is reads and then writes.** With one data piece touched and `m = 2`:
  three reads and three stages, where a replicated write of the same bytes is three stages
  and no read.
- **Space**: `(k + m) / k`, a header a piece, a checksum a unit.
- **A rebuild reads `k` pieces to make one**, which on the lab's network is where its time
  goes ([X12](spikes.md#x12-recovery-and-scrub-rates)).

## What it breaks

- "Redundancy is copies of the same bytes": pieces of a stripe differ, so comparing two of
  them proves nothing, which changes what a scrub can do ([S11](scrub.md)).
- "The workspace has one dependency whose output is persisted", gxhash: an erasure code is a
  second. Its output is on disk for as long as a pool lives, so a crate whose encoding
  changed between releases would be a format break.

## Invariants to uphold

- A codeword is encoded, decoded and rebuilt only from units whose pieces' labels the row
  names together.
- A piece's position fixes its role, data or parity, for the life of the stripe.
- A parity holder stages new parity. A change is never staged.
- A stage of part of a piece is refused by a holder not at the label it expects.
- A unit that fails its checksum is never merged into and never a source.
- No call into the code runs past the executor's latency budget without yielding
  ([S13](isolation.md)).
- The encoding a pool was written with is the encoding it is read with, for ever.

## Prerequisites

An erasure coding crate, chosen by X4 ([S1](prerequisites.md#dependencies-to-choose)).
[S6](device-store.md) and [S7](write-path.md), which this page only adds a kind of piece to.

## How it would be measured

[X4](spikes.md#x4-erasure-coding-crates-performance-and-tradeoffs) records, for every
candidate, the properties in the table above and its speed a core: encode, decode at one to
`m` losses, update and recode, at 2+1 through 10+4 and units from 4 KiB to 1 MiB, on titan
and on europa, from a `znver1` build. Correctness comes first: every loss pattern of the
small layouts decodes, and for a random code the fraction of sets of `k` that fail is
counted.

In a running cluster the cost shows in three arms of [S15](performance.md): a write of part
of a stripe against the same write to a replicated pool, a read with a holder down against
one with none, and a rebuild beside a foreground load.

## Acceptance tests

| Test | Asserts | Milestone |
| --- | --- | --- |
| `every_loss_pattern_decodes` | For each supported k+m up to a bound, every set of `m` lost pieces is recovered | M18 |
| `decode_never_mixes_labels` | A decode offered pieces under two labels uses one label's or fails by name | M18 |
| `partial_overwrite_equals_a_fresh_encode` | After any sequence of partial writes, each parity piece equals what encoding the data pieces afresh gives | M18 |
| `parity_stage_refuses_a_base_it_does_not_hold` | A parity holder at another label refuses the stage, and the write falls back or fails | M18 |
| `short_stripe_stores_no_padding` | A stripe shorter than a unit stores `1 + m` pieces of its own length | M18 |
| `encoding_is_stable_across_builds` | Fixed inputs encode to fixed bytes, pinned by literal, on Zen1 and Zen4 builds alike | M18 |
| `erasure_coded_ack_requires_k_plus_f_current` | A write touching one data piece of a k+m stripe is acknowledged only when `k + f` pieces are current after it, and is read back with the touched device lost | M18 |

## Related

[S7](write-path.md) for the protocol a partial overwrite runs under; [S6](device-store.md)
for staging; [S9](read-path.md) for degraded reads; [S10](recovery.md) for rebuilds;
[S11](scrub.md) for checking parity; [S18](contract.md#decision-record) for the candidates;
[S17](prior-art.md#ceph) for Ceph's erasure coding.
