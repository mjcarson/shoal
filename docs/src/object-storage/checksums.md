# X5. Checksums, measured

**Reported 2026-10-03.** This is the record of spike [X5](spikes.md#x5-checksums). It covers
ten candidates from eight crates: CRC-32C twice, CRC-64/NVME twice, XXH3 at two widths, BLAKE3,
gxhash at the workspace's major and the next one, and CRC-32 as a reference. One harness fed them
the same buffers. Each was held to published check values, compared across three hosts, five
builds and every way of feeding it tried, and timed on one core. The page ends in a
recommendation, which [S18](contract.md#q21-in-part-the-checksum-2026-10-03) records as the
choice: **CRC-64/NVME, through `crc-fast` 1.10.0, with a combine Shoal writes itself from the
definition.**

Three facts decide it:

- **gxhash cannot checksum bytes that arrive in pieces.** In both majors, its `Hasher` gives a
  different answer for the same bytes cut differently. It never agreed with its own one-shot
  function on any input fed in pieces. This held on every host and build.
- **Only a CRC combines.** A CRC can make a chunk's checksum from its units' checksums, and can
  carry a unit's checksum to its place, without reading the bytes again. Both CRC-32C and
  CRC-64/NVME did this at every cut tried. With the multiplier for a unit's length made once, a
  combine costs 78 ns on titan. The crates' own combines take 25 to 236 µs, longer than
  checksumming the bytes, so Shoal writes its own.
- **CRC-64/NVME costs no more than CRC-32C and is twice as wide.** Through `crc-fast` on titan
  it runs at 11.4 GiB/s out of cache and 13.1 in it, at 64 KiB units, against 11.1 and 12.4 for
  CRC-32C through the same crate.

**What was given up is speed on Zen1.** XXH3 has a published definition and runs at 14.0 and
20.9 GiB/s on titan, but has no combine and cannot resume from a finished value. The cpu
features, the builds and the AVX2 allowance did not decide anything. What X5 did not settle is
under [What X5 does not settle](#what-x5-does-not-settle).

## The question

[Q21](contract.md#questions-to-answer) asks three things:

- which checksum guards a chunk unit;
- at what granule;
- whether the row keeps a digest of each stripe chunk, to catch a write that was lost whole.

It prefers a definition that no crate's release can move. X5 answers the first, and supplies
the costs the other two are decided with.

The trigger X5 named in advance was gxhash, the one hash the workspace has. If it gave
different output for the same bytes across cpu features, builds or ways of feeding it, it could
not be an on-disk format for bytes that outlive a build. Its exact pin exists because two of its
majors once disagreed ([Resolved #65](../appendix/resolved/gxhash-pin.md)), and its README
says its `avx2` feature gives other hashes.

The user ruled on that feature on 2026-10-03, while X5 was being planned: **AVX2 may be required
of a node.** A checksum that needs AVX2 at compile time, or gxhash's AVX2 path, therefore counts
as a build a node could be given, not as a hazard.

## How it was judged

These criteria were set in the plan, before the harness existed.

**Required.** A candidate without one of these is not recommended, however fast it is:

| Requirement | Why |
| --- | --- |
| The same output for the same input on every host and every build a node could be given, AVX2 assumed | A checksum is on a device for as long as the unit it guards |
| The same output however the bytes are fed | Bytes arrive in frames whose cuts the sender chooses ([S12](wire-and-client.md)), and a unit is checksummed as it arrives |
| No thread of its own | One executor a core ([Thread per Core](../architecture/thread-per-core.md)) |
| A licence Shoal can take | Shoal is MIT |

**Then ranked by**, in this order:

1. a definition that lives outside the crate;
2. whether it combines;
3. titan's rate at 64 KiB units, cold and hot;
4. the width of the output;
5. how much `unsafe` it carries, whether it needs a C toolchain, and how it is maintained.

## What was run

### The harness

`shoal-spike-checksum/` is a binary with one adapter for each candidate behind one trait. The
trait has four calls, each used where the crate offers it:

- one call over the bytes;
- the crate's incremental interface, fed in pieces;
- a combine of two parts' checksums;
- a resume from a finished checksum.

It is **not a workspace member**, for X4's reason: no candidate reaches the workspace's
lockfile before M13 adds the chosen one. The chosen one reached it sooner, through a spike:
[X13](benchmark-shape.md) checksums its seeded bytes with `crc-fast` 1.10.0, taken with only `std`
as M13 will take it, so the lockfile holds it and `spin` 0.10.1 beside it, and no Shoal crate calls
it until M13. gxhash 2.3.1, the workspace's own, is pinned at the
same version. gxhash 3.5.0 sits beside it under another name.

It runs three passes, in this order.

**facts**: the threads each candidate starts, what one call allocates at 64 KiB (counted by a
global allocator), and the kernel the crate says it chose, where it says one.

**check**: correctness, below.

**speed**: every unit of 4 KiB, 16 KiB, 64 KiB, 256 KiB and 1 MiB. Each unit is timed two ways:
one call, and the incremental interface fed in pieces of 4 KiB, which is a unit arriving in
frames. Each cell is measured three times for at least 200 ms each, with the candidates
interleaved inside the cell, and the figure is the median of the three. The clock is read once
every 256 KiB of work, so reading it is not a tenth of a 4 KiB call. Then the nanoseconds of one
combine, at a second part of 4 KiB, 64 KiB and 1 MiB.

As in X4, there are two ways of holding the data, and every table says which:

- **Cold** takes units in turn from a 256 MiB arena, which no host keeps in cache.
- **Hot** is one unit over and over.

The harness also has **a combine of its own**, written from the definition: zlib's method
(`multmodp` and `x2nmodp` in its `crc32.c`), made generic over the width. For a CRC whose initial
value equals its final xor, as all three CRCs here do, `crc(a ‖ b) = crc(a) · x^(8·|b|) mod P ⊕
crc(b)`. The multiplier depends on the length of `b` alone. So for a fixed unit it is made once,
and a combine is one multiplication modulo P. The harness's combine was added once the first
quick run showed the crates' combines taking microseconds. It is checked against one call over
the whole, like every crate's.

| Candidate | Crate | Definition | How it was driven |
| --- | --- | --- | --- |
| `crc32c` | `crc32c` 0.6.8 | CRC-32C | `crc32c`; `crc32c_append` from zero, a piece at a time; `crc32c_combine` |
| `crc-fast crc32c` | `crc-fast` 1.10.0 | CRC-32C, which the crate calls CRC-32/ISCSI | `checksum`; `Digest::update`; `checksum_combine`; `get_calculator_target` |
| `crc-fast crc64nvme` | `crc-fast` 1.10.0 | CRC-64/NVME | the same calls |
| `crc64fast-nvme` | `crc64fast-nvme` 1.2.1 | CRC-64/NVME | `Digest::write` once, and a piece at a time. No combine |
| `xxh3-64`, `xxh3-128` | `xxhash-rust` 0.8.19 | XXH3, seed zero | `xxh3_64` and `xxh3_128`; `Xxh3::update` |
| `blake3` | `blake3` 1.8.7 | BLAKE3 | `hash`; `Hasher::update`; `hazmat` subtrees as its nearest thing to a combine |
| `gxhash 2` | `gxhash` 2.3.1 | the crate's code | `gxhash64(_, 0)`, which the workspace's archive records use; `GxHasher::with_seed(0)` and `write` |
| `gxhash 3` | `gxhash` 3.5.0 | the crate's code | the same calls |
| `crc32fast` | `crc32fast` 1.5.1 | CRC-32 (ISO-HDLC) | `hash`; `Hasher::update`; `Hasher::combine` over hashers resumed from the two checksums |

### Where, and from which builds

Under the `performance` governor on every host, put back afterwards: `schedutil` on titan and
hyperion, `powersave` on europa. One core was pinned: core 2 on the Zen1 hosts and core 8 on
europa. No shoal unit was running on any host. europa is also the development host, and its
figures carry that noise.

| Host | CPU | Build | Compiled for | Found at run time |
| --- | --- | --- | --- | --- |
| titan | Ryzen Embedded V1756B (Zen1) | `znver1`, what the lab's nodes run | SSE4.2, PCLMULQDQ, AES, AVX2 | the same |
| titan | the same | `x86-64-v3 +aes`: the AVX2 baseline the user allowed, plus the AES gxhash 3 refuses to build without | SSE4.2, AES, AVX2; no PCLMULQDQ | SSE4.2, PCLMULQDQ, AES, AVX2 |
| hyperion | the same | both, as a repeat of titan | | |
| europa | Ryzen 9 7945HX (Zen4) | `znver1`, what a node of a mixed cluster runs | SSE4.2, PCLMULQDQ, AES, AVX2 | adds AVX-512F and VL, VPCLMULQDQ, VAES |
| europa | the same | `x86-64-v3 +aes` | SSE4.2, AES, AVX2 | the same |
| europa | the same | `x86-64-v4 +aes`, what a cluster of AVX-512 nodes would build | adds AVX-512F and VL | the same |
| europa | the same | native (`znver4`) | everything it has | the same |
| europa | the same | native, with gxhash 3's `hybrid` | the same | the same |
| titan | Zen1 | `znver1` with gxhash 3's `hybrid`, the checks only | SSE4.2, PCLMULQDQ, AES, AVX2 | **SIGILL** at the first gxhash 3 call |

Nine runs completed. hyperion agreed with titan to a median of 0.16% a cell; 95% of the 200
cells were within 1.5% for the `znver1` build and 2.0% for `x86-64-v3`. The three measurements of
a cell spread by a median of 0.17% on titan and 0.36% on europa.

**One run had an unexplained fault.** In titan's first `x86-64-v3` run, one or two of the three
measurements fell to between 4.6 and 8.2 GiB/s in five cells of `xxh3-128`, cold and hot; every
other candidate's cells were normal. Neither hyperion's run of the same binary nor a repeat of
the speed pass on titan showed it (`results/titan-x86-64-v3-repeat.md`), and no figure on this
page comes from those cells.

rustc 1.100.0-nightly (2026-09-04).

### What it took to build

- **The x86-64 levels do not carry AES, and gxhash 3 will not build without it.** `x86-64-v3`
  and `x86-64-v4` stop at gxhash 3.5.0's `compile_error!` "Gxhash requires aes and sse2
  intrinsics" (`src/gxhash/platform/x86.rs:2`). Both level builds therefore add
  `-C target-feature=+aes`. gxhash 2.3.1 builds without it and gives the same output, but nothing
  in it checks for AES at run time (README:41-44).
- **gxhash 2.3.1's `avx2` feature does not compile.** It asks for nightly's `stdsimd`, which
  rustc 1.100 rejects: `error[E0635]: unknown feature 'stdsimd'` (`src/lib.rs:2`). That feature
  is the variant its README says gives other hashes. No compiler a node would use can build it,
  so the AVX2 allowance has nothing to enable in 2.x.
- **gxhash 3.5.0's `hybrid` builds for a cpu that cannot run it.** Its guard asks for `aes` and
  `avx2` (`src/gxhash/platform/x86.rs:4-5`), but its kernels use VAES (`:109-146`), which Zen1
  does not have. The `znver1` build with `hybrid` ran on europa. On titan it died of SIGILL
  (exit 132) at the first gxhash 3 call.
- **`blake3` assembles its own kernels** (`build.rs:206-278`) with a C compiler when it finds
  one, and falls back to intrinsics without AVX-512 when it does not. Every binary was built on
  europa, which has one.

To run it again, see `CLAUDE.md`. The raw tables of every run are in
`shoal-spike-checksum/results/`.

## Correctness and stability

Every check was run on every host and build. They all found the same.

### Published values

Every candidate gave every published value in every run:

| Candidate | Values | Where they are published |
| --- | --- | --- |
| CRC-32C, both crates | `"123456789"` → `e3069283`; `"Hello world!"` → `7b98e751` | The RevEng catalogue's check value, as `crc-catalog` carries it (`algorithm.rs:2270`); `crc32c`'s doc test (`lib.rs:8-12`) |
| CRC-64/NVME, both crates | `"123456789"` → `ae8b14860a799888`; 4,096 zero bytes → `6482d367eb22b64e` | The RevEng catalogue (`crc-catalog` `algorithm.rs:2500`); `crc64fast-nvme`'s tests (`lib.rs:202`) |
| CRC-32 | `"123456789"` → `cbf43926` | The RevEng catalogue |
| XXH3, 64 and 128 bits | Empty, 1,024 and 10,240 bytes of the bytes 0 to 250 repeating | `twox-hash` 2.1.4's tests, a second implementation of XXH3 checked against the C reference (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) |
| BLAKE3 | 0, 1, 1,024, 1,025 and 102,400 bytes of the same pattern | The reference repository's `test_vectors/test_vectors.json` at tag 1.8.7, fetched for this. The crate does not ship it |

gxhash publishes no value outside its crate's own tests.

### The same input on every host and build

Each candidate checksummed seven sets of seeded input in every run:

- every length from 0 to 257 bytes, each with its own seed, folded into one FNV-1a;
- one unit of each of the five sizes;
- 1 MiB and 13 bytes, which ends in a partial block.

In all nine runs, every candidate gave the same output for every set: 70 sets, each identical
across three hosts, two microarchitectures and five builds.

So **nothing a build or a cpu does moved any candidate's output**. That includes gxhash 2 and 3,
and gxhash 3's `hybrid` against its plain build on europa, which confirms its README's claim on
the one cpu that can run it. The trigger X5 named for cpu features and builds did not fire. The
digests are in `results/summary.md`; M13's frozen vectors start from the chosen candidate's.

**Every start offset.** The same 64 KiB and 7 bytes, copied to each of the 64 offsets of a cache
line, gave the same output at each, for every candidate in every run.

### Fed in pieces

Each candidate's incremental interface was fed 13 inputs, from empty to 1 MiB and 13 bytes. They
were cut into pieces of 1, 7, 64, 4,096 and 65,536 bytes, and in a hundred seeded random
cuttings with pieces of 0 to 9,999 bytes:

| Candidate | Fed in pieces, equal to one call | Fed in pieces, equal to the same interface fed whole |
| --- | --- | --- |
| Both CRC-32C, both CRC-64/NVME, both XXH3, BLAKE3, CRC-32 | 1,365 of 1,365, every run | 1,365 of 1,365 |
| `gxhash 2` | **0 of 1,365**, every run | 1,082 of 1,365: the short inputs, which a piece held whole |
| `gxhash 3` | **0 of 1,365**, every run | the same |

**gxhash's `Hasher` is not a streaming hash.** Each `write` hashes its slice alone and chains the
result into the state:

- 2.3.1: `state = compress_1(compress_all(bytes), state)` (`src/hasher.rs:117-119`);
- 3.5.0: `aes_encrypt_last(compress_all(bytes), aes_encrypt(state, KEYS))`
  (`src/hasher.rs:115-118`).

So the answer depends on where the pieces were cut. Even fed whole, it does not equal `gxhash64`
of the same bytes and seed, because the one-shot function finishes with another round
(`src/gxhash/mod.rs:67-69` in 2.3.1). gxhash 3's README says the `Hasher` and the one-shot
function differ (README:46). This is what the X5 trigger asked about "ways of feeding it", and it
fired.

**It is not a defect in Shoal.** Every gxhash in the tree hashes pieces its own format fixes,
identically on both sides:

- a WAL sidecar's `checksum_of` is one `write` of the whole
  (`shoal-core/src/server/wal/mod.rs:118`);
- an archive record is `gxhash64` of the payload;
- a partition's digest is a row's length and then its bytes, row by row
  (`shoal-core/src/server/replication/digest.rs:62`).

Nothing hashes bytes cut where a sender chose. That is what rules gxhash out here, and nowhere
else.

**The tree has met this once already.** The snapshot stream's `FileHasher` exists because "two
feeds of the same bytes chunked differently hash differently; a sender writing a megabyte at a
time and a receiver assembling whatever the lane delivered would never agree"
(`shoal-core/src/server/replication/snapshot.rs:86-91`). It copies every byte into a fixed
64 KiB block and hashes the blocks in turn. That is the workaround a chunk unit would need, a
copy of every byte. A CRC needs no buffer, because it gives the same answer however it is fed.

### A whole from its parts

| Candidate | How | Equal to one call over the whole, every run |
| --- | --- | --- |
| `crc32c`, `crc-fast` (both), `crc32fast` | Sixteen units into a chunk at each unit size, and 1,000 seeded cuts of 1 MiB + 13 bytes in two at any byte, the empty part included | 5 of 5 and 1,000 of 1,000 |
| `crc64fast-nvme` | 100 cuts, its checksums combined by `crc-fast`'s `checksum_combine` | 100 of 100: one definition, two crates, one combine |
| The harness's combine, CRC-32C and CRC-64/NVME | 1,000 cuts at any length; and sixteen units at each size with the multiplier made once | 1,000 of 1,000 and 5 of 5 |
| `blake3` | Sixteen units into a chunk through `hazmat`: each unit's chaining value at its offset, merged up the tree | 5 of 5 |

BLAKE3's merge only works under conditions:

- each unit is a power of two of 1 KiB;
- each unit sits at an offset that is a multiple of its length;
- each unit's stored value is its chaining value, not its hash.

A chunk of units that are not a power of two cannot be merged. XXH3 and gxhash have no combine at
all.

**Carried to its place.** The checksum of a unit followed by a 32-byte identity was made from the
checksum of the unit alone and the identity, without the unit's bytes. All three CRCs did it at
every unit size in every run: by resuming from the finished value (`crc32c_append`,
`crc32fast`'s `new_with_initial`), and by a combine with the identity's own checksum. This is the
property [S12](wire-and-client.md#ranged-frames) asked of Q21. A client checksums a unit's bytes
once. The slice stores that checksum bound to the unit's place, and never reads the bytes to
compute it.

## Properties

Source facts were read at the versions given, at the path named, from the crates as published.
Release dates are from crates.io.

| | `crc32c` | `crc-fast` | `crc64fast-nvme` | `xxhash-rust` (XXH3) | `blake3` | `gxhash` 2.3.1 | `gxhash` 3.5.0 | `crc32fast` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Definition | CRC-32C: six parameters in the RevEng catalogue. Polynomial `0x1EDC6F41`, reflected, initial value and final xor all ones | CRC-32C, and CRC-64/NVME: polynomial `0xAD93D23594C93659`, reflected, initial value and final xor all ones (`crc64/consts.rs:43-52`) | CRC-64/NVME | XXH3, a specification of the xxHash project, implemented separately by `twox-hash` and the C reference | BLAKE3, a specification with reference test vectors | **the crate's code** | **the crate's code**, which is not 2.x's | CRC-32 (ISO-HDLC) |
| Width | 32 | 32 or 64 | 64 | 64 or 128 | 256 | 64; 32 and 128 offered | the same | 32 |
| Same output fed in pieces (measured) | yes | yes | yes | yes | yes | **no** | **no** | yes |
| Combine | `crc32c_combine` (`lib.rs:71`), 25 to 50 µs on titan | `checksum_combine` (`lib.rs:1054`), 28 to 236 µs on titan | none; `crc-fast`'s works on its output | none | `hazmat` subtrees only: power-of-two units at aligned offsets, chaining values stored | none | none | `Hasher::combine` (`lib.rs:151`), 79 to 90 ns |
| Resume from a finished value | `crc32c_append` (`lib.rs:49`) | through its combine | none | none | none | none | none | `Hasher::new_with_initial` (`lib.rs:87`) |
| SIMD, chosen | run time: SSE4.2's `crc32`, three streams (`hw_x86_64.rs:46-55`); no PCLMULQDQ | **run time**, in tiers: PCLMULQDQ, AVX-512VL, VPCLMULQDQ; CRC-32C mixes in `crc32` (`feature_detection.rs:185-268`). Names its tier: `x86-sse-pclmulqdq` on titan, `x86_64-avx512-vpclmulqdq` on europa from every build | run time: PCLMULQDQ; VPCLMULQDQ only behind a nightly feature (`lib.rs:38-41`) | **compile time only** (`lib.rs:48-59`): SSE2 unless the build allows AVX2 or AVX-512 | run time: SSE2 to AVX-512 (`platform.rs:411-474`) | compile time: AES-NI, checked by nothing | compile time: refuses to build without `aes`; `hybrid` runs VAES it does not check for | run time: PCLMULQDQ, then 256- or 512-bit VPCLMULQDQ (`pclmulqdq.rs:65-85`) |
| Allocations a call (measured) | none | none | none | none | none | none | none | none |
| Threads | none | none | none | none | none; `rayon` behind a feature | none | none | none |
| C toolchain | no | no | no | no | **yes**, when one is found: it assembles its own kernels, and falls back to intrinsics without AVX-512 (`build.rs:161-278`) | no | no | no |
| `unsafe` | about 14 lines | about 361 lines | about 64 lines | about 34 lines | about 238 lines, and assembly | about 80 lines | about 58 lines | about 15 lines |
| Licence | Apache-2.0 / MIT | MIT OR Apache-2.0 | MIT OR Apache-2.0 | BSL-1.0 | CC0-1.0 OR Apache-2.0 OR Apache-2.0 WITH LLVM-exception | MIT | MIT | MIT OR Apache-2.0 |
| MSRV | none stated | 1.89 | 1.70 | none stated | none stated; edition 2024 | none stated | none stated | 1.63 |
| Dependencies | none | `digest`, `spin` | `crc`, for a binary of its own | none | `arrayvec`, `constant_time_eq`, `cfg-if`, `cpufeatures`; `cc` to build | `rand` | `rustversion` | `cfg-if` |
| Released | 2024-06-09 | 2025-12-31 | 2025-11-14, and **deprecated** for `crc-fast` | 2026-09-28 | 2026-08-20 | 2023-12-23 | 2025-03-11 | 2026-08-22 |

What a crate ran was read from its dispatch, not observed, except `crc-fast`, which names its
tier. Paths are in each crate's `src/` unless they say otherwise.

## Speed

Every figure is GiB a second for one core, the median of three measurements. These are the
summaries; every cell of every run is in `shoal-spike-checksum/results/`.

### At 64 KiB, every candidate

From the `znver1` build, which is what every node of a mixed cluster runs. *Cold / hot*:

| Candidate | titan, one call | europa, one call | titan, in 4 KiB pieces | europa, in 4 KiB pieces |
| --- | --- | --- | --- | --- |
| `crc-fast crc64nvme` | **11.4 / 13.1** | **40.5 / 75.0** | **11.5 / 12.3** | **40.1 / 62.1** |
| `crc64fast-nvme` | 11.5 / 13.2 | 17.8 / 19.5 | 9.83 / 12.6 | 17.8 / 19.1 |
| `crc-fast crc32c` | 11.1 / 12.4 | 39.5 / 75.1 | 4.65 / 9.48 | 39.9 / 57.8 |
| `crc32c` | 6.47 / 6.82 | 19.6 / 34.4 | 5.36 / 6.69 | 24.8 / 28.0 |
| `crc32fast` (CRC-32) | 11.6 / 13.2 | 40.5 / 75.1 | 11.4 / 12.7 | 41.0 / 56.6 |
| `xxh3-64` | 14.0 / 20.9 | 42.9 / 77.8 | 12.7 / 18.2 | 42.9 / 68.8 |
| `xxh3-128` | 14.0 / 20.7 | 40.5 / 77.4 | 12.7 / 18.2 | 40.1 / 68.6 |
| `blake3` | 1.76 / 1.98 | 5.81 / 8.62 | 1.22 / 1.32 | 3.11 / 3.77 |
| `gxhash 2` | 19.0 / 51.0 | 40.6 / 112 | 18.5 / 46.1, other bytes | 40.4 / 103, other bytes |
| `gxhash 3` | 18.9 / 49.2 | 40.5 / 119 | 18.5 / 48.5, other bytes | 40.4 / 117, other bytes |

What the table says:

- **On Zen1 the three PCLMULQDQ CRCs run at the same rate**: 11.4 to 11.6 out of cache and 13.1
  to 13.2 in it. Which crate and which width cost nothing there.
- **XXH3 is 1.2 times faster cold and 1.6 times hot on Zen1.** gxhash is 1.7 and 3.9 times. On
  Zen4 out of cache, everything but `crc32c`, `crc64fast-nvme` and BLAKE3 runs at about
  40 GiB/s, which is what memory feeds one core, the same ceiling X4 found.
- **`crc-fast`'s CRC-32C is slow in small calls on Zen1**: 4.65 GiB/s cold fed 4 KiB at a time,
  and 4.79 for one call over 4 KiB, against 11.1 at 64 KiB. Above 256 bytes it runs one kernel
  there, which mixes PCLMULQDQ with three streams of the `crc32` instruction
  (`src/crc32/fusion/x86/mod.rs:35-65`), so the loss at 4 KiB is that kernel's cost for each
  call. Its CRC-64/NVME holds at 11.5 either way.
- **`crc64fast-nvme` has no VPCLMULQDQ kernel in a stable build**, so on europa it is a quarter
  of `crc-fast`'s rate in cache.
- **BLAKE3 is a tenth of the CRCs on both cpus**, and fed in 4 KiB pieces it loses its wide SIMD,
  which needs many 1 KiB chunks at once.
- **gxhash fed in pieces is fast and wrong**: the figures are for bytes it hashes differently.

### By build

Every candidate at every build, at 64 KiB, *cold / hot*. titan's two builds are what a Zen1
node runs. europa's four are what a Zen4 node runs from the mixed cluster's build, from the AVX2
baseline, from a portable AVX-512 build and from one for its own cpu.

| Candidate | titan `znver1` | titan `x86-64-v3` | europa `znver1` | europa `x86-64-v3` | europa `x86-64-v4` | europa native |
| --- | --- | --- | --- | --- | --- | --- |
| `crc-fast crc64nvme` | 11.4 / 13.1 | 11.3 / 13.1 | 40.5 / 75.0 | 37.4 / 75.1 | 37.5 / 75.1 | 38.2 / 75.4 |
| `crc-fast crc32c` | 11.1 / 12.4 | 9.64 / 10.9 | 39.5 / 75.1 | 39.4 / 75.3 | 39.2 / 75.1 | 39.5 / 75.5 |
| `crc32c` | 6.47 / 6.82 | 6.51 / 7.09 | 19.6 / 34.4 | 19.7 / 34.4 | 19.6 / 34.2 | 19.7 / 34.4 |
| `crc64fast-nvme` | 11.5 / 13.2 | 11.5 / 13.2 | 17.8 / 19.5 | 17.8 / 19.5 | 17.8 / 19.5 | 17.9 / 19.6 |
| `xxh3-64` | 14.0 / 20.9 | 14.1 / 20.9 | 42.9 / 77.8 | 43.2 / 77.3 | 37.9 / 90.5 | 38.7 / 90.2 |
| `xxh3-128` | 14.0 / 20.7 | 14.0 / 20.8 | 40.5 / 77.4 | 42.7 / 76.6 | 40.4 / 84.2 | 40.5 / 87.9 |
| `blake3` | 1.76 / 1.98 | 1.78 / 1.99 | 5.81 / 8.62 | 5.77 / 8.58 | 5.79 / 8.61 | 5.84 / 8.62 |
| `gxhash 2` | 19.0 / 51.0 | 19.0 / 51.0 | 40.6 / 112 | 40.7 / 112 | 40.6 / 112 | 40.7 / 112 |
| `gxhash 3` | 18.9 / 49.2 | 18.8 / 49.1 | 40.5 / 119 | 40.6 / 119 | 40.5 / 118 | 40.5 / 118 |

- **The build does not move the CRCs.** `crc-fast` chose its tier at run time in every build:
  PCLMULQDQ on titan and VPCLMULQDQ on europa, even from the `x86-64-v3` build, which does not
  let the compiler assume PCLMULQDQ at all. Its CRC-32C is 13% slower from the `x86-64-v3` build
  on Zen1, for a reason X5 did not look into. CRC-64/NVME does not move.
- **XXH3 is the one candidate a build moves**, and only on europa in cache. It chooses its kernel
  at compile time, so the `x86-64-v4` and native builds run AVX-512, 90.5 against 77.8. Out of
  cache memory holds it at about 40 whatever the build. On Zen1 both builds run its AVX2 kernel.
- **gxhash 3's `hybrid` on europa** ran at 39.6 / 148 against 40.5 / 118 without it, and gave
  the same digests. It is the only candidate VAES speeds up, and the same build kills a Zen1 cpu.

### Unit size

GiB a second, one call:

| Candidate | Run | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- | --- |
| `crc-fast crc64nvme` | titan, cold | 11.3 | 11.3 | 11.4 | 11.5 | 11.5 |
| `crc-fast crc64nvme` | titan, hot | 12.3 | 12.9 | 13.1 | 13.2 | 13.2 |
| `crc-fast crc32c` | titan, cold | 4.79 | 8.79 | 11.1 | 11.8 | 12.1 |
| `xxh3-64` | titan, cold | 14.0 | 14.0 | 14.0 | 14.1 | 14.1 |
| `xxh3-64` | titan, hot | 21.0 | 21.6 | 20.9 | 20.9 | 20.9 |

**CRC-64/NVME is at its rate from 4 KiB**, within 2% cold and 7% hot on titan. So the checksum
sets no floor under the chunk unit. X4's encode, which reaches its rate from 16 to 64 KiB, does.

**How long one call holds the core**, which [S13](isolation.md)'s yield budget has to fit. One
call, cold, in microseconds:

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| `crc-fast crc64nvme`, titan | 0.34 | 1.34 | 5.34 | 21.3 | 84.9 |
| `crc-fast crc64nvme`, europa `znver1` | 0.095 | 0.386 | 1.51 | 6.05 | 24.0 |
| `xxh3-64`, titan | 0.27 | 1.09 | 4.35 | 17.4 | 69.5 |
| `blake3`, titan | 3.08 | 8.75 | 34.6 | 138 | 554 |
| `crc32c`, titan | 0.78 | 2.49 | 9.43 | 37.0 | 147 |

A 1 MiB unit holds a Zen1 core for 85 µs to checksum, a sixth of the half millisecond X4 found
for encoding a 4+2 unit row of the same unit. A unit fed in pieces as it arrives can yield
between pieces at the cost the third column of the first table shows.

### A combine

Nanoseconds for one combine, by the length of the second part. titan; hyperion's figures agree
to within 1%; europa's are in brackets.

| Combine | 4 KiB | 64 KiB | 1 MiB |
| --- | --- | --- | --- |
| `crc32c`'s `crc32c_combine` | 25,243 (6,106) | 37,261 (8,021) | 50,308 (10,004) |
| `crc-fast`'s `checksum_combine`, CRC-32C | 28,115 (6,466) | 40,825 (8,540) | 54,084 (10,649) |
| `crc-fast`'s `checksum_combine`, CRC-64/NVME | 124,206 (22,729) | 179,837 (30,887) | 236,158 (39,559) |
| `crc32fast`'s `Hasher::combine`, CRC-32 | 78.8 (36.0) | 88.8 (37.2) | 89.9 (39.3) |
| The harness's, CRC-32C | 73.2 (40.0) | 79.1 (42.1) | 77.4 (43.5) |
| The harness's, CRC-32C, the multiplier made once | 38.9 (24.5) | 38.6 (24.3) | 39.1 (25.0) |
| The harness's, CRC-64/NVME | 150 (91.2) | 155 (93.9) | 156 (94.1) |
| **The harness's, CRC-64/NVME, the multiplier made once** | **77.3 (50.2)** | **78.0 (50.5)** | **79.0 (50.3)** |

**The cost of a combine belongs to the implementation, not the definition.** `crc-fast`
combines CRC-64/NVME of a 64 KiB part in 180 µs on titan, where checksumming those 64 KiB takes
5.3 µs. The harness does it in 78 ns, two thousand times less, with zlib's method for CRC-32
made generic over the width. A chunk of sixteen units combined from its units' checksums costs
about 1.2 µs on titan, where reading the chunk again to checksum it would cost 85 µs. That is what
makes a chunk's digest affordable without a second pass, and it needs a combine Shoal writes
itself ([O85](../appendix/optimizations.md#o85-a-crc-is-combined-by-the-general-method)).

### Against the code

Every unit of every stripe chunk carries a checksum, so a 4+2 stripe checksums half as many
bytes again as it encodes. On titan out of cache:

- checksumming six chunks of 64 KiB with CRC-64/NVME takes 1.5 / 11.4 = 0.132 s for each GiB of
  data;
- encoding them with `rusty_erasure` takes 1 / 7.62 = 0.131 s
  ([X4](erasure-coding-crates.md#by-target)).

**On Zen1 the checksums cost as much CPU as the code.** On europa they cost three quarters of
it out of cache, and twice as much in cache, where GFNI makes the code cheaper than the CRC.
[X9](spikes.md#x9-table-latency-beside-object-work), which runs object work beside a table,
should therefore checksum as well as encode, or it measures half the work. In cache CRC-64/NVME
runs at 13.1 on titan and 75 on europa, against 11.4 and 40.5 out of it. So, as for the code,
checksumming a unit while its bytes are still in cache is worth more on Zen4 than on Zen1
([O86](../appendix/optimizations.md#o86-a-unit-is-checksummed-after-its-bytes-have-left-the-cache)).

## What would have changed the design

The result X5 named in advance:

| If gxhash gave different output for the same bytes across | Found | So |
| --- | --- | --- |
| cpu features | No. Zen1 and Zen4, from every build, gave the same digests. Its AVX2 variant in 2.x cannot be compiled, and 3.x's `hybrid` gave the same output on europa | Not the reason |
| builds | No. Five builds on europa and two on each Zen1 host, all equal | Not the reason |
| ways of feeding it | **Yes.** Fed in pieces, its `Hasher` never equals its one-shot function, and two cuttings of the same bytes rarely equal each other | **gxhash is not the checksum.** A CRC with a published definition is taken |

## The comparison

The options side by side. The figures are titan's at 64 KiB from the `znver1` build, *cold /
hot*.

| Option | Performance | Strengths | Weaknesses | Tradeoffs |
| --- | --- | --- | --- | --- |
| **CRC-64/NVME through `crc-fast`** (recommended) | 11.4 / 13.1; europa 40.5 / 75.0. Even in 4 KiB pieces | Every requirement met. A definition of six parameters, with a check value. Combines and resumes. 64 bits. Picks its tier at run time, and names it. No C, no allocation, no thread | 361 lines of `unsafe`. Its combine is two thousand times slower than it needs to be. One maintained implementation in Rust: the other one was deprecated for it | Width and combine against XXH3's 1.2 to 1.6 times on Zen1. Mitigated by the format being the definition's: any implementation that gives the check value reads every unit |
| **CRC-32C through `crc-fast`** | 11.1 / 12.4; in 4 KiB pieces 4.65 / 9.48 | A hardware instruction on x86 since SSE4.2 and on ARMv8, and BlueStore's default ([S17](prior-art.md)). Combines and resumes | Half as wide, at no saving on either cpu. Slow in small pieces on Zen1 | Ubiquity against width |
| **CRC-32C through `crc32c`** | 6.47 / 6.82 | Small, 14 lines of `unsafe`, run-time dispatch, its own combine and resume | Half the PCLMULQDQ CRCs' rate on Zen1, and of `crc-fast`'s on Zen4 | Simplicity against speed |
| **`crc64fast-nvme`** | 11.5 / 13.2; europa 17.8 / 19.5 | The same CRC-64/NVME bytes | Deprecated by its authors. No combine. No VPCLMULQDQ without nightly | None worth taking |
| **XXH3** | 14.0 / 20.9; europa 42.9 / 77.8 | A published, frozen definition with two independent implementations. Fastest after gxhash. 64 or 128 bits | No combine and no resume, so a chunk digest is a second pass and a unit's checksum cannot be bound to its place without one. SIMD fixed at compile time: AVX2 only from a build that allows it | Speed against the two properties a CRC's linearity gives |
| **BLAKE3** | 1.76 / 1.98; europa 5.81 / 8.62 | Cryptographic: the one candidate that also detects a deliberate change | A tenth of the CRCs. A C toolchain by default. Its merge needs power-of-two units | Strength the contract does not ask for ([P15](contract.md#the-contract) is about accidents) |
| **gxhash 2 or 3** | 19.0 / 51.0; europa 40.6 / 112 | The fastest, and already in the tree | **Fails a requirement**: the answer depends on how the bytes were fed. No definition outside the crate; its two majors disagree. Its wide paths either do not build (2.x) or build for a cpu that cannot run them (3.x) | Disqualified |
| **CRC-32 through `crc32fast`** | 11.6 / 13.2 | Already in the lockfile; the fastest combine of any crate | 32 bits, and not a polynomial S18 pinned | A reference, not a candidate |

**Feature support:**

| | CRC-64/NVME, `crc-fast` | CRC-32C, `crc-fast` | CRC-32C, `crc32c` | `crc64fast-nvme` | XXH3 | BLAKE3 | gxhash |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Same output fed in pieces | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ |
| Definition outside the crate | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ |
| Check value published | ✓ | ✓ | ✓ | ✓ | ✓ (test suites) | ✓ | ✗ |
| Combine | ✓ (slow) | ✓ (slow) | ✓ (slow) | ✗ | ✗ | aligned subtrees | ✗ |
| Resume from a finished value | ✓ | ✓ | ✓ | ✗ | ✗ | ✗ | ✗ |
| Run-time SIMD | ✓ | ✓ | ✓ | ✓ | ✗ | ✓ | ✗ |
| No C toolchain | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ (falls back) | ✓ |
| Maintained | ✓ | ✓ | ✓ | ✗ | ✓ | ✓ | ✓ |
| Width in bits | 64 | 32 | 32 | 64 | 64, 128 | 256 | 64 |

## Recommendation

**CRC-64/NVME**, the polynomial `0xAD93D23594C93659` reflected, with initial value and final
xor all ones, and check value `0xAE8B14860A799888`. It is computed **through `crc-fast`
`=1.10.0`**, pinned exactly as gxhash is, and combined **by Shoal's own code**. The CRCs are the
only candidates that meet every requirement and also combine and resume, and CRC-64/NVME is the
wider of the two the lab can run fast, at no cost over CRC-32C.

- **The format is the definition, not the crate.** A unit's checksum is six parameters and a
  check value. M13's frozen vectors are the digests of `results/summary.md`, computed by two
  crates in nine runs: three hosts and five builds. Any implementation that gives them reads
  every unit Shoal writes, and `crc-fast` can be replaced without a format change. That is the
  property Q21 preferred, and the one gxhash cannot give.
- **The combine is Shoal's**, from zlib's method, with the multiplier for each unit length made
  once. It costs 78 ns on titan, and it is held to `crc-fast`'s one-call output by the same check
  this harness ran. `crc-fast`'s own combine is correct and takes 180 µs.
- **A unit's checksum binds it to its place by a combine.** A client checksums a unit's bytes.
  The slice stores the CRC of the bytes followed by the unit's identity, made from the client's
  checksum and the identity's, and it never reads the bytes to do so. That answers
  [S12](wire-and-client.md#ranged-frames)'s question: a unit can be checksummed once from the
  caller to the disk. What the identity's bytes are is [S6](device-store.md)'s, at M14.
- **A chunk's digest costs no second pass.** Sixteen units combine in about 1.2 µs on titan.
  Whether the row keeps it is not X5's to settle.
- **Why not CRC-32C**, BlueStore's default and a hardware instruction on x86 and ARM. Through
  the best crate it is no faster than CRC-64/NVME on either cpu, and slower in small pieces on
  Zen1. Through `crc32c` it is half the rate. A 32-bit check passes a corrupt unit with a
  chance of one in 2^32. A deep scrub of a 16 TiB device in 64 KiB units checks 2^28 units, so if
  every one of them had rotted, it would expect to miss a sixteenth of a unit with 32 bits, and
  2^-36 of one with 64.
- **Why not XXH3**, faster on Zen1 by 1.2 to 1.6 times. It has no combine and no resume. So a
  chunk digest is a second pass over the chunk, and a client's checksum can be bound to a place
  only by hashing the hash, which is a second definition for every reader to implement. On Zen4
  out of cache the two run at the same rate.
- **What `crc-fast` costs, and what is done about it.** It has 361 lines of `unsafe`, which M13
  reads before it adds the dependency, as M18 does for `rusty_erasure`'s. It builds a `cdylib`
  and a `staticlib` beside its `rlib`, and its default features add an FFI and a panic handler
  Shoal does not need, so M13 takes it with only `std`. Its tier names, and a detection that
  checks fewer features than its widest kernel uses, are on the defects table below.
- **The AVX2 allowance is not leaned on.** `crc-fast` picks its tier at run time, so one
  `znver1` build ran PCLMULQDQ on titan and VPCLMULQDQ on europa. The allowance would have
  mattered to XXH3, whose AVX2 path is chosen at compile time. It stays recorded on S18 for the
  next choice that wants it.

## What X5 does not settle

- **The granule.** X5 says CRC-64/NVME is at its rate from 4 KiB and that a 1 MiB unit holds a
  Zen1 core for 85 µs. The unit's size is Q20's geometry, with X4's encode, X6's device and
  [S15](performance.md)'s read arms.
- **Whether the row keeps a digest of each stripe chunk.** X5 says it costs about 1.2 µs a chunk
  to make and eight bytes to keep. Whether a lost whole write is caught that way or by the
  labels is [X1](spikes.md#x1-the-stripe-protocol-as-a-model)'s, ✅ answered: by the labels, and the
  row keeps no digest ([X1](stripe-model.md#the-chunk-digest)). What a row can carry is
  [X10](spikes.md#x10-what-a-stripe-row-costs)'s. X10 measured it: six digests cost a stripe row
  64 bytes archived, 61 on disk and 65 in the WAL, and nothing in any index, since an index entry
  is the key's ([X10's record](stripe-row-costs.md#2-bytes-a-row)).
- **The identity's bytes**, and whether a unit's identity is a suffix or a prefix. A suffix is
  what the combine above makes cheap. [S6](device-store.md) owns it, at M14.
- **A checksum inside a node**, beside a table, which is
  [X9](spikes.md#x9-table-latency-beside-object-work).
- ~~**S3's checksums.** S3 is recalled to offer full-object CRC-64/NVME and CRC-32C checksums,
  which a combine of Shoal's unit checksums could answer without reading an object. That is
  recalled and not read; [X14](spikes.md#x14-ceph-and-s3-at-the-source) reads the S3 reference.~~
  **S3's checksums**, read by [X14](ceph-and-s3-sources.md#13-s3-checksums) in AWS's model of the
  API. S3 offers full-object CRC-64/NVME, CRC-32C and CRC-32. CRC-64/NVME is its default for an
  upload that names none, and for a multipart upload S3 computes it "from the part-level
  checksums". That is this page's checksum and its combine. A Shoal object's S3 checksum can
  be made without a read, if its units' CRCs can be had without the bytes, which is the digest
  a chunk Q21 leaves open.
- **The tables' own checksums.** WAL frames, archive records, the control store and snapshot
  chunks keep gxhash. X5 found nothing wrong with how they use it, and moving them is a format
  change with no defect behind it ([todos](../appendix/todos.md#the-tables-keep-gxhash)).

## What it did not measure

- **Error detection itself.** How many bit flips a polynomial always catches at a given length
  is a property of the definition, published for these CRCs, and not something a run on healthy
  hardware can show. No corruption was injected.
- **ARM.** Every figure is x86. `crc-fast` and `crc32c` have AArch64 kernels; none was run.
- **An Intel cpu.** Every AVX-512 and VPCLMULQDQ figure is Zen4's.
- **Crates S18 did not pin**, such as `crc`, a table-driven implementation with no SIMD that
  forbids `unsafe`, or the CRC kernels of the Linux kernel and ISA-L.
- **A keyed or cryptographic check against deliberate tampering.** BLAKE3 was timed; the contract
  does not ask for it ([P15](contract.md#the-contract) and the failure model under it).

## Defects found upstream

None is Shoal's, so none is filed on [Known Issues](../appendix/known-issues.md). Each is written
here because it is what a reader of these crates would otherwise rediscover.

| Crate | Defect | Where |
| --- | --- | --- |
| `gxhash` 2.3.1 | The `avx2` feature cannot be compiled by a current nightly: `#![feature(stdsimd)]` names a gate rustc no longer has | `src/lib.rs:2` |
| `gxhash` 3.5.0 | `hybrid` checks for `aes` and `avx2` but runs VAES instructions it does not check for. A `znver1` build compiles and dies of SIGILL on a Zen1 cpu | `src/gxhash/platform/x86.rs:4-5`, `:109-146` |
| `crc-fast` 1.10.0 | The VPCLMULQDQ tier's kernels enable `avx512bw` and `avx512f`, but detection checks `avx512vl` and `vpclmulqdq` only. No shipping cpu has the two without the others; a hypervisor that masks features could | `src/feature_detection.rs:193-194`; `src/arch/x86_64/avx512_vpclmulqdq.rs:115`, `:377` |
| `crc-fast` 1.10.0 | `get_calculator_target` names the SSE tier `x86-sse-pclmulqdq` on x86_64, because both tiers share one instance. It ignores its algorithm argument, so it never shows the CRC-32C path that mixes in the `crc32` instruction | `src/feature_detection.rs:299`, `:378`; `src/lib.rs:1131` |
| `crc-fast` 1.10.0 | `checksum_combine` takes 124 to 236 µs for CRC-64/NVME on titan, and 25 to 54 µs for CRC-32C. That is longer than checksumming the second part, where zlib's method takes under 160 ns. Not wrong; too slow to use | `src/lib.rs:1054` |
| `crc64fast-nvme` 1.2.1 | Deprecated by its authors in favour of `crc-fast` (CHANGELOG.md:3-4). It still depends on `crc` as a normal dependency for a binary of its own | `Cargo.toml` |

## Related

- [X5](spikes.md#x5-checksums) for what was planned.
- [S6](device-store.md) for where a unit's checksum lives.
- [S12](wire-and-client.md) for a checksum computed once from the client to the disk.
- [S18](contract.md#q21-in-part-the-checksum-2026-10-03) for the decision.
- [S1](prerequisites.md#dependencies-to-choose) for the prerequisite it closes.
- [M13](milestones.md#m13-the-wire-and-the-baseline) for where the dependency is added.
- [X4's record](erasure-coding-crates.md) for the form this page copies.
- [O85](../appendix/optimizations.md#o85-a-crc-is-combined-by-the-general-method) and
  [O86](../appendix/optimizations.md#o86-a-unit-is-checksummed-after-its-bytes-have-left-the-cache)
  for the two findings that are a node's to act on.
