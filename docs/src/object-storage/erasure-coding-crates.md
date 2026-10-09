# X4. Erasure coding crates, measured

**Reported 2026-10-03.** This is the record of spike
[X4](spikes.md#x4-erasure-coding-crates-performance-and-tradeoffs). It covers seven candidates,
fed the same buffers by one harness. Each was checked against every loss pattern and timed on
one core, on titan and hyperion (Zen1) and on europa (Zen4), from every build a node could be
given. It ends in a recommendation, which [S18](contract.md#q20-in-part-the-code-and-the-crate-2026-10-03)
records as the choice: **Reed-Solomon over GF(2^8), systematic, on ISA-L's Cauchy matrix,
through `rusty_erasure` 0.4.1, with plain XOR at one parity chunk.**

Three facts decide it:

- It is the fastest candidate at every operation on both microarchitectures. On titan it
  encodes 4+2 at 7.6 GiB/s out of cache and 9.9 in it; on europa, at 20 and 97.
- It has every property [S8](erasure-coding.md#what-the-code-has-to-be) asks for, and an update
  in its public API.
- Its parity is byte for byte ISA-L's, so what a pool writes is defined by a matrix older than
  the crate.

**None of the four results that would have moved S8's preference came out.** What it did not
settle is under [What X4 does not settle](#what-x4-does-not-settle).

## The question

[Q20](contract.md#questions-to-answer) asks which family of code, which crate and what
geometry. X4 is the half of it a measurement can answer: the family and the crate, with what
the geometry can be told from a code's speed. [X14](spikes.md#x14-ceph-and-s3-at-the-source),
the reading of Ceph, ~~is~~ was the other half, and it found nothing in Ceph's geometry that X4
and X6 had not already bounded ([X14](ceph-and-s3-sources.md#3-partial-writes-and-the-shard-versions)). The user asked for code families and not only
Reed-Solomon crates, with `rlnc` named as one to include, and on 2026-10-03 for a comparison of
every option and its performance under each build target, so that a cluster of AVX-512 nodes
knows what to expect.

## How it was judged

Stated before any number was read, from [S8](erasure-coding.md#what-the-code-has-to-be),
[S13](isolation.md) and the rules for a dependency whose output is persisted.

**Required.** A candidate without one of these is not recommended, however fast it is:

| Requirement | Why |
| --- | --- |
| Systematic | A healthy read of a range touches one chunk and decodes nothing |
| Decodes from any k, always | [P11](contract.md#the-contract) is stated for any k. A probability is not a contract |
| An update of one data chunk | A small write rewrites 1 + m chunks, not k + m, and reads none of the others |
| Writes into the caller's buffers | Direct I/O wants aligned buffers the caller owns; a crate's `Vec` is a copy into one |
| Picks its SIMD at run time | Nodes are built for the oldest cpu in the deployment, so a choice made at compile time gets Zen1's instructions on every host |
| No thread pool of its own | One executor a core ([Thread per Core](../architecture/thread-per-core.md)) |
| The same bytes for the same input, on every host and build, by a definition that outlives the release | The encoding a pool was written with is the encoding it is read with, for ever |
| A licence Shoal can take | Shoal is MIT |

**Then ranked by** titan's 4+2 encode against X4's line of about a gibibyte a second; degraded
decode, rebuild and update; how much `unsafe` it carries and how long it has been maintained;
and whether it needs a C toolchain.

## What was run

### The harness

`shoal-spike-erasure/` is a binary with one adapter for each candidate behind one trait:
encode, decode with chunks lost, rebuild one chunk, update part of one data unit. It is **not a
workspace member**: its manifest carries a `[workspace]` of its own, with its own `Cargo.lock`.
That keeps the six erasure coding crates out of the workspace's lockfile ~~until M18 adds the
chosen one~~, and it keeps `isa-l`'s C build out of every workspace build. The chosen one entered
the workspace's lockfile ahead of M18 with [X9](table-latency.md), decided with the user on
2026-10-08: `rusty_erasure` 0.4.1 behind shoal-core's `x9` feature, which only X9's harness builds,
its two sub-crates held at the 0.4.1 measured here since a fresh resolve takes 0.4.2.

It runs three passes, in this order:

- **facts**: the threads each candidate starts, and what one call of each operation allocates
  at 4+2 and 64 KiB, counted by a global allocator;
- **check**: correctness, below;
- **speed**: every layout of 2+1, 4+2, 6+3, 8+3 and 10+4, at every unit of 4 KiB, 16 KiB,
  64 KiB, 256 KiB and 1 MiB, for every operation. Each cell is measured three times, at least
  200 ms of whole calls each, with the candidates interleaved inside the cell so that drift
  falls on all of them. The figure is the median of the three.

Two ways of holding the data are measured, and every table says which:

- **Cold** takes rows in turn from a 128 MiB data arena and a 256 MiB arena for what the code
  stores. No host keeps that in cache, so a figure is bounded by memory as a node's would be if
  it encoded bytes that had left the cache.
- **Hot** is one row, over and over, so the figure is the code's own: what it does with bytes
  that just arrived. It was added once the first cold run showed europa's fastest candidates
  and XOR all stopped at the same rate, which was the memory's. A row larger than the cache is
  not hot, which shows at 1 MiB on titan.

That first round was cold only, and its XOR made a pass over the parity for every source, which
is not a ceiling. It was superseded by the round on this page, from one binary with both fixed,
and nothing from it is quoted. Its figures for the other candidates agreed with this round's.

The figures are counted by:

- encode and decode: GiB a second of stripe data, k × unit;
- rebuild: the chunk rebuilt;
- update: the bytes changed.

A decode loses data chunks first, the worst case for a systematic code. A code that is not
systematic decodes on every read, so it is also measured with nothing lost.

Decode plans and tables are built once for a loss pattern and reused, where the API allows it,
as a degraded read of many stripes would. `rusty_erasure`'s `decode_plan`, ISA-L's tables and
`reed-solomon-erasure`'s own cache all do. `reed-solomon-simd`, `raptorq` and `rlnc` have no
plan and pay their whole decode on every call.

| Candidate | Version | How it was driven |
| --- | --- | --- |
| XOR | the harness | Parity is the xor of the data, in blocks of 1 KiB across every source so the output is written once. One parity chunk only |
| `rusty_erasure` | 0.4.1 | `Matrix::cauchy`, `rusty_erasure::coder` (the best kernel set for the cpu), `encode`, `decode_plan` + `recover_with`, `update`. All into the harness's buffers |
| `isa-l` | 0.2.0, ISA-L 2.29.0 bundled by `libisal-sys` 0.1.2 | `gf_gen_cauchy1_matrix`, `ec_init_tables`, `ec_encode_data`, decode tables from `gf_invert_matrix`. The update is the C library's `ec_encode_data_update`, declared by the harness because the crate binds none |
| `reed-solomon-erasure` | 6.0.0, `simd-accel` | `encode_sep`; `reconstruct_data` and `reconstruct` over `(&mut [u8], bool)`. The update is built by the harness from the public `galois_8::mul_slice_xor` and coefficients read out of the crate by encoding unit vectors |
| `reed-solomon-simd` | 3.1.0 | One encoder and decoder reused through `reset`; shards copied in, and results copied out of its work space into the harness's buffers |
| `raptorq` | 2.0.1 | `SourceBlockEncoder::with_encoding_plan` and `SourceBlockDecoder`, one source block of k symbols a unit row. A symbol is a `u16`, so a unit above 32 KiB is cut into 32 KiB columns, each a block of its own, all losing the same chunks |
| `rlnc` | 0.8.7 | As published: k + m coded pieces from `Encoder`, decoded by `Decoder`, rebuilt by `Recoder`. Coefficients from a seeded ChaCha8 |
| `rlnc`, systematic | 0.8.7 | Bent by the harness. Data chunks are the data; parity is `Recoder` output over pieces whose coefficient vectors are unit vectors, plus a constant piece holding the padding marker the decoder insists on. It shows what `rlnc`'s kernels give in S8's layout. Neither trick is in the crate's documentation |

### Where, and from which builds

No shoal unit was running on any host, so nothing had to be stopped. Every host ran under the
`performance` governor and was put back afterwards: `schedutil` on titan and hyperion,
`powersave` on europa. One core was pinned, core 2 on the Zen1 hosts and core 8 on europa.
europa is also the development host, and its figures carry that noise. The three measurements
of a cell spread by 0.5% typically on titan and 1% on europa.

| Host | CPU | Build | `reed-solomon-erasure`'s C | What the candidates found at run time |
| --- | --- | --- | --- | --- |
| titan | Ryzen Embedded V1756B (Zen1) | `target-cpu=znver1` | `-march=haswell` | SSSE3, AVX2 |
| hyperion | the same | the same | the same | the same: a repeat of titan. The two agreed to a median of 0.4% a cell, and 95% of the 1,475 cells to within 2.7%. The widest, up to 20%, are 4 KiB cells, where a call takes a microsecond or two |
| europa | Ryzen 9 7945HX (Zen4) | `target-cpu=znver1`, what a node of a mixed cluster runs | `-march=haswell` | SSSE3, AVX2, AVX-512 F/BW/VL, GFNI |
| europa | the same | `target-cpu=x86-64-v4`, what a cluster of AVX-512 nodes would build | `-march=x86-64-v4` | the same |
| europa | the same | native (`znver4`) | `-march=native` | the same |

The `kernels` pass forced `rusty_erasure` through each kernel set the cpu has, on titan and on
europa, at 4+2 and 10+4.

rustc 1.100.0-nightly (2026-09-04).

### What it took to build

- **`isa-l`** builds the ISA-L 2.29.0 it bundles with autotools and nasm: `sudo apt install
  nasm autoconf automake libtool pkgconf` on europa, and a static link, so titan needed
  nothing.
- **It also needs an old `pkg-config` crate.** `libisal-sys` falls back to that build only when
  its probe fails with `pkg_config::Error::Failure` (`build.rs:16-17`). Every `pkg-config`
  since 0.3.23 reports a missing library as `ProbeFailure`, which the build script panics on
  (`build.rs:49-50`). So with any `pkg-config` released since 2022 the crate cannot build
  without a system `libisal`. The harness's lockfile pins `pkg-config` 0.3.22.
- **`reed-solomon-erasure`'s C kernels** are compiled for `-march=haswell` unless
  `RUST_REED_SOLOMON_ERASURE_ARCH` names another (`build.rs:160-183`). Its build script does not
  declare that variable, so cargo does not rerun it when the variable changes. Each europa build
  was made from an empty target directory. The kernel's instructions were read back from each
  binary: AVX2 in the `znver1` build, AVX-512 in the other two.

To run it again, see `CLAUDE.md`. The raw tables of every run are in
`shoal-spike-erasure/results/`.

## Correctness

Run on every host and build. All five runs found the same, at a 4 KiB unit.

**Every loss pattern.** Every set of one to m lost chunks, data or parity, was tried at every k
from 2 to 6 and every m from 1 to 3, and at 8+3 and 10+4: 2,186 patterns a candidate, 1,476 of
them losing exactly m. Each was decoded and the data compared byte for byte. Then every single
chunk was rebuilt and compared, 115 of them.

| Candidate | Patterns decoded | Undecodable | Wrong bytes | Rebuilds wrong |
| --- | --- | --- | --- | --- |
| XOR (m = 1 only) | 25 of 25 | 0 | 0 | 0 |
| `rusty_erasure` | 2,186 of 2,186 | 0 | 0 | 0 |
| `isa-l` | 2,186 of 2,186 | 0 | 0 | 0 |
| `reed-solomon-erasure` | 2,186 of 2,186 | 0 | 0 | 0 |
| `reed-solomon-simd` | 2,186 of 2,186 | 0 | 0 | 0 |
| `raptorq` | 2,182 | **4**, all at 10+4 with four chunks lost | 0 | 0 |
| `rlnc` | 2,182 | **4**, at 4+3, 5+2 and 10+4 | 0 | 0, and 1 recoded chunk the stripe could not then decode with |
| `rlnc`, systematic | 2,184 | **2**, at 8+3 and 10+4 | 0 | 0 |

**RaptorQ does not decode from every k.** Its failures are fixed by the code, not drawn: the
same four patterns of 10+4 fail on every host. Four of 1,001 is RFC 6330's "below about 1% at
zero overhead", and it means P11 cannot be stated for it.

**Random coefficients fail at the rate theory says.** 100,000 trials a layout, each with fresh
coefficients and m chunks lost at random:

| Candidate | 4+2 | 6+3 | 10+4 |
| --- | --- | --- | --- |
| `rlnc` | 0.388% | 0.402% | 0.383% |
| `rlnc`, systematic | 0.364% | 0.367% | 0.408% |

That is one in 255 whatever the layout, which is the chance that k random vectors over GF(2^8)
are dependent. A stripe that loses m chunks has a one in 255 chance of being lost with them.

**Updates.** 500 updates of random ranges of random data units, then the parity compared with a
fresh encode of the data as it then stood, at 2+1, 4+2 and 10+4. It was equal for XOR,
`rusty_erasure`, `isa-l` (the C library's update) and `reed-solomon-erasure` (the harness's).
The other four have no update to check.

**The same input gives the same bytes.** Each candidate encoded fixed seeded data at the five
X4 layouts, twice, and an FNV-1a digest of what it stored was compared across the five runs:
three hosts, two microarchitectures and three builds. All 36 digests were identical in every run
and both times within a run, `rlnc`'s with its seeded coefficients included.

**Two candidates write the same parity.** `isa-l`'s and `rusty_erasure`'s Cauchy parity is the
same bytes at all five layouts. So is `reed-solomon-erasure`'s and `rusty_erasure`'s through
its compatibility matrix (`compat::reed_solomon_erasure_matrix`). At one parity chunk,
`reed-solomon-simd`'s parity is the XOR of the data: its digest at 2+1 is XOR's.

## Properties

The first of the two tables X4 asks for. Source facts were read at the versions given, at the
path named, from the crates as published.

| | XOR | `rusty_erasure` | `isa-l` | `reed-solomon-erasure` | `reed-solomon-simd` | `raptorq` | `rlnc` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Family | Parity | Reed-Solomon, GF(2^8) 0x11D, Cauchy or Vandermonde | Reed-Solomon, GF(2^8) 0x11D | Reed-Solomon, GF(2^8) 0x11D, Vandermonde·top⁻¹ | Reed-Solomon, Leopard FFT over GF(2^16) | Fountain, RFC 6330 | Random linear network coding, GF(2^8) **0x11B** (`common/gf256.rs:50`) |
| Systematic | yes | yes | yes | yes | yes | yes | **no**: every chunk is a random combination |
| Decodes from any k | at m = 1 | yes, measured | yes, measured | yes, measured | yes, measured | **no**: 4 of 1,001 patterns of 10+4 | **no**: 1 in 255 |
| Chunks a small write rewrites | 1 + 1 | 1 + m | 1 + m | 1 + m | 1 + m, reading the k - 1 others | 1 + m, reading the k - 1 others | k + m |
| Update of one data chunk in the public API | yes | **yes**, `Coder::update` (core `encode.rs:150`) | no; the C library's `ec_encode_data_update` exists and is not bound | no; `galois_8::mul_slice_xor` is public, the matrix is not; `encode_single` is incremental, in order (`core.rs:534-545`) | no | no | no |
| Recoding | - | - | - | - | - | - | yes, `Recoder`, from k pieces |
| Chunks read to rebuild one | k | k | k | k | k | k | k |
| Rebuilds only what is lost | yes | yes, any index (`recover_with`) | yes, by its tables | data only, or everything missing | originals only; a parity chunk is a whole encode | the whole column | a new piece, not the lost one |
| Bytes beyond the data | none | none | none | none | none | none | k coefficients and 1 byte of padding a piece |
| Size constraints | none | none | a unit fits a C `int` | none | even (`rate.rs:101`) | a symbol fits a `u16`; units above 32 KiB cut into columns | none |
| Caller's buffers | yes | **yes** | yes | yes | no: copies in and out of its work space | no: copies the column in, allocates every packet | half: `code_with_buf` writes a piece, but `Encoder::new` owns a copy of the data and a decode returns a `Vec` |
| Allocations, encode at 4+2 / 64 KiB (measured) | 2 / 80 B, the harness's | 2 / 96 B | 4 / 144 B | 2 / 96 B | 2 / 96 B | 22 / 2.1 MB | 3 / 256 KiB |
| Allocations, a decode | 3 / 112 B | 7 / 448 B | 8 / 392 B | 4 / 272 B | 3 / 128 B | 245 / 2.6 MB | 4 / 512 KiB |
| SIMD, chosen | compile time | **run time**: GFNI+AVX2, AVX2, SSSE3 (accel `x86.rs:31-99`). No AVX-512 | run time, in the C: SSE, AVX, AVX2, AVX-512. No GFNI in 2.29 | **compile time**, `-march=haswell` by default: the binary needs AVX2 | run time: AVX2, SSSE3. No AVX-512, no GFNI | run time: AVX-512, AVX2, SSSE3 | run time: GFNI+AVX-512, AVX-512BW, AVX2, SSSE3 |
| Threads | none | none | none | none (a mutex over its decode cache) | none | none | none; `rayon` behind `parallel` |
| Deterministic output | yes | yes | yes | yes | yes | yes | only with a seeded generator |
| Licence | - | MIT OR Apache-2.0 | BSD-3-Clause | MIT | MIT AND BSD-3-Clause | Apache-2.0 | BSD-3-Clause |
| MSRV | - | 1.95 | none stated | none stated | 1.82 | edition 2024 | 1.89 |
| `unsafe` | - | forbidden in the facade and core; about 100 lines in `rusty_erasure-accel` | FFI, and C | about 5 lines, and C | about 46 lines | about 59 lines | about 64 lines |
| C toolchain | no | no | **yes**: gcc, nasm, autoconf, automake, libtool, pkg-config | **yes**: a C compiler | no | no | no |
| Released | - | 2026-09-10, three weeks old | 2020-06-25 | 2022-09-23 | 2025-10-14 | 2026-03-09 | 2025-10-15 |

## Speed

Every figure is GiB a second for one core: the median of three measurements. These are the
summaries; every cell of every run is in `shoal-spike-erasure/results/`.

### At 4+2, every candidate

At a 64 KiB unit, from the build each host would run. titan and europa are both from the
`znver1` build.

| Candidate | Encode, titan cold / hot | Encode, europa cold / hot | Decode, 2 lost, titan / europa cold | Rebuild one chunk, titan / europa cold | Update one data chunk, titan / europa cold |
| --- | --- | --- | --- | --- | --- |
| XOR (2+1) | 10.4 / - | 17.8 / - | 10.4 / 17.3 (1 lost) | 5.23 / 8.61 | - |
| `rusty_erasure` | **7.62 / 9.87** | **20.2 / 97.0** | **7.62 / 20.1** | **3.03 / 6.61** | **3.25 / 6.03** |
| `isa-l` | 4.98 / 5.37 | 19.6 / 35.0 | 4.94 / 19.1 | 2.24 / 6.46 | 3.19 / 5.94 |
| `reed-solomon-erasure` | 5.63 / 8.11 | 11.8 / 33.4 | 5.60 / 11.3 | 2.51 / 3.93 | 2.85 / 4.83 |
| `reed-solomon-simd` | 4.75 / 7.51 | 9.81 / 21.3 | 0.50 / 1.41 | 0.13 / 0.35 | none |
| `raptorq` | 0.16 / 0.16 | 0.51 / 0.53 | 0.15 / 0.49 | 0.04 / 0.12 | none |
| `rlnc` | 1.49 / 2.10 | 4.76 / 8.06 | 1.77 / 5.31, and every read | 1.17 / 2.87, by recoding | none |
| `rlnc`, systematic | 2.44 / 2.96 | 7.37 / 10.8 | 1.35 / 3.18 | 0.44 / 0.93 | none |

What the table says:

- **Three Reed-Solomon crates are within a factor of two of each other cold**; `rusty_erasure`
  leads, by most on titan, where `isa-l` is the slowest of the three.
- **Hot, `rusty_erasure` pulls away on europa** (97 against 33 to 35), because GFNI does a
  GF(2^8) multiply in one instruction, which the others' nibble tables need several for.
- **`reed-solomon-simd` encodes well and decodes badly.** Its decode is an FFT over the whole
  code, a seventh of its encode on europa and a tenth on titan.
- **The two codes without S8's properties are the slowest measured**, not the fastest.

### By target

Every candidate at the four targets, at 64 KiB units, *cold / hot*. Each column is a build a
node could be given: titan's is what every node of a mixed cluster runs, and europa's three are
what an AVX-512 node runs from that same build, from a portable AVX-512 build, and from one
for its own cpu. hyperion's run is left out because it repeats titan's; it is in the results.

What each candidate ran at each target. Kernels chosen at run time are the same whatever the
build, so the build moves only `reed-solomon-erasure`:

| Candidate | titan, any build | europa `znver1` | europa `x86-64-v4` | europa native |
| --- | --- | --- | --- | --- |
| `rusty_erasure` | AVX2, named by the crate (`x86_64/avx2`) | GFNI on 256-bit registers (`x86_64/avx2_gfni`) | the same | the same |
| `isa-l` (2.29) | AVX2, by its own cpuid dispatch | AVX-512, by its dispatch | the same | the same |
| `reed-solomon-erasure` | AVX2, compiled in | **AVX2**, compiled in | **AVX-512**, compiled in | **AVX-512**, compiled in |
| `reed-solomon-simd` | AVX2 | AVX2: it has nothing wider | the same | the same |
| `raptorq` | AVX2 | AVX-512 | the same | the same |
| `rlnc` | AVX2 | GFNI on AVX-512 | the same | the same |

Only `rusty_erasure` names the set it chose; the others are what their sources' dispatch picks
on that cpu, read and not observed.

**4+2, encode, GiB/s of stripe data**, cold / hot:

| Candidate | titan `znver1` | europa `znver1` | europa `x86-64-v4` | europa native |
| --- | --- | --- | --- | --- |
| `rusty_erasure` | 7.62 / 9.87 | 20.2 / 97.0 | 20.3 / 97.6 | 20.2 / 97.3 |
| `isa-l` | 4.98 / 5.37 | 19.6 / 35.0 | 19.6 / 35.0 | 19.6 / 35.1 |
| `reed-solomon-erasure` | 5.63 / 8.11 | 11.8 / 33.4 | 12.6 / 36.2 | 12.7 / 36.4 |
| `reed-solomon-simd` | 4.75 / 7.51 | 9.81 / 21.3 | 9.83 / 24.6 | 9.45 / 21.4 |
| `raptorq` | 0.16 / 0.16 | 0.51 / 0.53 | 0.51 / 0.61 | 0.54 / 0.56 |
| `rlnc` | 1.49 / 2.10 | 4.76 / 8.06 | 4.77 / 8.40 | 4.76 / 8.06 |
| `rlnc`, systematic | 2.44 / 2.96 | 7.37 / 10.8 | 7.40 / 10.9 | 7.43 / 11.0 |

**4+2, decode with two data chunks lost, GiB/s of stripe data**, cold / hot:

| Candidate | titan `znver1` | europa `znver1` | europa `x86-64-v4` | europa native |
| --- | --- | --- | --- | --- |
| `rusty_erasure` | 7.62 / 9.80 | 20.1 / 93.7 | 20.2 / 94.2 | 20.1 / 94.4 |
| `isa-l` | 4.94 / 5.36 | 19.1 / 34.7 | 19.2 / 34.6 | 19.1 / 34.8 |
| `reed-solomon-erasure` | 5.60 / 8.04 | 11.3 / 32.3 | 12.4 / 35.3 | 12.4 / 35.7 |
| `reed-solomon-simd` | 0.50 / 0.52 | 1.41 / 1.54 | 1.53 / 1.61 | 1.38 / 1.50 |
| `raptorq` | 0.15 / 0.15 | 0.49 / 0.50 | 0.48 / 0.58 | 0.51 / 0.53 |
| `rlnc` | 1.77 / 2.30 | 5.31 / 8.93 | 5.31 / 9.12 | 5.29 / 8.98 |
| `rlnc`, systematic | 1.35 / 1.50 | 3.18 / 3.77 | 3.17 / 3.77 | 3.20 / 3.80 |

**4+2, rebuild one chunk, GiB/s of chunk rebuilt**, cold / hot:

| Candidate | titan `znver1` | europa `znver1` | europa `x86-64-v4` | europa native |
| --- | --- | --- | --- | --- |
| `rusty_erasure` | 3.03 / 4.03 | 6.61 / 28.0 | 6.63 / 28.1 | 6.61 / 28.0 |
| `isa-l` | 2.24 / 2.53 | 6.46 / 15.3 | 6.45 / 15.3 | 6.46 / 15.4 |
| `reed-solomon-erasure` | 2.51 / 3.93 | 3.93 / 15.4 | 4.47 / 16.9 | 4.48 / 17.1 |
| `reed-solomon-simd` | 0.13 / 0.13 | 0.35 / 0.39 | 0.38 / 0.40 | 0.34 / 0.37 |
| `raptorq` | 0.04 / 0.04 | 0.12 / 0.12 | 0.12 / 0.15 | 0.13 / 0.13 |
| `rlnc` | 1.17 / 1.64 | 2.87 / 5.24 | 2.86 / 5.26 | 2.86 / 5.24 |
| `rlnc`, systematic | 0.44 / 0.49 | 0.93 / 1.10 | 0.93 / 1.11 | 0.93 / 1.10 |

**4+2, update one data chunk, GiB/s of bytes changed**, cold / hot:

| Candidate | titan `znver1` | europa `znver1` | europa `x86-64-v4` | europa native |
| --- | --- | --- | --- | --- |
| `rusty_erasure` | 3.25 / 6.67 | 6.03 / 24.1 | 6.04 / 24.3 | 5.96 / 24.3 |
| `isa-l` | 3.19 / 6.86 | 5.94 / 23.2 | 5.94 / 23.6 | 5.89 / 23.7 |
| `reed-solomon-erasure` | 2.85 / 5.86 | 4.83 / 19.6 | 5.27 / 20.8 | 5.22 / 20.7 |

**10+4, encode, GiB/s of stripe data**, cold / hot:

| Candidate | titan `znver1` | europa `znver1` | europa `x86-64-v4` | europa native |
| --- | --- | --- | --- | --- |
| `rusty_erasure` | 5.29 / 5.95 | 16.8 / 36.3 | 17.0 / 36.0 | 16.8 / 34.8 |
| `isa-l` | 3.34 / 3.48 | 13.5 / 18.3 | 14.0 / 18.4 | 13.8 / 18.4 |
| `reed-solomon-erasure` | 3.35 / 3.96 | 8.77 / 16.3 | 9.66 / 17.9 | 9.68 / 17.9 |
| `reed-solomon-simd` | 3.24 / 4.26 | 9.15 / 14.1 | 9.48 / 15.4 | 8.52 / 13.1 |
| `raptorq` | 0.37 / 0.38 | 1.16 / 1.41 | 1.21 / 1.40 | 1.35 / 1.27 |
| `rlnc` | 0.76 / 0.84 | 2.96 / 3.83 | 2.99 / 3.96 | 3.02 / 3.88 |
| `rlnc`, systematic | 1.87 / 2.11 | 6.40 / 7.89 | 6.55 / 8.00 | 6.27 / 8.08 |

**10+4, decode with four data chunks lost, GiB/s of stripe data**, cold / hot:

| Candidate | titan `znver1` | europa `znver1` | europa `x86-64-v4` | europa native |
| --- | --- | --- | --- | --- |
| `rusty_erasure` | 5.33 / 5.93 | 16.8 / 35.8 | 16.8 / 35.7 | 16.7 / 34.2 |
| `isa-l` | 3.33 / 3.48 | 13.2 / 18.3 | 13.3 / 18.3 | 13.5 / 18.4 |
| `reed-solomon-erasure` | 3.35 / 3.95 | 8.58 / 16.0 | 9.48 / 17.8 | 9.49 / 17.7 |
| `reed-solomon-simd` | 0.76 / 0.80 | 2.14 / 2.33 | 2.21 / 2.42 | 2.27 / 2.48 |
| `raptorq` | 0.35 / 0.36 | 1.09 / 1.33 | 1.15 / 1.32 | 1.24 / 1.21 |
| `rlnc` | 1.00 / 1.12 | 3.63 / 4.55 | 3.65 / 4.60 | 3.73 / 4.63 |
| `rlnc`, systematic | 1.35 / 1.46 | 3.98 / 4.66 | 3.95 / 4.69 | 4.03 / 4.74 |

The four columns from europa are one cpu, so they differ only by build, and they agree to within
the runs' spread for every candidate but `reed-solomon-erasure`. That one gains 7 to 14% from
an AVX-512 build and stays at a third of `rusty_erasure` in cache. Between titan and europa the
gap is the microarchitecture and the instruction sets together. Cold it is about 2.7 times,
which is mostly the memory. Hot at 4+2 it is about ten times for `rusty_erasure`, because Zen4
has GFNI and Zen1 not only lacks it but runs AVX2 on 128-bit halves (see
[the kernel sets](#one-host-every-kernel-set)).


### An AVX-512 cluster

What a cluster whose nodes all have AVX-512 should expect, read from europa, the lab's one
AVX-512 cpu.

1. **Building for AVX-512 changes nothing for the code chosen.** `rusty_erasure` picks its
   kernels at run time, so the `znver1`, `x86-64-v4` and native builds ran the same kernels:
   4+2 encode 20.2, 20.3 and 20.2 cold, and 97.0, 97.6 and 97.3 hot. A cluster of AVX-512 nodes
   may build for `x86-64-v4` for Shoal's own code; the erasure code does not ask it to.
2. **GFNI decides, not AVX-512.** `rusty_erasure` has no AVX-512 kernels. On europa it runs its
   GFNI kernels on 256-bit registers. Held to its AVX2 kernels on the same core, it runs at
   about half in cache. So:
   - **with GFNI** (AMD Zen4 and Zen5; Intel Ice Lake and later), a node should expect the GFNI
     column below, scaled by its clock and its memory;
   - **without it** (Intel Skylake-SP and Cascade Lake, which have AVX-512 and no GFNI), the
     same crate runs its AVX2 kernels: the AVX2 column, on a core that runs AVX2 at full width
     as Zen4 does.

   No Intel cpu was measured.
3. **Out of cache, the instruction set hardly matters.** Cold, every kernel set from SSSE3 up is
   held to what memory feeds one Zen4 core. What AVX-512-class hardware buys is the in-cache
   rate, so it buys it only if a node encodes while the bytes are still in cache
   ([O84](../appendix/optimizations.md#o84-an-erasure-code-is-run-on-bytes-that-have-left-the-cache)).

`rusty_erasure` on one Zen4 core, 64 KiB units, GiB/s, *hot / cold*, from the kernels pass
except where it says main run:

| Operation | GFNI (what a Zen4, Zen5 or Ice Lake node runs) | AVX2 (what an AVX-512 node without GFNI runs) |
| --- | --- | --- |
| 4+2 encode | 97.2 / 20.4 | 50.1 / 19.3 |
| 4+2 decode, two lost | 94.4 / 20.0 | 45.9 / 19.0 |
| 4+2 rebuild one chunk (GiB/s rebuilt) | 28.0 / 6.63 | 17.6 / 6.37 |
| 4+2 update one data chunk (GiB/s changed) | 24.3 / 5.96, main run | 16 to 24 / 5.66 (see below) |
| 10+4 encode | 35.1 / 16.9 | 23.4 / 14.1 |
| 10+4 decode, four lost | 34.3 / 16.5 | 23.5 / 13.3 |

The kernels pass's in-cache updates moved between its own runs: its first run of each matched
the main runs, and the next two fell by up to a third, for the GFNI and AVX2 sets alike. Every
other cell of the pass spread by under 1%. So the update row says what the main runs say for
GFNI, and gives AVX2 as the range it moved over.


### One host, every kernel set

`rusty_erasure` forced through each kernel set the cpu has, at 64 KiB units, GiB/s,
*cold / hot*. This separates what an instruction set is worth from what a microarchitecture is.

| Host | Kernel set | 4+2 encode | 4+2 decode, two lost | 4+2 rebuild | 10+4 encode | 10+4 decode, four lost |
| --- | --- | --- | --- | --- | --- | --- |
| titan | scalar | 0.56 / 0.58 | 0.56 / 0.57 | 0.28 / 0.29 | 0.29 / 0.29 | 0.29 / 0.29 |
| titan | SSSE3 | 7.66 / 9.76 | 7.72 / 9.73 | 3.04 / 4.08 | 5.20 / 5.74 | 5.28 / 5.74 |
| titan | AVX2 | 7.68 / 9.80 | 7.63 / 9.74 | 3.05 / 4.06 | 5.39 / 5.95 | 5.33 / 5.93 |
| europa | scalar | 0.96 / 0.97 | 0.95 / 0.97 | 0.48 / 0.48 | 0.48 / 0.49 | 0.48 / 0.49 |
| europa | SSSE3 | 17.2 / 25.3 | 16.9 / 24.3 | 5.93 / 9.61 | 9.81 / 10.8 | 9.38 / 11.0 |
| europa | AVX2 | 19.3 / 50.1 | 19.0 / 45.9 | 6.37 / 17.6 | 14.1 / 23.4 | 13.3 / 23.5 |
| europa | GFNI | 20.4 / 97.2 | 20.0 / 94.4 | 6.63 / 28.0 | 16.9 / 35.1 | 16.5 / 34.3 |

- **On Zen1, AVX2 buys nothing over SSSE3.** Zen1 splits a 256-bit operation into two 128-bit
  halves, so the AVX2 kernels do the same work in the same time. A Zen1 node gets the same from
  any build.
- **On Zen4, each step doubles the in-cache rate at 4+2** (25, 50, 97). SSSE3 to AVX2 doubles
  the width, and AVX2 to GFNI replaces a nibble-table multiply with one instruction. At 10+4
  the steps are 2.2 and 1.5 times.
- **Out of cache, the three SIMD sets are within a fifth of each other at 4+2**, because memory
  is the bound. At 10+4, with more arithmetic for each byte, SSSE3 falls to 58% of GFNI.
- **The scalar set is a fallback, not an option**: a fourteenth of the SIMD sets on titan, and
  a hundredth of GFNI in cache on europa.


### Unit size

`rusty_erasure`, 4+2 encode, GiB/s:

| Run | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| titan, cold | 5.93 | 7.35 | 7.62 | 7.77 | 7.79 |
| titan, hot | 10.3 | 9.46 | 9.87 | 9.88 | 8.30 |
| europa `znver1`, cold | 15.3 | 17.2 | 20.2 | 21.0 | 21.2 |
| europa `znver1`, hot | 80.6 | 87.2 | 97.0 | 75.2 | 75.4 |

Encoding reaches its rate from 16 KiB on titan and from 64 KiB on europa. Below that a call's
fixed cost shows: at 4 KiB, a fifth of titan's rate is lost. Hot falls past 64 KiB on europa,
where a 4+2 row of 256 KiB units no longer fits a Zen4 core's 1 MiB of L2. On titan the 1 MiB
row no longer fits its L3.

**How long one call holds the core**, which [S13](isolation.md)'s yield budget has to fit. A
4+2 encode, cold, in microseconds:

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| `rusty_erasure`, titan | 2.57 | 8.30 | 32.0 | 126 | 501 |
| `rusty_erasure`, europa | 1.00 | 3.55 | 12.1 | 46.5 | 184 |
| `isa-l`, titan | 3.28 | 12.6 | 49.0 | 194 | 777 |
| `raptorq`, titan | 98.1 | 385 | 1538 | 6144 | 24554 |
| `rlnc`, titan | 10.5 | 42.2 | 164 | 622 | 3194 |

A unit row of 1 MiB units holds a Zen1 core for half a millisecond. A call has no yield
point inside it, so a unit above about 256 KiB is encoded in slices, or the budget is larger
than a table query would like.

### Layout

`rusty_erasure` cold, 64 KiB units, GiB/s of stripe data:

| Operation | Host | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- | --- |
| Encode | titan | 10.0 | 7.62 | 6.43 | 6.63 | 5.29 |
| Encode | europa | 20.1 | 20.2 | 17.3 | 19.6 | 16.8 |
| Decode, m lost | titan | 9.94 | 7.62 | 6.47 | 6.72 | 5.33 |
| Decode, m lost | europa | 19.5 | 20.1 | 17.3 | 19.4 | 16.8 |
| Rebuild one chunk (GiB/s rebuilt) | titan | 4.97 | 3.03 | 1.96 | 1.47 | 1.18 |
| Rebuild one chunk (GiB/s rebuilt) | europa | 9.75 | 6.61 | 5.08 | 3.91 | 3.20 |

Encode slows with m, the rows of parity computed, on titan, where the work is the arithmetic.
On europa, where it is the memory, it hardly moves. A rebuild reads k chunks to write one, so
its rate falls as k grows, as [X12](spikes.md#x12-recovery-and-scrub-rates) ~~assumes~~ measured:
on one device a rebuild ran at the device's rate ÷ (k + 1), and across 1 GbE a 4+2 at 0.96 of the
link's 112 MiB/s ÷ 4 ([X12](recovery-scrub-rates.md#3-the-whole-pipeline-on-one-device)).

### An update against reconstruct-write

A write that changes one data unit can fold the change into the parity, or encode the row
again from all k units. On titan cold, for each byte changed:

| Layout | Parity delta (`rusty_erasure` update) | Reconstruct-write (encode the row) | Delta reads | Reconstruct reads |
| --- | --- | --- | --- | --- |
| 4+2 | 3.25 GiB/s | 1.9 GiB/s (7.62 / 4) | the old unit and m parity chunks | k - 1 = 3 other units |
| 10+4 | 1.94 GiB/s | 0.53 GiB/s (5.29 / 10) | the same | 9 other units |

The delta is cheaper for the core as well as for the devices. The CPU is not what decides
between them; the reads are, and those are [X8](spikes.md#x8-one-small-write-three-ways)'s and
[S7](write-path.md)'s. X8 ran only a replicated pool's writes ([X8](small-writes.md#what-x8-does-not-settle)),
so a partial write's read round is still M18's to measure.

### One parity chunk

XOR against the nearest field code, 64 KiB, cold, GiB/s of stripe data:

| Host | Code | 2+1 | 4+1 | 6+1 | 8+1 | 10+1 |
| --- | --- | --- | --- | --- | --- | --- |
| titan | XOR | 10.4 | 14.5 | 16.3 | 16.9 | 16.7 |
| titan | `rusty_erasure` | 10.0 | 12.5 | 11.8 | 11.8 | 11.9 |
| europa `znver1` | XOR | 17.8 | 25.6 | 27.7 | 28.6 | 28.1 |
| europa `znver1` | `rusty_erasure` | 20.1 | 27.3 | 31.4 | 31.6 | 32.3 |

On Zen1, XOR is the fastest code at one parity chunk, as S8 assumed. On Zen4 out of cache,
both run at what memory feeds one core, and the field code's kernels feed it slightly better
than the harness's plain loop. S8's "the speed nothing else will beat" was true of Zen1 only.
XOR stays the code for m = 1 because it needs no crate, no matrix and no table, not for its
speed.

## What would have changed the design

The four results X4 named in advance:

| If | Found | So |
| --- | --- | --- |
| A code without S8's properties is several times faster | The two without them are the slowest measured: `rlnc` encodes 4+2 at 1.5 GiB/s on titan and decodes on every read at 1.8; `raptorq` runs at 0.16 | S8's preference stands |
| Recoding makes a rebuild measurably cheaper with one chunk of a stripe a slice | A recode reads k pieces, as a Reed-Solomon rebuild of one chunk reads k chunks, and runs at 1.17 GiB/s on titan against `rusty_erasure`'s 3.03. A recoded chunk is also a new combination, not the lost one, which brings back the 1 in 255 | Recoding is not a reason to take it |
| A Zen1 core encodes 4+2 under about a gibibyte a second | 7.6 GiB/s cold and 9.9 hot with `rusty_erasure`; the slowest Reed-Solomon crate is still 4.8 | Dedicated executors are not required by the code's cost; ~~[X9](spikes.md#x9-table-latency-beside-object-work) decides sharing~~ [X9](table-latency.md) found them required by the length of a step: a 1 MiB unit's slice of the encode held a table's shard about 270 µs at its p99 |
| No candidate offers an update of one data chunk | `rusty_erasure` has one in its API; ISA-L's C library has one its crate does not bind; `reed-solomon-erasure`'s public field kernels make one | M18 keeps its third step, parity delta |

## The comparison

The options side by side. The figures are 4+2 at 64 KiB; *cold / hot* where both are given.

| Option | Performance | Strengths | Weaknesses | Tradeoffs |
| --- | --- | --- | --- | --- |
| **`rusty_erasure`** (recommended) | Fastest everywhere. Encode titan 7.6 / 9.9, europa 20 / 97; decode the same as encode; update titan 3.3, europa 6.0 cold, 24 hot | Every requirement met. ISA-L's matrices byte for byte. Update and per-index recovery with reusable plans. GFNI at run time. Pure Rust, no C. Its facade and core forbid `unsafe` | Three weeks old with one author. No AVX-512 kernels, so an AVX-512 node without GFNI runs its AVX2 ones. About 100 lines of `unsafe` in its kernels to read | Youth against speed and API. Mitigated by the format being ISA-L's: another implementation reads the same bytes |
| **`isa-l`** | Second on europa cold, third on titan; a third of `rusty_erasure` hot on europa. Encode titan 5.0 / 5.4, europa 20 / 35 | ISA-L is what Ceph defaults to, for clusters made since Tentacle (`global.yaml.in:2617-2621` at `v20.2.0`, [X14](ceph-and-s3-sources.md#where-and-on-what)), mature and widely deployed. AVX-512 kernels | A six-year-old binding of one maintainer that does not bind the update. Unchecked lengths into C from a safe function. Needs nasm and autotools, and a `pkg-config` of 2022 or a system `libisal`. No GFNI in the 2.29 it bundles | The library is right and the binding is not. Using it means maintaining a binding |
| **`reed-solomon-erasure`** | Second on titan, third on europa. Encode titan 5.6 / 8.1, europa 12 / 33 | Mature, widely used, MIT. Writes into caller buffers. Decode cache built in | SIMD fixed at compile time (`haswell` unless told), so it never uses AVX-512 in a portable build, and needs AVX2 everywhere. Its C kernels are a C toolchain. No update in its API. Last released in 2022 | Stable and slow to change, which is also unmaintained |
| **`reed-solomon-simd`** | Encode competitive (titan 4.8 / 7.5); decode a tenth of encode (titan 0.50, europa 1.4) | Pure Rust, maintained. Its O(n log n) FFT scales to thousands of shards | No update. Owns its buffers, so every call copies. GF(2^16) parity matches nothing else. No AVX-512 or GFNI | Built for wide codes and large shard counts, which an object store of k + m up to 14 does not have |
| **`raptorq`** | Slowest: 0.16 on titan, 0.5 on europa, every operation | Systematic and rateless: any number of repair symbols | Fails 4 of 1,001 patterns of 10+4 at exactly k. Symbols stop at 64 KiB. Allocates megabytes a call. No decode plan, so every degraded read solves a matrix | A broadcast code: built for receivers that keep listening until they have enough, not for a stripe whose losses are fixed |
| **`rlnc`** | Encode titan 1.5 / 2.1, europa 4.8 / 8.1; every read decodes at about the encode rate | Recoding without decoding. Fast kernels: GFNI on AVX-512 | Not systematic: every read decodes, every write rewrites k + m chunks. Fails 1 in 255 at exactly k. Coefficients only from an RNG. A field (0x11B) unlike the others. A recoder that reaches undefined behaviour | A network code: its strengths are for relays that recode on the way, which a stripe on a set of devices never needs |
| **`rlnc`, systematic** | Encode titan 2.4 / 3.0, europa 7.4 / 11; decode half that | Shows `rlnc`'s kernels in S8's layout | Still 1 in 255. Still no update. Rests on two tricks the crate does not document | The kernels are fine; the code and the API are what disqualify it |
| **XOR** | m = 1 only. Titan 10 to 17, europa 18 to 29 cold | No crate, no matrix, no table. On Zen1 the fastest code there is | One parity chunk | The code for 2+1, which is all three hosts allow under a host failure domain |

**Feature support:**

| | XOR | `rusty_erasure` | `isa-l` | `reed-solomon-erasure` | `reed-solomon-simd` | `raptorq` | `rlnc` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Systematic | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ |
| Any k decodes | m = 1 | ✓ | ✓ | ✓ | ✓ | ✗ | ✗ |
| Update in the API | ✓ | ✓ | ✗ (C only) | ✗ (buildable) | ✗ | ✗ | ✗ |
| Rebuild one chunk alone | ✓ | ✓ | ✓ | ✓ | data only | ✗ | ✗ (recodes) |
| Recoding | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ |
| Caller's buffers | ✓ | ✓ | ✓ | ✓ | ✗ | ✗ | partly |
| Run-time SIMD | - | ✓ | ✓ | ✗ | ✓ | ✓ | ✓ |
| AVX-512 kernels | - | ✗ | ✓ | with `-march` | ✗ | ✓ | ✓ |
| GFNI kernels | - | ✓ | ✗ | ✗ | ✗ | ✗ | ✓ |
| No threads | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ (default) |
| No C toolchain | ✓ | ✓ | ✗ | ✗ | ✓ | ✓ | ✓ |
| Same bytes everywhere | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | seeded only |
| Format defined outside the crate | ✓ | ✓ (ISA-L's) | ✓ | ✓ (with `rusty_erasure`'s compat) | ✗ | ✓ (RFC 6330) | ✗ |

## Recommendation

**Reed-Solomon over GF(2^8), systematic, on ISA-L's Cauchy matrix, through `rusty_erasure`
`=0.4.1`, with XOR at one parity chunk.** It is the only candidate that meets every
requirement in its public API, and it is the fastest at every operation on both
microarchitectures.

- **Why the Cauchy matrix and not the Vandermonde one.** ISA-L's `gf_gen_rs_matrix` has a
  region of k and m where a decode matrix is singular, which `rusty_erasure` refuses by name;
  the Cauchy matrix inverts everywhere. All five X4 layouts are outside that region, so this
  is a margin and not a measurement.
- **Why not `isa-l`, whose library Ceph trusts.** The library is not the problem; the binding
  is. It does not bind the update, passes unchecked lengths into C from a safe function, and
  cannot build its bundled library with any current `pkg-config`. ISA-L stays in reach without
  it: its parity and `rusty_erasure`'s are the same bytes, so a binding Shoal writes later reads
  every pool `rusty_erasure` wrote.
- **What its youth costs, and what is done about it.** The crate was three weeks old when it
  was read. The format risk is closed by the byte equality with ISA-L. The code risk is the
  roughly 100 lines of `unsafe` in `rusty_erasure-accel`, which M18 reads before it adds the
  dependency, and the version is pinned exactly, as gxhash is. M18's
  `encoding_is_stable_across_builds` pins fixed vectors by literal; the digests here are a
  start for them.

## What X4 does not settle

- **The geometry.** Stripe size, chunk unit, and how data is dealt across the data chunks.
  X4 says encoding reaches its rate from 16 to 64 KiB, and that a call on a 1 MiB unit row holds
  a Zen1 core for half a millisecond. The rest is X6's, X12's and [S15](performance.md)'s. ✅ X12
  found a rebuild's cpu bounds no lab device: a Zen1 core rebuilds a 4+2 chunk of 4 MiB, its
  survivors verified and its own checksums taken, at 1.3 GiB/s
  ([X12](recovery-scrub-rates.md#1-a-cores-cpu)).
- ~~**A code inside a node.** Bytes arriving off a socket, leaving for a device, and an executor
  shared with tables. That is [X9](spikes.md#x9-table-latency-beside-object-work).~~ Run by
  [X9](table-latency.md), copied in, checksummed, encoded and written beside a table: on its own core
  it cost the table nothing, on the table's shards it moved its p99 two to four times at 1 MiB units
  ([the record](contract.md#q24-and-q15-in-part-table-latency-beside-object-work-2026-10-09)).
- ~~**Ceph's reading of the same question**, which is [X14](spikes.md#x14-ceph-and-s3-at-the-source).~~
  Read by [X14](ceph-and-s3-sources.md#3-partial-writes-and-the-shard-versions): Ceph deals 4 KiB
  units round-robin over the data shards by default, advises 16 KiB with its optimizations, and
  updates parity by delta only where the delta reads fewer shards than a reconstruction.

## What it did not measure

- **Crates S18 did not pin**, such as `klauspost`-style Go ports or Jerasure.
- **Recoding as a repair protocol** across nodes, where a recoded piece could be made from
  fewer than k pieces at each of several relays. It only times one recode from k.
- **A rotational disk or a network.** Every figure is a core and its memory.
- **An Intel AVX-512 cpu.** Every AVX-512 figure is Zen4's. An Intel core with AVX-512 and no
  GFNI is read from the kernels pass's AVX2 row ([An AVX-512 cluster](#an-avx-512-cluster)).
- **ISA-L newer than 2.29.0**, which is what `libisal-sys` bundles. Later releases add GFNI
  kernels; a binding of one would be measured before it is used.

## Defects found upstream

None is Shoal's, so none is filed on [Known Issues](../appendix/known-issues.md). Each is
written here because it is what a reader of these crates would otherwise rediscover.

| Crate | Defect | Where |
| --- | --- | --- |
| `rlnc` 0.8.7 | `Recoder::new` calls `unwrap_unchecked` on an `Err` when given less than one whole piece: undefined behaviour from a safe function | `src/full/recoder.rs:83`, `:97` |
| `rlnc` 0.8.7 | The 128-bit GFNI branch can never run: its condition is the 256-bit one's | `src/common/simd/x86/mod.rs:17` |
| `isa-l` 0.2.0 | `ec_encode_data` does not check that it was given k sources, each at least `len` long, before passing them to C: an out-of-bounds read from a safe function | `src/lib.rs:92-120` |
| `libisal-sys` 0.1.2 | The source build is unreachable with `pkg-config` 0.3.23 or later, which reports a missing library as `ProbeFailure` | `build.rs:16-17`, `:49-50` |
| `reed-solomon-erasure` 6.0.0 | `RUST_REED_SOLOMON_ERASURE_ARCH` is read but not declared to cargo, so changing it rebuilds nothing | `build.rs:160-183` |
| `reed-solomon-simd` 3.1.0 | `Avx2::new()` is a safe function that is undefined behaviour on a cpu without AVX2 | `src/engine/engine_avx2.rs:36`, `:53` |

## Related

[X4](spikes.md#x4-erasure-coding-crates-performance-and-tradeoffs) for what was planned;
[S8](erasure-coding.md) for what the code has to be;
[S18](contract.md#q20-in-part-the-code-and-the-crate-2026-10-03) for the decision;
[S1](prerequisites.md#dependencies-to-choose) for the prerequisite it closes;
[M18](milestones.md#m18-erasure-coding) for where the dependency is added;
[O84](../appendix/optimizations.md#o84-an-erasure-code-is-run-on-bytes-that-have-left-the-cache)
for what cold against hot means for a node;
[C13's Q1 spike](../distributed/protocol.md#q1-and-q13-at-m1) for the form this record copies.
