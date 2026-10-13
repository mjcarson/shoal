## rusty_erasure by kernel set

| Run | Host | CPU | Build | rse C | Governor | Core | Detected | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| titan znver1, kernel sets | titan | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | `target-cpu=znver1` | `-march=haswell` | performance | 2 | ssse3, avx2 | 3 × 200 ms | 2026-10-03T18:25:41Z |
| europa native, kernel sets | europa | AMD Ryzen 9 7945HX with Radeon Graphics | `target-cpu=native` | `-march=native` | performance | 8 | ssse3, avx2, avx512f, avx512bw, avx512vl, gfni | 3 × 200 ms | 2026-10-03T18:54:59Z |

#### titan znver1, kernel sets, cold (rows in turn, out of cache): GiB/s at 64 KiB

| Kernel set | 4+2 encode | 4+2 decode, 2 lost | 4+2 rebuild | 4+2 update | 10+4 encode | 10+4 decode, 4 lost |
| --- | --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.56 | 0.56 | 0.28 | 0.19 | 0.29 | 0.29 |
| rusty_erasure [ssse3] | 7.66 | 7.72 | 3.04 | 3.16 | 5.20 | 5.28 |
| rusty_erasure [avx2] | 7.68 | 7.63 | 3.05 | 3.24 | 5.39 | 5.33 |

#### titan znver1, kernel sets, hot (one row, in cache): GiB/s at 64 KiB

| Kernel set | 4+2 encode | 4+2 decode, 2 lost | 4+2 rebuild | 4+2 update | 10+4 encode | 10+4 decode, 4 lost |
| --- | --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.58 | 0.57 | 0.29 | 0.33 | 0.29 | 0.29 |
| rusty_erasure [ssse3] | 9.76 | 9.73 | 4.08 | 6.46 | 5.74 | 5.74 |
| rusty_erasure [avx2] | 9.80 | 9.74 | 4.06 | 6.65 | 5.95 | 5.93 |

#### titan znver1, kernel sets, cold (rows in turn, out of cache): encode at 4+2 by unit, GiB/s

| Kernel set | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.56 | 0.56 | 0.56 | 0.56 | 0.57 |
| rusty_erasure [ssse3] | 5.89 | 7.18 | 7.66 | 7.86 | 7.89 |
| rusty_erasure [avx2] | 6.00 | 7.40 | 7.68 | 7.82 | 7.83 |

#### titan znver1, kernel sets, hot (one row, in cache): encode at 4+2 by unit, GiB/s

| Kernel set | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.57 | 0.58 | 0.58 | 0.57 | 0.56 |
| rusty_erasure [ssse3] | 9.93 | 9.74 | 9.76 | 9.97 | 8.19 |
| rusty_erasure [avx2] | 10.3 | 9.50 | 9.80 | 9.90 | 8.41 |

#### europa native, kernel sets, cold (rows in turn, out of cache): GiB/s at 64 KiB

| Kernel set | 4+2 encode | 4+2 decode, 2 lost | 4+2 rebuild | 4+2 update | 10+4 encode | 10+4 decode, 4 lost |
| --- | --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.96 | 0.95 | 0.48 | 0.73 | 0.48 | 0.48 |
| rusty_erasure [ssse3] | 17.2 | 16.9 | 5.93 | 5.76 | 9.81 | 9.38 |
| rusty_erasure [avx2] | 19.3 | 19.0 | 6.37 | 5.66 | 14.1 | 13.3 |
| rusty_erasure [gfni] | 20.4 | 20.0 | 6.63 | 5.99 | 16.9 | 16.5 |

#### europa native, kernel sets, hot (one row, in cache): GiB/s at 64 KiB

| Kernel set | 4+2 encode | 4+2 decode, 2 lost | 4+2 rebuild | 4+2 update | 10+4 encode | 10+4 decode, 4 lost |
| --- | --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.97 | 0.97 | 0.48 | 0.76 | 0.49 | 0.49 |
| rusty_erasure [ssse3] | 25.3 | 24.3 | 9.61 | 17.1 | 10.8 | 11.0 |
| rusty_erasure [avx2] | 50.1 | 45.9 | 17.6 | 16.9 | 23.4 | 23.5 |
| rusty_erasure [gfni] | 97.2 | 94.4 | 28.0 | 18.5 | 35.1 | 34.3 |

#### europa native, kernel sets, cold (rows in turn, out of cache): encode at 4+2 by unit, GiB/s

| Kernel set | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.96 | 0.96 | 0.96 | 0.95 | 0.96 |
| rusty_erasure [ssse3] | 12.2 | 14.9 | 17.2 | 17.9 | 18.8 |
| rusty_erasure [avx2] | 14.5 | 16.5 | 19.3 | 20.1 | 20.4 |
| rusty_erasure [gfni] | 15.4 | 17.6 | 20.4 | 21.3 | 21.5 |

#### europa native, kernel sets, hot (one row, in cache): encode at 4+2 by unit, GiB/s

| Kernel set | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.97 | 0.96 | 0.97 | 0.97 | 0.97 |
| rusty_erasure [ssse3] | 23.1 | 23.9 | 25.3 | 25.0 | 25.1 |
| rusty_erasure [avx2] | 42.5 | 44.2 | 50.1 | 41.9 | 37.6 |
| rusty_erasure [gfni] | 88.1 | 87.1 | 97.2 | 75.5 | 74.4 |

## Runs

| Run | Host | CPU | Build | rse C | Governor | Core | Detected | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| titan znver1 | titan | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | `target-cpu=znver1` | `-march=haswell` | performance | 2 | ssse3, avx2 | 3 × 200 ms | 2026-10-03T18:09:17Z |
| hyperion znver1 | hyperion | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | `target-cpu=znver1` | `-march=haswell` | performance | 2 | ssse3, avx2 | 3 × 200 ms | 2026-10-03T18:09:17Z |
| europa znver1 | europa | AMD Ryzen 9 7945HX with Radeon Graphics | `target-cpu=znver1` | `-march=haswell` | performance | 8 | ssse3, avx2, avx512f, avx512bw, avx512vl, gfni | 3 × 200 ms | 2026-10-03T18:09:17Z |
| europa x86-64-v4 | europa | AMD Ryzen 9 7945HX with Radeon Graphics | `target-cpu=x86-64-v4` | `-march=x86-64-v4` | performance | 8 | ssse3, avx2, avx512f, avx512bw, avx512vl, gfni | 3 × 200 ms | 2026-10-03T18:39:45Z |
| europa native | europa | AMD Ryzen 9 7945HX with Radeon Graphics | `target-cpu=native` | `-march=native` | performance | 8 | ssse3, avx2, avx512f, avx512bw, avx512vl, gfni | 3 × 200 ms | 2026-10-03T18:24:31Z |

## Correctness

### Loss patterns (titan znver1)

| Candidate | Layouts | Patterns | Decoded | Undecodable | Undecodable at m lost | Wrong bytes | Rebuilds | Rebuilt wrong | Recoded, then undecodable | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| xor | 5 | 25 | 25 | 0 | 0 of 25 | 0 | 25 | 0 | 0 | 0 |
| rusty_erasure | 17 | 2186 | 2186 | 0 | 0 of 1476 | 0 | 115 | 0 | 0 | 0 |
| isa-l | 17 | 2186 | 2186 | 0 | 0 of 1476 | 0 | 115 | 0 | 0 | 0 |
| reed-solomon-erasure | 17 | 2186 | 2186 | 0 | 0 of 1476 | 0 | 115 | 0 | 0 | 0 |
| reed-solomon-simd | 17 | 2186 | 2186 | 0 | 0 of 1476 | 0 | 115 | 0 | 0 | 0 |
| raptorq | 17 | 2186 | 2182 | 4 | 4 of 1476 | 0 | 115 | 0 | 0 | 0 |
| rlnc | 17 | 2186 | 2182 | 4 | 4 of 1476 | 0 | 115 | 0 | 1 | 0 |
| rlnc, systematic | 17 | 2186 | 2184 | 2 | 2 of 1476 | 0 | 115 | 0 | 0 | 0 |

- raptorq at 10+4: 4 of 1470 patterns undecodable (4 of 1001 with m lost)
- rlnc at 4+3: 1 of 63 patterns undecodable (1 of 35 with m lost)
- rlnc at 5+2: 1 of 28 patterns undecodable (1 of 21 with m lost)
- rlnc at 10+4: 2 of 1470 patterns undecodable (2 of 1001 with m lost)
- rlnc, systematic at 8+3: 1 of 231 patterns undecodable (1 of 165 with m lost)
- rlnc, systematic at 10+4: 1 of 1470 patterns undecodable (1 of 1001 with m lost)

### Random coefficients: k chunks that fail to decode

| Candidate | Layout | Trials | Failures | Fraction | Wrong bytes |
| --- | --- | --- | --- | --- | --- |
| rlnc | 4+2 | 100000 | 388 | 0.388% | 0 |
| rlnc | 6+3 | 100000 | 402 | 0.402% | 0 |
| rlnc | 10+4 | 100000 | 383 | 0.383% | 0 |
| rlnc, systematic | 4+2 | 100000 | 364 | 0.364% | 0 |
| rlnc, systematic | 6+3 | 100000 | 367 | 0.367% | 0 |
| rlnc, systematic | 10+4 | 100000 | 408 | 0.408% | 0 |

### Partial updates against a fresh encode

| Candidate | Layout | Updates | Parity equals a fresh encode | Note |
| --- | --- | --- | --- | --- |
| xor | 2+1 | 500 | yes |  |
| rusty_erasure | 2+1 | 500 | yes |  |
| rusty_erasure | 4+2 | 500 | yes |  |
| rusty_erasure | 10+4 | 500 | yes |  |
| isa-l | 2+1 | 500 | yes |  |
| isa-l | 4+2 | 500 | yes |  |
| isa-l | 10+4 | 500 | yes |  |
| reed-solomon-erasure | 2+1 | 500 | yes |  |
| reed-solomon-erasure | 4+2 | 500 | yes |  |
| reed-solomon-erasure | 10+4 | 500 | yes |  |
| reed-solomon-simd | 2+1 | 0 | - | no update in the API; the FFT form exposes no coefficient to fold a change with |
| reed-solomon-simd | 4+2 | 0 | - | no update in the API; the FFT form exposes no coefficient to fold a change with |
| reed-solomon-simd | 10+4 | 0 | - | no update in the API; the FFT form exposes no coefficient to fold a change with |
| raptorq | 2+1 | 0 | - | no update in the API; a repair symbol depends on the block's intermediate symbols |
| raptorq | 4+2 | 0 | - | no update in the API; a repair symbol depends on the block's intermediate symbols |
| raptorq | 10+4 | 0 | - | no update in the API; a repair symbol depends on the block's intermediate symbols |
| rlnc | 2+1 | 0 | - | not systematic: a write of any byte rewrites every chunk |
| rlnc | 4+2 | 0 | - | not systematic: a write of any byte rewrites every chunk |
| rlnc | 10+4 | 0 | - | not systematic: a write of any byte rewrites every chunk |
| rlnc, systematic | 2+1 | 0 | - | no update in the API; its GF(2^8) kernels are in a private module |
| rlnc, systematic | 4+2 | 0 | - | no update in the API; its GF(2^8) kernels are in a private module |
| rlnc, systematic | 10+4 | 0 | - | no update in the API; its GF(2^8) kernels are in a private module |

### The same input, the same bytes?

| Candidate | Layout | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native | Same in every run |
| --- | --- | --- | --- | --- | --- | --- | --- |
| xor | 2+1 | `9e4fbbc1` | `9e4fbbc1` | `9e4fbbc1` | `9e4fbbc1` | `9e4fbbc1` | yes |
| rusty_erasure | 2+1 | `978cca72` | `978cca72` | `978cca72` | `978cca72` | `978cca72` | yes |
| rusty_erasure | 4+2 | `785f7e13` | `785f7e13` | `785f7e13` | `785f7e13` | `785f7e13` | yes |
| rusty_erasure | 6+3 | `e2ac4410` | `e2ac4410` | `e2ac4410` | `e2ac4410` | `e2ac4410` | yes |
| rusty_erasure | 8+3 | `14840a39` | `14840a39` | `14840a39` | `14840a39` | `14840a39` | yes |
| rusty_erasure | 10+4 | `33ccf46e` | `33ccf46e` | `33ccf46e` | `33ccf46e` | `33ccf46e` | yes |
| isa-l | 2+1 | `978cca72` | `978cca72` | `978cca72` | `978cca72` | `978cca72` | yes |
| isa-l | 4+2 | `785f7e13` | `785f7e13` | `785f7e13` | `785f7e13` | `785f7e13` | yes |
| isa-l | 6+3 | `e2ac4410` | `e2ac4410` | `e2ac4410` | `e2ac4410` | `e2ac4410` | yes |
| isa-l | 8+3 | `14840a39` | `14840a39` | `14840a39` | `14840a39` | `14840a39` | yes |
| isa-l | 10+4 | `33ccf46e` | `33ccf46e` | `33ccf46e` | `33ccf46e` | `33ccf46e` | yes |
| reed-solomon-erasure | 2+1 | `6c6dccfc` | `6c6dccfc` | `6c6dccfc` | `6c6dccfc` | `6c6dccfc` | yes |
| reed-solomon-erasure | 4+2 | `b50cf79c` | `b50cf79c` | `b50cf79c` | `b50cf79c` | `b50cf79c` | yes |
| reed-solomon-erasure | 6+3 | `63237f99` | `63237f99` | `63237f99` | `63237f99` | `63237f99` | yes |
| reed-solomon-erasure | 8+3 | `e7a13244` | `e7a13244` | `e7a13244` | `e7a13244` | `e7a13244` | yes |
| reed-solomon-erasure | 10+4 | `c5cf9ddf` | `c5cf9ddf` | `c5cf9ddf` | `c5cf9ddf` | `c5cf9ddf` | yes |
| reed-solomon-simd | 2+1 | `9e4fbbc1` | `9e4fbbc1` | `9e4fbbc1` | `9e4fbbc1` | `9e4fbbc1` | yes |
| reed-solomon-simd | 4+2 | `79be2e70` | `79be2e70` | `79be2e70` | `79be2e70` | `79be2e70` | yes |
| reed-solomon-simd | 6+3 | `99ca406b` | `99ca406b` | `99ca406b` | `99ca406b` | `99ca406b` | yes |
| reed-solomon-simd | 8+3 | `7c00ce7f` | `7c00ce7f` | `7c00ce7f` | `7c00ce7f` | `7c00ce7f` | yes |
| reed-solomon-simd | 10+4 | `a05715e9` | `a05715e9` | `a05715e9` | `a05715e9` | `a05715e9` | yes |
| raptorq | 2+1 | `fb35776b` | `fb35776b` | `fb35776b` | `fb35776b` | `fb35776b` | yes |
| raptorq | 4+2 | `39ee781a` | `39ee781a` | `39ee781a` | `39ee781a` | `39ee781a` | yes |
| raptorq | 6+3 | `8b53b41f` | `8b53b41f` | `8b53b41f` | `8b53b41f` | `8b53b41f` | yes |
| raptorq | 8+3 | `a2d88133` | `a2d88133` | `a2d88133` | `a2d88133` | `a2d88133` | yes |
| raptorq | 10+4 | `6688ab32` | `6688ab32` | `6688ab32` | `6688ab32` | `6688ab32` | yes |
| rlnc | 2+1 | `6ca866b3` | `6ca866b3` | `6ca866b3` | `6ca866b3` | `6ca866b3` | yes |
| rlnc | 4+2 | `df8ca30f` | `df8ca30f` | `df8ca30f` | `df8ca30f` | `df8ca30f` | yes |
| rlnc | 6+3 | `df501fa1` | `df501fa1` | `df501fa1` | `df501fa1` | `df501fa1` | yes |
| rlnc | 8+3 | `4ed18b99` | `4ed18b99` | `4ed18b99` | `4ed18b99` | `4ed18b99` | yes |
| rlnc | 10+4 | `8ab623e6` | `8ab623e6` | `8ab623e6` | `8ab623e6` | `8ab623e6` | yes |
| rlnc, systematic | 2+1 | `fcad899c` | `fcad899c` | `fcad899c` | `fcad899c` | `fcad899c` | yes |
| rlnc, systematic | 4+2 | `e47124cf` | `e47124cf` | `e47124cf` | `e47124cf` | `e47124cf` | yes |
| rlnc, systematic | 6+3 | `9e4293bc` | `9e4293bc` | `9e4293bc` | `9e4293bc` | `9e4293bc` | yes |
| rlnc, systematic | 8+3 | `f83dc2f2` | `f83dc2f2` | `f83dc2f2` | `f83dc2f2` | `f83dc2f2` | yes |
| rlnc, systematic | 10+4 | `a0348f08` | `a0348f08` | `a0348f08` | `a0348f08` | `a0348f08` | yes |

### Parity that is the same bytes

| Encoded by | Compared with | Layouts | Equal at every layout |
| --- | --- | --- | --- |
| isa-l | rusty_erasure | 2+1, 4+2, 6+3, 8+3, 10+4 | yes |
| reed-solomon-erasure | rusty_erasure, compat matrix | 2+1, 4+2, 6+3, 8+3, 10+4 | yes |

## Facts

### Threads, kernels and allocations (titan znver1)

| Candidate | Kernels | Threads before → after | encode | decode-1 | rebuild | update |
| --- | --- | --- | --- | --- | --- | --- |
| xor | - | 1 → 1 | 2 / 80 B | 5 / 192 B | 4 / 160 B | 2 / 80 B |
| rusty_erasure | x86_64/avx2 | 1 → 1 | 2 / 96 B | 7 / 448 B | 7 / 448 B | 4 / 144 B |
| isa-l | - | 1 → 1 | 4 / 144 B | 8 / 392 B | 8 / 392 B | 3 / 112 B |
| reed-solomon-erasure | - | 1 → 1 | 2 / 96 B | 4 / 272 B | 3 / 240 B | 2 / 96 B |
| reed-solomon-simd | - | 1 → 1 | 2 / 96 B | 3 / 128 B | 3 / 128 B | n/a |
| raptorq | - | 1 → 1 | 22 / 2163664 B | 245 / 2636820 B | 245 / 2636820 B | n/a |
| rlnc | - | 1 → 1 | 3 / 262308 B | 4 / 524472 B | 6 / 524492 B | n/a |
| rlnc, systematic | - | 1 → 1 | 6 / 655511 B | 5 / 655513 B | 5 / 655513 B | n/a |

### Threads, kernels and allocations (hyperion znver1)

| Candidate | Kernels | Threads before → after | encode | decode-1 | rebuild | update |
| --- | --- | --- | --- | --- | --- | --- |
| xor | - | 1 → 1 | 2 / 80 B | 5 / 192 B | 4 / 160 B | 2 / 80 B |
| rusty_erasure | x86_64/avx2 | 1 → 1 | 2 / 96 B | 7 / 448 B | 7 / 448 B | 4 / 144 B |
| isa-l | - | 1 → 1 | 4 / 144 B | 8 / 392 B | 8 / 392 B | 3 / 112 B |
| reed-solomon-erasure | - | 1 → 1 | 2 / 96 B | 4 / 272 B | 3 / 240 B | 2 / 96 B |
| reed-solomon-simd | - | 1 → 1 | 2 / 96 B | 3 / 128 B | 3 / 128 B | n/a |
| raptorq | - | 1 → 1 | 22 / 2163664 B | 245 / 2636820 B | 245 / 2636820 B | n/a |
| rlnc | - | 1 → 1 | 3 / 262308 B | 4 / 524472 B | 6 / 524492 B | n/a |
| rlnc, systematic | - | 1 → 1 | 6 / 655511 B | 5 / 655513 B | 5 / 655513 B | n/a |

### Threads, kernels and allocations (europa znver1)

| Candidate | Kernels | Threads before → after | encode | decode-1 | rebuild | update |
| --- | --- | --- | --- | --- | --- | --- |
| xor | - | 1 → 1 | 2 / 80 B | 5 / 192 B | 4 / 160 B | 2 / 80 B |
| rusty_erasure | x86_64/avx2_gfni | 1 → 1 | 2 / 96 B | 7 / 448 B | 7 / 448 B | 3 / 128 B |
| isa-l | - | 1 → 1 | 4 / 144 B | 8 / 392 B | 8 / 392 B | 3 / 112 B |
| reed-solomon-erasure | - | 1 → 1 | 2 / 96 B | 4 / 272 B | 3 / 240 B | 2 / 96 B |
| reed-solomon-simd | - | 1 → 1 | 2 / 96 B | 3 / 128 B | 3 / 128 B | n/a |
| raptorq | - | 1 → 1 | 22 / 2163664 B | 245 / 2636820 B | 245 / 2636820 B | n/a |
| rlnc | - | 1 → 1 | 3 / 262308 B | 4 / 524472 B | 6 / 524492 B | n/a |
| rlnc, systematic | - | 1 → 1 | 6 / 655511 B | 5 / 655513 B | 5 / 655513 B | n/a |

### Threads, kernels and allocations (europa x86-64-v4)

| Candidate | Kernels | Threads before → after | encode | decode-1 | rebuild | update |
| --- | --- | --- | --- | --- | --- | --- |
| xor | - | 1 → 1 | 2 / 80 B | 5 / 192 B | 4 / 160 B | 2 / 80 B |
| rusty_erasure | x86_64/avx2_gfni | 1 → 1 | 2 / 96 B | 7 / 448 B | 7 / 448 B | 3 / 128 B |
| isa-l | - | 1 → 1 | 4 / 144 B | 8 / 392 B | 8 / 392 B | 3 / 112 B |
| reed-solomon-erasure | - | 1 → 1 | 2 / 96 B | 4 / 272 B | 3 / 240 B | 2 / 96 B |
| reed-solomon-simd | - | 1 → 1 | 2 / 96 B | 3 / 128 B | 3 / 128 B | n/a |
| raptorq | - | 1 → 1 | 22 / 2163664 B | 245 / 2636820 B | 245 / 2636820 B | n/a |
| rlnc | - | 1 → 1 | 3 / 262308 B | 4 / 524472 B | 6 / 524492 B | n/a |
| rlnc, systematic | - | 1 → 1 | 6 / 655511 B | 5 / 655513 B | 5 / 655513 B | n/a |

### Threads, kernels and allocations (europa native)

| Candidate | Kernels | Threads before → after | encode | decode-1 | rebuild | update |
| --- | --- | --- | --- | --- | --- | --- |
| xor | - | 1 → 1 | 2 / 80 B | 5 / 192 B | 4 / 160 B | 2 / 80 B |
| rusty_erasure | x86_64/avx2_gfni | 1 → 1 | 2 / 96 B | 7 / 448 B | 7 / 448 B | 3 / 128 B |
| isa-l | - | 1 → 1 | 4 / 144 B | 8 / 392 B | 8 / 392 B | 3 / 112 B |
| reed-solomon-erasure | - | 1 → 1 | 2 / 96 B | 4 / 272 B | 3 / 240 B | 2 / 96 B |
| reed-solomon-simd | - | 1 → 1 | 2 / 96 B | 3 / 128 B | 3 / 128 B | n/a |
| raptorq | - | 1 → 1 | 22 / 2163664 B | 245 / 2636820 B | 245 / 2636820 B | n/a |
| rlnc | - | 1 → 1 | 3 / 262308 B | 4 / 524472 B | 6 / 524492 B | n/a |
| rlnc, systematic | - | 1 → 1 | 6 / 655511 B | 5 / 655513 B | 5 / 655513 B | n/a |

## By target, 4+2 at 64 KiB, cold (rows in turn, out of cache)

#### Encode, GiB/s of stripe data

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 7.62 | 7.64 | 20.2 | 20.3 | 20.2 |
| isa-l | 4.98 | 4.97 | 19.6 | 19.6 | 19.6 |
| reed-solomon-erasure | 5.63 | 5.62 | 11.8 | 12.6 | 12.7 |
| reed-solomon-simd | 4.75 | 4.77 | 9.81 | 9.83 | 9.45 |
| raptorq | 0.16 | 0.16 | 0.51 | 0.51 | 0.54 |
| rlnc | 1.49 | 1.49 | 4.76 | 4.77 | 4.76 |
| rlnc, systematic | 2.44 | 2.44 | 7.37 | 7.40 | 7.43 |

#### Decode with one data chunk lost, GiB/s of stripe data

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 12.1 | 12.3 | 26.4 | 26.5 | 26.6 |
| isa-l | 8.94 | 9.03 | 25.9 | 25.7 | 25.9 |
| reed-solomon-erasure | 10.1 | 10.1 | 15.7 | 17.9 | 17.9 |
| reed-solomon-simd | 0.51 | 0.51 | 1.42 | 1.54 | 1.39 |
| raptorq | 0.15 | 0.15 | 0.48 | 0.48 | 0.51 |
| rlnc | 1.77 | 1.78 | 5.28 | 5.28 | 5.28 |
| rlnc, systematic | 1.77 | 1.80 | 3.72 | 3.73 | 3.74 |

#### Decode with 2 data chunks lost, GiB/s of stripe data

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 7.62 | 7.64 | 20.1 | 20.2 | 20.1 |
| isa-l | 4.94 | 4.95 | 19.1 | 19.2 | 19.1 |
| reed-solomon-erasure | 5.60 | 5.60 | 11.3 | 12.4 | 12.4 |
| reed-solomon-simd | 0.50 | 0.50 | 1.41 | 1.53 | 1.38 |
| raptorq | 0.15 | 0.15 | 0.49 | 0.48 | 0.51 |
| rlnc | 1.77 | 1.78 | 5.31 | 5.31 | 5.29 |
| rlnc, systematic | 1.35 | 1.35 | 3.18 | 3.17 | 3.20 |

#### Rebuild one chunk, GiB/s of chunk rebuilt

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.03 | 3.06 | 6.61 | 6.63 | 6.61 |
| isa-l | 2.24 | 2.26 | 6.46 | 6.45 | 6.46 |
| reed-solomon-erasure | 2.51 | 2.51 | 3.93 | 4.47 | 4.48 |
| reed-solomon-simd | 0.13 | 0.13 | 0.35 | 0.38 | 0.34 |
| raptorq | 0.04 | 0.04 | 0.12 | 0.12 | 0.13 |
| rlnc | 1.17 | 1.17 | 2.87 | 2.86 | 2.86 |
| rlnc, systematic | 0.44 | 0.45 | 0.93 | 0.93 | 0.93 |

#### Update one data chunk, GiB/s of bytes changed

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.25 | 3.25 | 6.03 | 6.04 | 5.96 |
| isa-l | 3.19 | 3.13 | 5.94 | 5.94 | 5.89 |
| reed-solomon-erasure | 2.85 | 2.83 | 4.83 | 5.27 | 5.22 |

## By target, 10+4 at 64 KiB, cold (rows in turn, out of cache)

#### Encode, GiB/s of stripe data

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.29 | 5.26 | 16.8 | 17.0 | 16.8 |
| isa-l | 3.34 | 3.38 | 13.5 | 14.0 | 13.8 |
| reed-solomon-erasure | 3.35 | 3.35 | 8.77 | 9.66 | 9.68 |
| reed-solomon-simd | 3.24 | 3.23 | 9.15 | 9.48 | 8.52 |
| raptorq | 0.37 | 0.37 | 1.16 | 1.21 | 1.35 |
| rlnc | 0.76 | 0.76 | 2.96 | 2.99 | 3.02 |
| rlnc, systematic | 1.87 | 1.87 | 6.40 | 6.55 | 6.27 |

#### Decode with one data chunk lost, GiB/s of stripe data

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 11.8 | 11.8 | 31.9 | 32.6 | 32.0 |
| isa-l | 8.51 | 8.53 | 29.7 | 29.7 | 29.6 |
| reed-solomon-erasure | 11.7 | 11.7 | 17.6 | 20.1 | 20.1 |
| reed-solomon-simd | 0.77 | 0.77 | 2.17 | 2.25 | 2.30 |
| raptorq | 0.35 | 0.35 | 1.06 | 1.12 | 1.24 |
| rlnc | 0.99 | 0.99 | 3.63 | 3.63 | 3.72 |
| rlnc, systematic | 2.61 | 2.62 | 5.82 | 5.85 | 5.88 |

#### Decode with 4 data chunks lost, GiB/s of stripe data

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.33 | 5.22 | 16.8 | 16.8 | 16.7 |
| isa-l | 3.33 | 3.32 | 13.2 | 13.3 | 13.5 |
| reed-solomon-erasure | 3.35 | 3.35 | 8.58 | 9.48 | 9.49 |
| reed-solomon-simd | 0.76 | 0.76 | 2.14 | 2.21 | 2.27 |
| raptorq | 0.35 | 0.36 | 1.09 | 1.15 | 1.24 |
| rlnc | 1.00 | 0.99 | 3.63 | 3.65 | 3.73 |
| rlnc, systematic | 1.35 | 1.35 | 3.98 | 3.95 | 4.03 |

#### Rebuild one chunk, GiB/s of chunk rebuilt

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.18 | 1.18 | 3.20 | 3.25 | 3.20 |
| isa-l | 0.85 | 0.85 | 2.97 | 2.97 | 2.96 |
| reed-solomon-erasure | 1.16 | 1.16 | 1.76 | 2.01 | 2.00 |
| reed-solomon-simd | 0.08 | 0.08 | 0.22 | 0.23 | 0.23 |
| raptorq | 0.03 | 0.03 | 0.11 | 0.11 | 0.12 |
| rlnc | 0.47 | 0.48 | 1.40 | 1.41 | 1.42 |
| rlnc, systematic | 0.26 | 0.26 | 0.58 | 0.58 | 0.59 |

#### Update one data chunk, GiB/s of bytes changed

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.94 | 1.93 | 4.54 | 4.55 | 4.49 |
| isa-l | 1.84 | 1.86 | 4.49 | 4.50 | 4.43 |
| reed-solomon-erasure | 1.56 | 1.56 | 2.97 | 3.34 | 3.30 |

## By target, 4+2 encode at 4 KiB, cold (rows in turn, out of cache)

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.93 | 5.87 | 15.3 | 15.1 | 15.3 |
| isa-l | 4.65 | 4.62 | 14.5 | 14.5 | 14.6 |
| reed-solomon-erasure | 5.57 | 5.49 | 13.8 | 15.1 | 15.6 |
| reed-solomon-simd | 4.47 | 4.51 | 7.74 | 7.88 | 7.99 |
| raptorq | 0.16 | 0.16 | 0.55 | 0.68 | 0.56 |
| rlnc | 1.45 | 1.46 | 3.72 | 3.84 | 3.88 |
| rlnc, systematic | 2.07 | 2.05 | 5.68 | 5.75 | 5.95 |

## By target, 4+2 encode at 1 MiB, cold (rows in turn, out of cache)

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 7.79 | 7.81 | 21.2 | 21.2 | 21.2 |
| isa-l | 5.03 | 5.02 | 20.6 | 20.6 | 20.5 |
| reed-solomon-erasure | 4.61 | 4.61 | 13.9 | 14.4 | 14.3 |
| reed-solomon-simd | 2.92 | 2.89 | 11.7 | 11.6 | 11.8 |
| raptorq | 0.16 | 0.16 | 0.51 | 0.60 | 0.59 |
| rlnc | 1.22 | 1.23 | 4.88 | 4.87 | 4.89 |
| rlnc, systematic | 1.36 | 1.40 | 7.46 | 7.47 | 7.46 |

## By target, 4+2 at 64 KiB, hot (one row, in cache)

#### Encode, GiB/s of stripe data

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 9.87 | 9.78 | 97.0 | 97.6 | 97.3 |
| isa-l | 5.37 | 5.28 | 35.0 | 35.0 | 35.1 |
| reed-solomon-erasure | 8.11 | 8.07 | 33.4 | 36.2 | 36.4 |
| reed-solomon-simd | 7.51 | 7.42 | 21.3 | 24.6 | 21.4 |
| raptorq | 0.16 | 0.16 | 0.53 | 0.61 | 0.56 |
| rlnc | 2.10 | 2.09 | 8.06 | 8.40 | 8.06 |
| rlnc, systematic | 2.96 | 2.95 | 10.8 | 10.9 | 11.0 |

#### Decode with one data chunk lost, GiB/s of stripe data

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 16.1 | 16.2 | 112 | 112 | 112 |
| isa-l | 10.1 | 9.80 | 61.3 | 61.2 | 61.5 |
| reed-solomon-erasure | 15.8 | 15.7 | 62.8 | 68.2 | 69.2 |
| reed-solomon-simd | 0.52 | 0.53 | 1.54 | 1.62 | 1.50 |
| raptorq | 0.15 | 0.15 | 0.50 | 0.58 | 0.52 |
| rlnc | 2.30 | 2.30 | 8.91 | 9.08 | 9.02 |
| rlnc, systematic | 1.96 | 1.99 | 4.42 | 4.43 | 4.41 |

#### Decode with 2 data chunks lost, GiB/s of stripe data

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 9.80 | 9.75 | 93.7 | 94.2 | 94.4 |
| isa-l | 5.36 | 5.30 | 34.7 | 34.6 | 34.8 |
| reed-solomon-erasure | 8.04 | 7.99 | 32.3 | 35.3 | 35.7 |
| reed-solomon-simd | 0.52 | 0.52 | 1.54 | 1.61 | 1.50 |
| raptorq | 0.15 | 0.15 | 0.50 | 0.58 | 0.53 |
| rlnc | 2.30 | 2.30 | 8.93 | 9.12 | 8.98 |
| rlnc, systematic | 1.50 | 1.50 | 3.77 | 3.77 | 3.80 |

#### Rebuild one chunk, GiB/s of chunk rebuilt

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.03 | 4.06 | 28.0 | 28.1 | 28.0 |
| isa-l | 2.53 | 2.45 | 15.3 | 15.3 | 15.4 |
| reed-solomon-erasure | 3.93 | 3.90 | 15.4 | 16.9 | 17.1 |
| reed-solomon-simd | 0.13 | 0.13 | 0.39 | 0.40 | 0.37 |
| raptorq | 0.04 | 0.04 | 0.12 | 0.15 | 0.13 |
| rlnc | 1.64 | 1.64 | 5.24 | 5.26 | 5.24 |
| rlnc, systematic | 0.49 | 0.50 | 1.10 | 1.11 | 1.10 |

#### Update one data chunk, GiB/s of bytes changed

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 6.67 | 6.66 | 24.1 | 24.3 | 24.3 |
| isa-l | 6.86 | 6.85 | 23.2 | 23.6 | 23.7 |
| reed-solomon-erasure | 5.86 | 5.88 | 19.6 | 20.8 | 20.7 |

## By target, 10+4 at 64 KiB, hot (one row, in cache)

#### Encode, GiB/s of stripe data

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.95 | 5.94 | 36.3 | 36.0 | 34.8 |
| isa-l | 3.48 | 3.47 | 18.3 | 18.4 | 18.4 |
| reed-solomon-erasure | 3.96 | 3.95 | 16.3 | 17.9 | 17.9 |
| reed-solomon-simd | 4.26 | 4.25 | 14.1 | 15.4 | 13.1 |
| raptorq | 0.38 | 0.38 | 1.41 | 1.40 | 1.27 |
| rlnc | 0.84 | 0.84 | 3.83 | 3.96 | 3.88 |
| rlnc, systematic | 2.11 | 2.13 | 7.89 | 8.00 | 8.08 |

#### Decode with one data chunk lost, GiB/s of stripe data

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.8 | 15.7 | 96.1 | 98.5 | 93.6 |
| isa-l | 9.24 | 9.21 | 56.2 | 55.7 | 56.2 |
| reed-solomon-erasure | 15.6 | 15.5 | 60.0 | 65.9 | 67.4 |
| reed-solomon-simd | 0.80 | 0.80 | 2.34 | 2.43 | 2.49 |
| raptorq | 0.35 | 0.36 | 1.31 | 1.29 | 1.13 |
| rlnc | 1.11 | 1.12 | 4.53 | 4.59 | 4.65 |
| rlnc, systematic | 2.94 | 2.98 | 7.22 | 7.22 | 7.24 |

#### Decode with 4 data chunks lost, GiB/s of stripe data

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.93 | 5.91 | 35.8 | 35.7 | 34.2 |
| isa-l | 3.48 | 3.48 | 18.3 | 18.3 | 18.4 |
| reed-solomon-erasure | 3.95 | 3.94 | 16.0 | 17.8 | 17.7 |
| reed-solomon-simd | 0.80 | 0.80 | 2.33 | 2.42 | 2.48 |
| raptorq | 0.36 | 0.36 | 1.33 | 1.32 | 1.21 |
| rlnc | 1.12 | 1.10 | 4.55 | 4.60 | 4.63 |
| rlnc, systematic | 1.46 | 1.48 | 4.66 | 4.69 | 4.74 |

#### Rebuild one chunk, GiB/s of chunk rebuilt

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.58 | 1.56 | 9.60 | 9.85 | 9.25 |
| isa-l | 0.92 | 0.92 | 5.62 | 5.58 | 5.63 |
| reed-solomon-erasure | 1.55 | 1.55 | 5.96 | 6.56 | 6.69 |
| reed-solomon-simd | 0.08 | 0.08 | 0.23 | 0.24 | 0.25 |
| raptorq | 0.04 | 0.04 | 0.13 | 0.13 | 0.11 |
| rlnc | 0.63 | 0.61 | 1.93 | 1.95 | 1.93 |
| rlnc, systematic | 0.29 | 0.30 | 0.72 | 0.72 | 0.72 |

#### Update one data chunk, GiB/s of bytes changed

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.26 | 4.23 | 18.0 | 17.7 | 18.0 |
| isa-l | 3.21 | 3.21 | 16.9 | 16.9 | 17.2 |
| reed-solomon-erasure | 3.34 | 3.34 | 12.8 | 13.2 | 13.2 |

## By target, 4+2 encode at 4 KiB, hot (one row, in cache)

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 10.3 | 8.95 | 80.6 | 88.5 | 87.9 |
| isa-l | 5.61 | 4.79 | 31.1 | 31.1 | 31.2 |
| reed-solomon-erasure | 7.45 | 7.38 | 29.7 | 37.6 | 36.8 |
| reed-solomon-simd | 7.13 | 7.21 | 25.6 | 26.7 | 22.1 |
| raptorq | 0.16 | 0.16 | 0.58 | 0.72 | 0.76 |
| rlnc | 2.03 | 2.04 | 7.71 | 9.42 | 9.58 |
| rlnc, systematic | 2.62 | 2.63 | 11.3 | 11.9 | 11.7 |

## By target, 4+2 encode at 1 MiB, hot (one row, in cache)

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 8.30 | 8.42 | 75.4 | 76.5 | 77.4 |
| isa-l | 5.11 | 5.12 | 32.7 | 33.0 | 32.9 |
| reed-solomon-erasure | 5.87 | 5.82 | 30.7 | 31.3 | 31.2 |
| reed-solomon-simd | 2.87 | 2.93 | 19.1 | 21.8 | 19.4 |
| raptorq | 0.16 | 0.16 | 0.53 | 0.55 | 0.61 |
| rlnc | 1.21 | 1.19 | 7.27 | 7.24 | 7.33 |
| rlnc, systematic | 1.39 | 1.40 | 9.88 | 9.91 | 9.99 |

## rlnc's healthy read: decode with nothing lost at 4+2 and 64 KiB, cold

| Candidate | titan znver1 | hyperion znver1 | europa znver1 | europa x86-64-v4 | europa native |
| --- | --- | --- | --- | --- | --- |
| rlnc | 1.77 | 1.78 | 5.29 | 5.29 | 5.28 |

## titan znver1

#### Encode at 4+2 by unit, cold (rows in turn, out of cache), GiB/s of stripe data

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.93 | 7.35 | 7.62 | 7.77 | 7.79 |
| isa-l | 4.65 | 4.85 | 4.98 | 5.03 | 5.03 |
| reed-solomon-erasure | 5.57 | 5.73 | 5.63 | 5.81 | 4.61 |
| reed-solomon-simd | 4.47 | 4.62 | 4.75 | 4.74 | 2.92 |
| raptorq | 0.16 | 0.16 | 0.16 | 0.16 | 0.16 |
| rlnc | 1.45 | 1.45 | 1.49 | 1.57 | 1.22 |
| rlnc, systematic | 2.07 | 2.30 | 2.44 | 2.08 | 1.36 |

#### Microseconds a call: encode at 4+2 by unit, cold (rows in turn, out of cache)

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.57 | 8.30 | 32.0 | 126 | 501 |
| isa-l | 3.28 | 12.6 | 49.0 | 194 | 777 |
| reed-solomon-erasure | 2.74 | 10.7 | 43.4 | 168 | 846 |
| reed-solomon-simd | 3.41 | 13.2 | 51.4 | 206 | 1340 |
| raptorq | 98.1 | 385 | 1538 | 6144 | 24554 |
| rlnc | 10.5 | 42.2 | 164 | 622 | 3194 |
| rlnc, systematic | 7.39 | 26.6 | 100 | 469 | 2878 |

#### Update one data chunk at 4+2 by unit, cold (rows in turn, out of cache), GiB/s of bytes changed

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.54 | 2.99 | 3.25 | 3.15 | 2.98 |
| isa-l | 2.59 | 3.08 | 3.19 | 3.07 | 2.93 |
| reed-solomon-erasure | 2.53 | 2.75 | 2.85 | 2.77 | 2.65 |

#### Encode at 4+2 by unit, hot (one row, in cache), GiB/s of stripe data

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 10.3 | 9.46 | 9.87 | 9.88 | 8.30 |
| isa-l | 5.61 | 5.27 | 5.37 | 5.33 | 5.11 |
| reed-solomon-erasure | 7.45 | 7.83 | 8.11 | 8.12 | 5.87 |
| reed-solomon-simd | 7.13 | 7.46 | 7.51 | 7.21 | 2.87 |
| raptorq | 0.16 | 0.16 | 0.16 | 0.16 | 0.16 |
| rlnc | 2.03 | 2.07 | 2.10 | 1.91 | 1.21 |
| rlnc, systematic | 2.62 | 2.89 | 2.96 | 2.15 | 1.39 |

#### Microseconds a call: encode at 4+2 by unit, hot (one row, in cache)

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.49 | 6.45 | 24.7 | 98.8 | 471 |
| isa-l | 2.72 | 11.6 | 45.5 | 183 | 764 |
| reed-solomon-erasure | 2.05 | 7.79 | 30.1 | 120 | 666 |
| reed-solomon-simd | 2.14 | 8.18 | 32.5 | 135 | 1361 |
| raptorq | 95.5 | 381 | 1527 | 6086 | 24550 |
| rlnc | 7.52 | 29.5 | 116 | 512 | 3230 |
| rlnc, systematic | 5.83 | 21.1 | 82.5 | 455 | 2808 |

#### Update one data chunk at 4+2 by unit, hot (one row, in cache), GiB/s of bytes changed

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.35 | 6.25 | 6.67 | 6.60 | 3.65 |
| isa-l | 5.67 | 6.48 | 6.86 | 6.74 | 3.67 |
| reed-solomon-erasure | 5.01 | 5.58 | 5.86 | 5.82 | 3.22 |

#### Encode at 64 KiB by layout, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 10.4 | n/a | n/a | n/a | n/a |
| rusty_erasure | 10.0 | 7.62 | 6.43 | 6.63 | 5.29 |
| isa-l | 8.69 | 4.98 | 3.71 | 3.73 | 3.34 |
| reed-solomon-erasure | 8.33 | 5.63 | 4.18 | 4.34 | 3.35 |
| reed-solomon-simd | 6.70 | 4.75 | 2.88 | 3.69 | 3.24 |
| raptorq | 0.08 | 0.16 | 0.23 | 0.30 | 0.37 |
| rlnc | 1.96 | 1.49 | 1.04 | 1.05 | 0.76 |
| rlnc, systematic | 2.76 | 2.44 | 2.11 | 2.22 | 1.87 |

#### Decode with m data chunks lost at 64 KiB by layout, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 10.4 | - | - | - | - |
| rusty_erasure | 9.94 | 7.62 | 6.47 | 6.72 | 5.33 |
| isa-l | 8.68 | 4.94 | 3.70 | 3.73 | 3.33 |
| reed-solomon-erasure | 8.23 | 5.60 | 4.17 | 4.34 | 3.35 |
| reed-solomon-simd | 0.33 | 0.50 | 0.51 | 0.67 | 0.76 |
| raptorq | 0.08 | 0.15 | 0.22 | 0.29 | 0.35 |
| rlnc | 2.54 | 1.77 | 1.42 | 1.17 | 1.00 |
| rlnc, systematic | 1.15 | 1.35 | 1.37 | 1.48 | 1.35 |

#### Rebuild one chunk at 64 KiB by layout, cold, GiB/s of chunk rebuilt

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 5.23 | - | - | - | - |
| rusty_erasure | 4.97 | 3.03 | 1.96 | 1.47 | 1.18 |
| isa-l | 4.29 | 2.24 | 1.31 | 1.05 | 0.85 |
| reed-solomon-erasure | 4.08 | 2.51 | 1.82 | 1.43 | 1.16 |
| reed-solomon-simd | 0.16 | 0.13 | 0.09 | 0.08 | 0.08 |
| raptorq | 0.04 | 0.04 | 0.04 | 0.04 | 0.03 |
| rlnc | 2.01 | 1.17 | 0.77 | 0.63 | 0.47 |
| rlnc, systematic | 0.58 | 0.44 | 0.36 | 0.30 | 0.26 |

#### One parity chunk: encode at 64 KiB, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+1 | 6+1 | 8+1 | 10+1 |
| --- | --- | --- | --- | --- | --- |
| xor | 10.4 | 14.5 | 16.3 | 16.9 | 16.7 |
| rusty_erasure | 10.0 | 12.5 | 11.8 | 11.8 | 11.9 |
| isa-l | 8.69 | - | - | - | - |
| reed-solomon-erasure | 8.33 | - | - | - | - |
| reed-solomon-simd | 6.70 | - | - | - | - |
| raptorq | 0.08 | - | - | - | - |
| rlnc | 1.96 | - | - | - | - |
| rlnc, systematic | 2.76 | - | - | - | - |

Spread of the 3 measurements a cell, (max - min) / median: 0.5% typical; the widest 73.4%, rlnc rebuild 2+1 at 1 MiB.

## hyperion znver1

#### Encode at 4+2 by unit, cold (rows in turn, out of cache), GiB/s of stripe data

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.87 | 7.35 | 7.64 | 7.80 | 7.81 |
| isa-l | 4.62 | 4.88 | 4.97 | 5.02 | 5.02 |
| reed-solomon-erasure | 5.49 | 5.61 | 5.62 | 5.81 | 4.61 |
| reed-solomon-simd | 4.51 | 4.61 | 4.77 | 4.75 | 2.89 |
| raptorq | 0.16 | 0.16 | 0.16 | 0.16 | 0.16 |
| rlnc | 1.46 | 1.45 | 1.49 | 1.57 | 1.23 |
| rlnc, systematic | 2.05 | 2.31 | 2.44 | 2.08 | 1.40 |

#### Microseconds a call: encode at 4+2 by unit, cold (rows in turn, out of cache)

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.60 | 8.30 | 32.0 | 125 | 500 |
| isa-l | 3.30 | 12.5 | 49.1 | 195 | 778 |
| reed-solomon-erasure | 2.78 | 10.9 | 43.4 | 168 | 847 |
| reed-solomon-simd | 3.38 | 13.2 | 51.2 | 205 | 1353 |
| raptorq | 98.1 | 385 | 1537 | 6127 | 24569 |
| rlnc | 10.5 | 42.1 | 163 | 623 | 3185 |
| rlnc, systematic | 7.43 | 26.4 | 100 | 469 | 2781 |

#### Update one data chunk at 4+2 by unit, cold (rows in turn, out of cache), GiB/s of bytes changed

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.52 | 2.98 | 3.25 | 3.12 | 3.02 |
| isa-l | 2.58 | 3.01 | 3.13 | 3.09 | 2.97 |
| reed-solomon-erasure | 2.59 | 2.74 | 2.83 | 2.77 | 2.68 |

#### Encode at 4+2 by unit, hot (one row, in cache), GiB/s of stripe data

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 8.95 | 9.20 | 9.78 | 9.87 | 8.42 |
| isa-l | 4.79 | 4.83 | 5.28 | 5.32 | 5.12 |
| reed-solomon-erasure | 7.38 | 7.78 | 8.07 | 8.12 | 5.82 |
| reed-solomon-simd | 7.21 | 7.44 | 7.42 | 7.31 | 2.93 |
| raptorq | 0.16 | 0.16 | 0.16 | 0.16 | 0.16 |
| rlnc | 2.04 | 2.07 | 2.09 | 1.89 | 1.19 |
| rlnc, systematic | 2.63 | 2.90 | 2.95 | 2.12 | 1.40 |

#### Microseconds a call: encode at 4+2 by unit, hot (one row, in cache)

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.70 | 6.63 | 25.0 | 98.9 | 464 |
| isa-l | 3.19 | 12.6 | 46.2 | 184 | 763 |
| reed-solomon-erasure | 2.07 | 7.84 | 30.3 | 120 | 672 |
| reed-solomon-simd | 2.12 | 8.20 | 32.9 | 134 | 1333 |
| raptorq | 95.5 | 382 | 1525 | 6062 | 24581 |
| rlnc | 7.49 | 29.5 | 117 | 516 | 3289 |
| rlnc, systematic | 5.80 | 21.0 | 82.8 | 460 | 2784 |

#### Update one data chunk at 4+2 by unit, hot (one row, in cache), GiB/s of bytes changed

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.49 | 6.22 | 6.66 | 6.62 | 3.62 |
| isa-l | 5.82 | 6.46 | 6.85 | 6.78 | 3.68 |
| reed-solomon-erasure | 5.01 | 5.55 | 5.88 | 5.82 | 3.13 |

#### Encode at 64 KiB by layout, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 10.4 | n/a | n/a | n/a | n/a |
| rusty_erasure | 10.1 | 7.64 | 6.49 | 6.67 | 5.26 |
| isa-l | 8.72 | 4.97 | 3.71 | 3.74 | 3.38 |
| reed-solomon-erasure | 8.32 | 5.62 | 4.19 | 4.35 | 3.35 |
| reed-solomon-simd | 6.73 | 4.77 | 2.88 | 3.68 | 3.23 |
| raptorq | 0.08 | 0.16 | 0.23 | 0.30 | 0.37 |
| rlnc | 1.96 | 1.49 | 1.05 | 1.05 | 0.76 |
| rlnc, systematic | 2.76 | 2.44 | 2.10 | 2.21 | 1.87 |

#### Decode with m data chunks lost at 64 KiB by layout, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 10.4 | - | - | - | - |
| rusty_erasure | 9.92 | 7.64 | 6.50 | 6.78 | 5.22 |
| isa-l | 8.70 | 4.95 | 3.69 | 3.72 | 3.32 |
| reed-solomon-erasure | 8.21 | 5.60 | 4.17 | 4.34 | 3.35 |
| reed-solomon-simd | 0.33 | 0.50 | 0.51 | 0.67 | 0.76 |
| raptorq | 0.08 | 0.15 | 0.22 | 0.29 | 0.36 |
| rlnc | 2.54 | 1.78 | 1.43 | 1.18 | 0.99 |
| rlnc, systematic | 1.17 | 1.35 | 1.40 | 1.50 | 1.35 |

#### Rebuild one chunk at 64 KiB by layout, cold, GiB/s of chunk rebuilt

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 5.23 | - | - | - | - |
| rusty_erasure | 4.96 | 3.06 | 1.96 | 1.48 | 1.18 |
| isa-l | 4.34 | 2.26 | 1.32 | 1.06 | 0.85 |
| reed-solomon-erasure | 4.08 | 2.51 | 1.82 | 1.42 | 1.16 |
| reed-solomon-simd | 0.17 | 0.13 | 0.09 | 0.08 | 0.08 |
| raptorq | 0.04 | 0.04 | 0.04 | 0.04 | 0.03 |
| rlnc | 2.01 | 1.17 | 0.77 | 0.63 | 0.48 |
| rlnc, systematic | 0.58 | 0.45 | 0.37 | 0.31 | 0.26 |

#### One parity chunk: encode at 64 KiB, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+1 | 6+1 | 8+1 | 10+1 |
| --- | --- | --- | --- | --- | --- |
| xor | 10.4 | 14.4 | 16.3 | 16.9 | 16.8 |
| rusty_erasure | 10.1 | 12.4 | 11.8 | 11.8 | 11.9 |
| isa-l | 8.72 | - | - | - | - |
| reed-solomon-erasure | 8.32 | - | - | - | - |
| reed-solomon-simd | 6.73 | - | - | - | - |
| raptorq | 0.08 | - | - | - | - |
| rlnc | 1.96 | - | - | - | - |
| rlnc, systematic | 2.76 | - | - | - | - |

Spread of the 3 measurements a cell, (max - min) / median: 0.4% typical; the widest 94.7%, rlnc, systematic decode-2 4+2 at 64 KiB hot.

## europa znver1

#### Encode at 4+2 by unit, cold (rows in turn, out of cache), GiB/s of stripe data

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.3 | 17.2 | 20.2 | 21.0 | 21.2 |
| isa-l | 14.5 | 16.9 | 19.6 | 20.3 | 20.6 |
| reed-solomon-erasure | 13.8 | 10.9 | 11.8 | 11.4 | 13.9 |
| reed-solomon-simd | 7.74 | 8.59 | 9.81 | 11.6 | 11.7 |
| raptorq | 0.55 | 0.68 | 0.51 | 0.59 | 0.51 |
| rlnc | 3.72 | 4.91 | 4.76 | 4.87 | 4.88 |
| rlnc, systematic | 5.68 | 7.23 | 7.37 | 7.62 | 7.46 |

#### Microseconds a call: encode at 4+2 by unit, cold (rows in turn, out of cache)

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.00 | 3.55 | 12.1 | 46.5 | 184 |
| isa-l | 1.05 | 3.60 | 12.5 | 48.0 | 189 |
| reed-solomon-erasure | 1.11 | 5.60 | 20.7 | 85.5 | 280 |
| reed-solomon-simd | 1.97 | 7.10 | 24.9 | 83.9 | 335 |
| raptorq | 27.6 | 89.5 | 475 | 1662 | 7587 |
| rlnc | 4.10 | 12.4 | 51.3 | 200 | 801 |
| rlnc, systematic | 2.69 | 8.45 | 33.1 | 128 | 523 |

#### Update one data chunk at 4+2 by unit, cold (rows in turn, out of cache), GiB/s of bytes changed

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.39 | 5.49 | 6.03 | 6.97 | 7.76 |
| isa-l | 4.44 | 5.36 | 5.94 | 6.96 | 7.50 |
| reed-solomon-erasure | 6.17 | 4.78 | 4.83 | 5.15 | 6.82 |

#### Encode at 4+2 by unit, hot (one row, in cache), GiB/s of stripe data

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 80.6 | 87.2 | 97.0 | 75.2 | 75.4 |
| isa-l | 31.1 | 34.0 | 35.0 | 34.0 | 32.7 |
| reed-solomon-erasure | 29.7 | 30.5 | 33.4 | 29.3 | 30.7 |
| reed-solomon-simd | 25.6 | 20.9 | 21.3 | 20.9 | 19.1 |
| raptorq | 0.58 | 0.64 | 0.53 | 0.60 | 0.53 |
| rlnc | 7.71 | 9.37 | 8.06 | 7.13 | 7.27 |
| rlnc, systematic | 11.3 | 12.6 | 10.8 | 9.65 | 9.88 |

#### Microseconds a call: encode at 4+2 by unit, hot (one row, in cache)

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 0.19 | 0.70 | 2.52 | 13.0 | 51.8 |
| isa-l | 0.49 | 1.79 | 6.98 | 28.7 | 119 |
| reed-solomon-erasure | 0.51 | 2.00 | 7.31 | 33.3 | 127 |
| reed-solomon-simd | 0.60 | 2.91 | 11.4 | 46.7 | 204 |
| raptorq | 26.4 | 94.9 | 461 | 1628 | 7371 |
| rlnc | 1.98 | 6.51 | 30.3 | 137 | 537 |
| rlnc, systematic | 1.35 | 4.86 | 22.6 | 101 | 395 |

#### Update one data chunk at 4+2 by unit, hot (one row, in cache), GiB/s of bytes changed

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 25.1 | 20.8 | 24.1 | 20.4 | 21.3 |
| isa-l | 22.3 | 21.8 | 23.2 | 20.3 | 20.7 |
| reed-solomon-erasure | 21.3 | 9.07 | 19.6 | 17.2 | 18.3 |

#### Encode at 64 KiB by layout, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 17.8 | n/a | n/a | n/a | n/a |
| rusty_erasure | 20.1 | 20.2 | 17.3 | 19.6 | 16.8 |
| isa-l | 19.6 | 19.6 | 15.9 | 18.4 | 13.5 |
| reed-solomon-erasure | 15.5 | 11.8 | 9.78 | 10.4 | 8.77 |
| reed-solomon-simd | 11.0 | 9.81 | 7.74 | 9.66 | 9.15 |
| raptorq | 0.31 | 0.51 | 0.85 | 1.10 | 1.16 |
| rlnc | 6.14 | 4.76 | 3.94 | 3.55 | 2.96 |
| rlnc, systematic | 7.59 | 7.37 | 6.77 | 7.13 | 6.40 |

#### Decode with m data chunks lost at 64 KiB by layout, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 17.3 | - | - | - | - |
| rusty_erasure | 19.5 | 20.1 | 17.3 | 19.4 | 16.8 |
| isa-l | 19.0 | 19.1 | 15.8 | 17.9 | 13.2 |
| reed-solomon-erasure | 14.3 | 11.3 | 9.49 | 10.1 | 8.58 |
| reed-solomon-simd | 0.90 | 1.41 | 1.43 | 1.87 | 2.14 |
| raptorq | 0.29 | 0.49 | 0.79 | 1.01 | 1.09 |
| rlnc | 6.11 | 5.31 | 4.66 | 4.15 | 3.63 |
| rlnc, systematic | 2.28 | 3.18 | 3.61 | 4.01 | 3.98 |

#### Rebuild one chunk at 64 KiB by layout, cold, GiB/s of chunk rebuilt

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 8.61 | - | - | - | - |
| rusty_erasure | 9.75 | 6.61 | 5.08 | 3.91 | 3.20 |
| isa-l | 9.50 | 6.46 | 4.72 | 3.51 | 2.97 |
| reed-solomon-erasure | 7.11 | 3.93 | 2.77 | 2.16 | 1.76 |
| reed-solomon-simd | 0.45 | 0.35 | 0.24 | 0.24 | 0.22 |
| raptorq | 0.15 | 0.12 | 0.13 | 0.12 | 0.11 |
| rlnc | 4.83 | 2.87 | 2.09 | 1.67 | 1.40 |
| rlnc, systematic | 1.14 | 0.93 | 0.78 | 0.67 | 0.58 |

#### One parity chunk: encode at 64 KiB, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+1 | 6+1 | 8+1 | 10+1 |
| --- | --- | --- | --- | --- | --- |
| xor | 17.8 | 25.6 | 27.7 | 28.6 | 28.1 |
| rusty_erasure | 20.1 | 27.3 | 31.4 | 31.6 | 32.3 |
| isa-l | 19.6 | - | - | - | - |
| reed-solomon-erasure | 15.5 | - | - | - | - |
| reed-solomon-simd | 11.0 | - | - | - | - |
| raptorq | 0.31 | - | - | - | - |
| rlnc | 6.14 | - | - | - | - |
| rlnc, systematic | 7.59 | - | - | - | - |

Spread of the 3 measurements a cell, (max - min) / median: 0.9% typical; the widest 106.4%, reed-solomon-erasure update 4+2 at 16 KiB hot.

## europa x86-64-v4

#### Encode at 4+2 by unit, cold (rows in turn, out of cache), GiB/s of stripe data

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.1 | 17.6 | 20.3 | 21.1 | 21.2 |
| isa-l | 14.5 | 17.0 | 19.6 | 20.3 | 20.6 |
| reed-solomon-erasure | 15.1 | 12.0 | 12.6 | 12.6 | 14.4 |
| reed-solomon-simd | 7.88 | 9.12 | 9.83 | 11.8 | 11.6 |
| raptorq | 0.68 | 0.51 | 0.51 | 0.58 | 0.60 |
| rlnc | 3.84 | 4.94 | 4.77 | 4.87 | 4.87 |
| rlnc, systematic | 5.75 | 7.29 | 7.40 | 7.72 | 7.47 |

#### Microseconds a call: encode at 4+2 by unit, cold (rows in turn, out of cache)

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.01 | 3.47 | 12.0 | 46.4 | 184 |
| isa-l | 1.05 | 3.59 | 12.5 | 48.0 | 189 |
| reed-solomon-erasure | 1.01 | 5.08 | 19.3 | 77.8 | 272 |
| reed-solomon-simd | 1.94 | 6.69 | 24.8 | 82.5 | 336 |
| raptorq | 22.3 | 120 | 482 | 1676 | 6481 |
| rlnc | 3.98 | 12.4 | 51.2 | 200 | 802 |
| rlnc, systematic | 2.65 | 8.37 | 33.0 | 127 | 523 |

#### Update one data chunk at 4+2 by unit, cold (rows in turn, out of cache), GiB/s of bytes changed

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.43 | 5.47 | 6.04 | 7.00 | 7.69 |
| isa-l | 4.43 | 5.37 | 5.94 | 6.91 | 7.51 |
| reed-solomon-erasure | 6.21 | 5.35 | 5.27 | 5.76 | 7.19 |

#### Encode at 4+2 by unit, hot (one row, in cache), GiB/s of stripe data

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 88.5 | 88.0 | 97.6 | 74.9 | 76.5 |
| isa-l | 31.1 | 34.0 | 35.0 | 34.4 | 33.0 |
| reed-solomon-erasure | 37.6 | 36.8 | 36.2 | 32.2 | 31.3 |
| reed-solomon-simd | 26.7 | 24.3 | 24.6 | 20.1 | 21.8 |
| raptorq | 0.72 | 0.53 | 0.61 | 0.52 | 0.55 |
| rlnc | 9.42 | 9.70 | 8.40 | 7.08 | 7.24 |
| rlnc, systematic | 11.9 | 12.3 | 10.9 | 10.2 | 9.91 |

#### Microseconds a call: encode at 4+2 by unit, hot (one row, in cache)

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 0.17 | 0.69 | 2.50 | 13.0 | 51.0 |
| isa-l | 0.49 | 1.79 | 6.98 | 28.4 | 118 |
| reed-solomon-erasure | 0.41 | 1.66 | 6.74 | 30.3 | 125 |
| reed-solomon-simd | 0.57 | 2.51 | 9.93 | 48.6 | 179 |
| raptorq | 21.2 | 116 | 397 | 1883 | 7082 |
| rlnc | 1.62 | 6.30 | 29.1 | 138 | 539 |
| rlnc, systematic | 1.29 | 4.94 | 22.4 | 96.0 | 394 |

#### Update one data chunk at 4+2 by unit, hot (one row, in cache), GiB/s of bytes changed

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 26.5 | 22.2 | 24.3 | 20.7 | 20.8 |
| isa-l | 22.3 | 22.1 | 23.6 | 20.4 | 21.3 |
| reed-solomon-erasure | 22.1 | 20.7 | 20.8 | 17.8 | 17.6 |

#### Encode at 64 KiB by layout, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 17.9 | n/a | n/a | n/a | n/a |
| rusty_erasure | 20.2 | 20.3 | 17.2 | 19.4 | 17.0 |
| isa-l | 19.4 | 19.6 | 15.9 | 18.2 | 14.0 |
| reed-solomon-erasure | 16.7 | 12.6 | 10.7 | 11.5 | 9.66 |
| reed-solomon-simd | 10.9 | 9.83 | 8.24 | 9.60 | 9.48 |
| raptorq | 0.28 | 0.51 | 0.84 | 0.95 | 1.21 |
| rlnc | 6.15 | 4.77 | 3.94 | 3.55 | 2.99 |
| rlnc, systematic | 7.69 | 7.40 | 6.80 | 6.98 | 6.55 |

#### Decode with m data chunks lost at 64 KiB by layout, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 17.3 | - | - | - | - |
| rusty_erasure | 19.5 | 20.2 | 17.1 | 19.2 | 16.8 |
| isa-l | 19.0 | 19.2 | 15.8 | 17.9 | 13.3 |
| reed-solomon-erasure | 15.6 | 12.4 | 10.5 | 11.2 | 9.48 |
| reed-solomon-simd | 0.98 | 1.53 | 1.47 | 2.08 | 2.21 |
| raptorq | 0.27 | 0.48 | 0.78 | 0.89 | 1.15 |
| rlnc | 6.12 | 5.31 | 4.68 | 4.17 | 3.65 |
| rlnc, systematic | 2.29 | 3.17 | 3.64 | 4.04 | 3.95 |

#### Rebuild one chunk at 64 KiB by layout, cold, GiB/s of chunk rebuilt

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 8.60 | - | - | - | - |
| rusty_erasure | 9.65 | 6.63 | 5.07 | 3.95 | 3.25 |
| isa-l | 9.46 | 6.45 | 4.73 | 3.49 | 2.97 |
| reed-solomon-erasure | 7.80 | 4.47 | 3.18 | 2.46 | 2.01 |
| reed-solomon-simd | 0.49 | 0.38 | 0.25 | 0.26 | 0.23 |
| raptorq | 0.13 | 0.12 | 0.13 | 0.12 | 0.11 |
| rlnc | 4.79 | 2.86 | 2.12 | 1.70 | 1.41 |
| rlnc, systematic | 1.15 | 0.93 | 0.78 | 0.67 | 0.58 |

#### One parity chunk: encode at 64 KiB, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+1 | 6+1 | 8+1 | 10+1 |
| --- | --- | --- | --- | --- | --- |
| xor | 17.9 | 25.8 | 28.7 | 29.7 | 28.5 |
| rusty_erasure | 20.2 | 27.5 | 31.5 | 32.4 | 33.0 |
| isa-l | 19.4 | - | - | - | - |
| reed-solomon-erasure | 16.7 | - | - | - | - |
| reed-solomon-simd | 10.9 | - | - | - | - |
| raptorq | 0.28 | - | - | - | - |
| rlnc | 6.15 | - | - | - | - |
| rlnc, systematic | 7.69 | - | - | - | - |

Spread of the 3 measurements a cell, (max - min) / median: 0.9% typical; the widest 87.9%, rlnc, systematic encode 2+1 at 1 MiB.

## europa native

#### Encode at 4+2 by unit, cold (rows in turn, out of cache), GiB/s of stripe data

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.3 | 17.2 | 20.2 | 21.0 | 21.2 |
| isa-l | 14.6 | 16.8 | 19.6 | 20.3 | 20.5 |
| reed-solomon-erasure | 15.6 | 12.0 | 12.7 | 12.6 | 14.3 |
| reed-solomon-simd | 7.99 | 8.81 | 9.45 | 11.8 | 11.8 |
| raptorq | 0.56 | 0.54 | 0.54 | 0.61 | 0.59 |
| rlnc | 3.88 | 5.05 | 4.76 | 4.94 | 4.89 |
| rlnc, systematic | 5.95 | 7.10 | 7.43 | 7.65 | 7.46 |

#### Microseconds a call: encode at 4+2 by unit, cold (rows in turn, out of cache)

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.00 | 3.55 | 12.1 | 46.5 | 184 |
| isa-l | 1.05 | 3.62 | 12.5 | 48.2 | 191 |
| reed-solomon-erasure | 0.98 | 5.09 | 19.3 | 77.7 | 273 |
| reed-solomon-simd | 1.91 | 6.93 | 25.8 | 82.5 | 330 |
| raptorq | 27.5 | 114 | 452 | 1603 | 6601 |
| rlnc | 3.93 | 12.1 | 51.3 | 198 | 799 |
| rlnc, systematic | 2.56 | 8.59 | 32.8 | 128 | 524 |

#### Update one data chunk at 4+2 by unit, cold (rows in turn, out of cache), GiB/s of bytes changed

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.36 | 5.40 | 5.96 | 6.76 | 7.53 |
| isa-l | 4.29 | 5.25 | 5.89 | 6.79 | 7.24 |
| reed-solomon-erasure | 5.69 | 5.19 | 5.22 | 5.62 | 6.97 |

#### Encode at 4+2 by unit, hot (one row, in cache), GiB/s of stripe data

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 87.9 | 87.2 | 97.3 | 74.9 | 77.4 |
| isa-l | 31.2 | 34.0 | 35.1 | 34.1 | 32.9 |
| reed-solomon-erasure | 36.8 | 36.7 | 36.4 | 32.1 | 31.2 |
| reed-solomon-simd | 22.1 | 25.0 | 21.4 | 22.0 | 19.4 |
| raptorq | 0.76 | 0.56 | 0.56 | 0.62 | 0.61 |
| rlnc | 9.58 | 9.48 | 8.06 | 7.17 | 7.33 |
| rlnc, systematic | 11.7 | 12.8 | 11.0 | 9.71 | 9.99 |

#### Microseconds a call: encode at 4+2 by unit, hot (one row, in cache)

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 0.17 | 0.70 | 2.51 | 13.0 | 50.5 |
| isa-l | 0.49 | 1.80 | 6.96 | 28.6 | 119 |
| reed-solomon-erasure | 0.41 | 1.66 | 6.71 | 30.5 | 125 |
| reed-solomon-simd | 0.69 | 2.44 | 11.4 | 44.5 | 201 |
| raptorq | 20.0 | 110 | 439 | 1563 | 6401 |
| rlnc | 1.59 | 6.44 | 30.3 | 136 | 533 |
| rlnc, systematic | 1.30 | 4.78 | 22.1 | 101 | 391 |

#### Update one data chunk at 4+2 by unit, hot (one row, in cache), GiB/s of bytes changed

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 25.4 | 22.2 | 24.3 | 20.3 | 21.0 |
| isa-l | 22.2 | 22.2 | 23.7 | 20.5 | 20.7 |
| reed-solomon-erasure | 22.1 | 19.9 | 20.7 | 17.8 | 17.6 |

#### Encode at 64 KiB by layout, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 16.8 | n/a | n/a | n/a | n/a |
| rusty_erasure | 20.1 | 20.2 | 17.1 | 19.3 | 16.8 |
| isa-l | 19.5 | 19.6 | 15.9 | 18.3 | 13.8 |
| reed-solomon-erasure | 16.7 | 12.7 | 10.7 | 11.5 | 9.68 |
| reed-solomon-simd | 10.8 | 9.45 | 7.28 | 9.17 | 8.52 |
| raptorq | 0.27 | 0.54 | 0.77 | 1.01 | 1.35 |
| rlnc | 6.22 | 4.76 | 3.89 | 3.57 | 3.02 |
| rlnc, systematic | 7.67 | 7.43 | 6.66 | 6.91 | 6.27 |

#### Decode with m data chunks lost at 64 KiB by layout, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 17.2 | - | - | - | - |
| rusty_erasure | 19.3 | 20.1 | 16.9 | 19.1 | 16.7 |
| isa-l | 18.9 | 19.1 | 15.7 | 17.8 | 13.5 |
| reed-solomon-erasure | 15.6 | 12.4 | 10.5 | 11.3 | 9.49 |
| reed-solomon-simd | 0.86 | 1.38 | 1.49 | 1.94 | 2.27 |
| raptorq | 0.25 | 0.51 | 0.72 | 0.94 | 1.24 |
| rlnc | 6.17 | 5.29 | 4.66 | 4.18 | 3.73 |
| rlnc, systematic | 2.29 | 3.20 | 3.66 | 4.07 | 4.03 |

#### Rebuild one chunk at 64 KiB by layout, cold, GiB/s of chunk rebuilt

| Candidate | 2+1 | 4+2 | 6+3 | 8+3 | 10+4 |
| --- | --- | --- | --- | --- | --- |
| xor | 8.77 | - | - | - | - |
| rusty_erasure | 9.72 | 6.61 | 5.01 | 3.95 | 3.20 |
| isa-l | 9.48 | 6.46 | 4.71 | 3.51 | 2.96 |
| reed-solomon-erasure | 7.78 | 4.48 | 3.17 | 2.47 | 2.00 |
| reed-solomon-simd | 0.43 | 0.34 | 0.25 | 0.24 | 0.23 |
| raptorq | 0.13 | 0.13 | 0.12 | 0.11 | 0.12 |
| rlnc | 4.89 | 2.86 | 2.10 | 1.71 | 1.42 |
| rlnc, systematic | 1.15 | 0.93 | 0.78 | 0.67 | 0.59 |

#### One parity chunk: encode at 64 KiB, cold, GiB/s of stripe data

| Candidate | 2+1 | 4+1 | 6+1 | 8+1 | 10+1 |
| --- | --- | --- | --- | --- | --- |
| xor | 16.8 | 23.4 | 27.3 | 28.1 | 27.0 |
| rusty_erasure | 20.1 | 27.6 | 31.3 | 32.1 | 32.6 |
| isa-l | 19.5 | - | - | - | - |
| reed-solomon-erasure | 16.7 | - | - | - | - |
| reed-solomon-simd | 10.8 | - | - | - | - |
| raptorq | 0.27 | - | - | - | - |
| rlnc | 6.22 | - | - | - | - |
| rlnc, systematic | 7.67 | - | - | - | - |

Spread of the 3 measurements a cell, (max - min) / median: 1.0% typical; the widest 88.2%, rlnc, systematic encode 2+1 at 1 MiB.

