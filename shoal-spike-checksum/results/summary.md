## Runs

| Run | CPU | Build | Compiled for | Detected | Governor | Core | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| titan znver1 | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | `znver1` | sse4.2, pclmulqdq, aes, avx2 | sse4.2, pclmulqdq, aes, avx2 | performance | 2 | 3 × 200 ms | 2026-10-03T21:28:20Z |
| titan x86-64-v3 +aes | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | `x86-64-v3 +aes` | sse4.2, aes, avx2 | sse4.2, pclmulqdq, aes, avx2 | performance | 2 | 3 × 200 ms | 2026-10-03T21:30:38Z |
| hyperion znver1 | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | `znver1` | sse4.2, pclmulqdq, aes, avx2 | sse4.2, pclmulqdq, aes, avx2 | performance | 2 | 3 × 200 ms | 2026-10-03T21:28:20Z |
| hyperion x86-64-v3 +aes | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | `x86-64-v3 +aes` | sse4.2, aes, avx2 | sse4.2, pclmulqdq, aes, avx2 | performance | 2 | 3 × 200 ms | 2026-10-03T21:30:38Z |
| europa znver1 | AMD Ryzen 9 7945HX with Radeon Graphics | `znver1` | sse4.2, pclmulqdq, aes, avx2 | sse4.2, pclmulqdq, aes, avx2, avx512f, avx512vl, vpclmulqdq, vaes | performance | 8 | 3 × 200 ms | 2026-10-03T21:30:40Z |
| europa x86-64-v3 +aes | AMD Ryzen 9 7945HX with Radeon Graphics | `x86-64-v3 +aes` | sse4.2, aes, avx2 | sse4.2, pclmulqdq, aes, avx2, avx512f, avx512vl, vpclmulqdq, vaes | performance | 8 | 3 × 200 ms | 2026-10-03T21:32:55Z |
| europa x86-64-v4 +aes | AMD Ryzen 9 7945HX with Radeon Graphics | `x86-64-v4 +aes` | sse4.2, aes, avx2, avx512f, avx512vl | sse4.2, pclmulqdq, aes, avx2, avx512f, avx512vl, vpclmulqdq, vaes | performance | 8 | 3 × 200 ms | 2026-10-03T21:35:11Z |
| europa native | AMD Ryzen 9 7945HX with Radeon Graphics | `native` | sse4.2, pclmulqdq, aes, avx2, avx512f, avx512vl, vpclmulqdq, vaes | sse4.2, pclmulqdq, aes, avx2, avx512f, avx512vl, vpclmulqdq, vaes | performance | 8 | 3 × 200 ms | 2026-10-03T21:28:25Z |
| europa native +gxhash3-hybrid | AMD Ryzen 9 7945HX with Radeon Graphics | `native +gxhash3-hybrid` | sse4.2, pclmulqdq, aes, avx2, avx512f, avx512vl, vpclmulqdq, vaes | sse4.2, pclmulqdq, aes, avx2, avx512f, avx512vl, vpclmulqdq, vaes | performance | 8 | 3 × 200 ms | 2026-10-03T21:37:26Z |

rustc 1.100.0-nightly (0ed41eb41 2026-09-04)

## Facts

| Candidate | Bits | Kernel, by run | Threads started | Allocations, one call at 64 KiB | Allocations, fed in 4 KiB pieces |
| --- | --- | --- | --- | --- | --- |
| crc32c | 32 | not said | 0 | 0 / 0 B | 0 / 0 B |
| crc-fast crc32c | 32 | titan znver1: `x86-sse-pclmulqdq`<br>titan x86-64-v3 +aes: `x86-sse-pclmulqdq`<br>hyperion znver1: `x86-sse-pclmulqdq`<br>hyperion x86-64-v3 +aes: `x86-sse-pclmulqdq`<br>europa znver1: `x86_64-avx512-vpclmulqdq`<br>europa x86-64-v3 +aes: `x86_64-avx512-vpclmulqdq`<br>europa x86-64-v4 +aes: `x86_64-avx512-vpclmulqdq`<br>europa native: `x86_64-avx512-vpclmulqdq`<br>europa native +gxhash3-hybrid: `x86_64-avx512-vpclmulqdq` | 0 | 0 / 0 B | 0 / 0 B |
| crc-fast crc64nvme | 64 | titan znver1: `x86-sse-pclmulqdq`<br>titan x86-64-v3 +aes: `x86-sse-pclmulqdq`<br>hyperion znver1: `x86-sse-pclmulqdq`<br>hyperion x86-64-v3 +aes: `x86-sse-pclmulqdq`<br>europa znver1: `x86_64-avx512-vpclmulqdq`<br>europa x86-64-v3 +aes: `x86_64-avx512-vpclmulqdq`<br>europa x86-64-v4 +aes: `x86_64-avx512-vpclmulqdq`<br>europa native: `x86_64-avx512-vpclmulqdq`<br>europa native +gxhash3-hybrid: `x86_64-avx512-vpclmulqdq` | 0 | 0 / 0 B | 0 / 0 B |
| crc64fast-nvme | 64 | not said | 0 | 0 / 0 B | 0 / 0 B |
| xxh3-64 | 64 | not said | 0 | 0 / 0 B | 0 / 0 B |
| xxh3-128 | 128 | not said | 0 | 0 / 0 B | 0 / 0 B |
| blake3 | 256 | not said | 0 | 0 / 0 B | 0 / 0 B |
| gxhash 2 | 64 | not said | 0 | 0 / 0 B | 0 / 0 B |
| gxhash 3 | 64 | not said | 0 | 0 / 0 B | 0 / 0 B |
| crc32fast | 32 | not said | 0 | 0 / 0 B | 0 / 0 B |

## Checks

### Published check values

| Candidate | Input | Expected | Source | Runs that gave it |
| --- | --- | --- | --- | --- |
| crc32c | "123456789" | `e3069283` | CRC-32/ISCSI check, RevEng catalogue (crc-catalog `algorithm.rs:2270`) | all 9 |
| crc32c | "Hello world!" | `7b98e751` | crc32c's own doc test (`lib.rs:8-12`) | all 9 |
| crc-fast crc32c | "123456789" | `e3069283` | CRC-32/ISCSI check, RevEng catalogue (crc-catalog `algorithm.rs:2270`) | all 9 |
| crc-fast crc32c | "Hello world!" | `7b98e751` | crc32c's own doc test (`lib.rs:8-12`) | all 9 |
| crc-fast crc64nvme | "123456789" | `ae8b14860a799888` | CRC-64/NVME check, RevEng catalogue (crc-catalog `algorithm.rs:2500`) | all 9 |
| crc-fast crc64nvme | 4,096 zero bytes | `6482d367eb22b64e` | crc64fast-nvme's tests (`lib.rs:202`) | all 9 |
| crc64fast-nvme | "123456789" | `ae8b14860a799888` | CRC-64/NVME check, RevEng catalogue (crc-catalog `algorithm.rs:2500`) | all 9 |
| crc64fast-nvme | 4,096 zero bytes | `6482d367eb22b64e` | crc64fast-nvme's tests (`lib.rs:202`) | all 9 |
| crc32fast | "123456789" | `cbf43926` | CRC-32/ISO-HDLC check, RevEng catalogue | all 9 |
| xxh3-64 | empty | `2d06800538d394c2` | twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) | all 9 |
| xxh3-64 | 1,024 bytes of the 251 pattern | `e5d78bafa45b2aa5` | twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) | all 9 |
| xxh3-64 | 10,240 bytes of the 251 pattern | `bcd63266df6e2244` | twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) | all 9 |
| xxh3-128 | empty | `99aa06d3014798d86001c324468d497f` | twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) | all 9 |
| xxh3-128 | 1,024 bytes of the 251 pattern | `d0ac1f7b93bf57b9e5d78bafa45b2aa5` | twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) | all 9 |
| xxh3-128 | 10,240 bytes of the 251 pattern | `4f6375cca7ece1e1bcd63266df6e2244` | twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) | all 9 |
| blake3 | empty | `af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262` | BLAKE3 `test_vectors/test_vectors.json` at tag 1.8.7, not shipped in the crate | all 9 |
| blake3 | 1 byte of the 251 pattern | `2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213` | BLAKE3 `test_vectors/test_vectors.json` at tag 1.8.7, not shipped in the crate | all 9 |
| blake3 | 1,024 bytes of the 251 pattern | `42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7` | BLAKE3 `test_vectors/test_vectors.json` at tag 1.8.7, not shipped in the crate | all 9 |
| blake3 | 1,025 bytes of the 251 pattern | `d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444` | BLAKE3 `test_vectors/test_vectors.json` at tag 1.8.7, not shipped in the crate | all 9 |
| blake3 | 102,400 bytes of the 251 pattern | `bc3e3d41a1146b069abffad3c0d44860cf664390afce4d9661f7902e7943e085` | BLAKE3 `test_vectors/test_vectors.json` at tag 1.8.7, not shipped in the crate | all 9 |

### The same input across runs

Every set's output compared across all 9 runs.

| Candidate | Sets | Identical in every run | Sets that differ |
| --- | --- | --- | --- |
| crc32c | 7 | 7 | none |
| crc-fast crc32c | 7 | 7 | none |
| crc-fast crc64nvme | 7 | 7 | none |
| crc64fast-nvme | 7 | 7 | none |
| xxh3-64 | 7 | 7 | none |
| xxh3-128 | 7 | 7 | none |
| blake3 | 7 | 7 | none |
| gxhash 2 | 7 | 7 | none |
| gxhash 3 | 7 | 7 | none |
| crc32fast | 7 | 7 | none |

### Outputs, from titan znver1

| Candidate | Set | Output |
| --- | --- | --- |
| crc32c | lengths 0-257 | `6c7c9f53c931b3f6` |
| crc32c | unit 4 KiB | `25718eb9` |
| crc32c | unit 16 KiB | `a341abd9` |
| crc32c | unit 64 KiB | `71896d9c` |
| crc32c | unit 256 KiB | `afc9b523` |
| crc32c | unit 1 MiB | `d7181f59` |
| crc32c | 1 MiB + 13 | `6f9cfaf4` |
| crc-fast crc32c | lengths 0-257 | `6c7c9f53c931b3f6` |
| crc-fast crc32c | unit 4 KiB | `25718eb9` |
| crc-fast crc32c | unit 16 KiB | `a341abd9` |
| crc-fast crc32c | unit 64 KiB | `71896d9c` |
| crc-fast crc32c | unit 256 KiB | `afc9b523` |
| crc-fast crc32c | unit 1 MiB | `d7181f59` |
| crc-fast crc32c | 1 MiB + 13 | `6f9cfaf4` |
| crc-fast crc64nvme | lengths 0-257 | `386cf13d45d3100f` |
| crc-fast crc64nvme | unit 4 KiB | `6169566420113d17` |
| crc-fast crc64nvme | unit 16 KiB | `75743428c915baef` |
| crc-fast crc64nvme | unit 64 KiB | `08ec8ea0c8cf5c22` |
| crc-fast crc64nvme | unit 256 KiB | `aa598083a907ea06` |
| crc-fast crc64nvme | unit 1 MiB | `a0c9c9f90458eafc` |
| crc-fast crc64nvme | 1 MiB + 13 | `b2187940c63bae90` |
| crc64fast-nvme | lengths 0-257 | `386cf13d45d3100f` |
| crc64fast-nvme | unit 4 KiB | `6169566420113d17` |
| crc64fast-nvme | unit 16 KiB | `75743428c915baef` |
| crc64fast-nvme | unit 64 KiB | `08ec8ea0c8cf5c22` |
| crc64fast-nvme | unit 256 KiB | `aa598083a907ea06` |
| crc64fast-nvme | unit 1 MiB | `a0c9c9f90458eafc` |
| crc64fast-nvme | 1 MiB + 13 | `b2187940c63bae90` |
| xxh3-64 | lengths 0-257 | `e0594ba4be04a36a` |
| xxh3-64 | unit 4 KiB | `214c8d9086d20789` |
| xxh3-64 | unit 16 KiB | `3c6e56bf64be0c01` |
| xxh3-64 | unit 64 KiB | `ffe82846d02622ba` |
| xxh3-64 | unit 256 KiB | `a864dc3dd3aea2df` |
| xxh3-64 | unit 1 MiB | `ded6491b6ea71036` |
| xxh3-64 | 1 MiB + 13 | `06f674c58a324ad7` |
| xxh3-128 | lengths 0-257 | `df240081ff328aa2` |
| xxh3-128 | unit 4 KiB | `ee702bd41a8c50a7214c8d9086d20789` |
| xxh3-128 | unit 16 KiB | `b2e5f98d764aaeb93c6e56bf64be0c01` |
| xxh3-128 | unit 64 KiB | `7272fa1832fee3fcffe82846d02622ba` |
| xxh3-128 | unit 256 KiB | `084692cbc9d1e770a864dc3dd3aea2df` |
| xxh3-128 | unit 1 MiB | `9d131743d9fd84b4ded6491b6ea71036` |
| xxh3-128 | 1 MiB + 13 | `0fbfbe87f3acfdb706f674c58a324ad7` |
| blake3 | lengths 0-257 | `ef93cbebce390042` |
| blake3 | unit 4 KiB | `edfa56ba8761ae877829563509b7eda5229e97150c7d04e6fdb18f1bcc6e0d29` |
| blake3 | unit 16 KiB | `b3e83a1f87d3e8ba9b827a0f64264bbf7ec0e1ce5c317ed242721f2ad3cfd143` |
| blake3 | unit 64 KiB | `b4622868f8aaafad5daa94f165b257ea803c66dfe0eb08a8bac39829f8a341c6` |
| blake3 | unit 256 KiB | `1d9e9d772d9b7906083dad2521a4fe6be065debd373c8be9ac4c173db04adabe` |
| blake3 | unit 1 MiB | `425cd0f463fcb4259470e2ced08cc278238c8a988ade1f2dca38e16063572b9c` |
| blake3 | 1 MiB + 13 | `b7b2ad6f43f55cba0c679701ea07a59ed218a88d86fda7ae8474741351a63a1c` |
| gxhash 2 | lengths 0-257 | `fea9fb8850ef1c3e` |
| gxhash 2 | unit 4 KiB | `9b37fd42877ed4b9` |
| gxhash 2 | unit 16 KiB | `2596e6798b52e4bf` |
| gxhash 2 | unit 64 KiB | `75f9673c39358ca9` |
| gxhash 2 | unit 256 KiB | `731a303d0e0aa65e` |
| gxhash 2 | unit 1 MiB | `29f136e482091158` |
| gxhash 2 | 1 MiB + 13 | `380d6324ea143461` |
| gxhash 3 | lengths 0-257 | `f9030b49a18a5687` |
| gxhash 3 | unit 4 KiB | `c6a109743157fb9f` |
| gxhash 3 | unit 16 KiB | `14cf8f4b2869bc69` |
| gxhash 3 | unit 64 KiB | `94a1c4b9dbdc94e1` |
| gxhash 3 | unit 256 KiB | `69a576b236d0e16e` |
| gxhash 3 | unit 1 MiB | `b9067eae621800c8` |
| gxhash 3 | 1 MiB + 13 | `d70d47d8c61d99b5` |
| crc32fast | lengths 0-257 | `be616535821bc149` |
| crc32fast | unit 4 KiB | `46265160` |
| crc32fast | unit 16 KiB | `dbdfa35c` |
| crc32fast | unit 64 KiB | `f6a976e1` |
| crc32fast | unit 256 KiB | `10dbf817` |
| crc32fast | unit 1 MiB | `9ee1974b` |
| crc32fast | 1 MiB + 13 | `351a868b` |

### Every start offset in a cache line

| Candidate | Offsets equal to offset zero, every run |
| --- | --- |
| crc32c | 64/64 in every run |
| crc-fast crc32c | 64/64 in every run |
| crc-fast crc64nvme | 64/64 in every run |
| crc64fast-nvme | 64/64 in every run |
| xxh3-64 | 64/64 in every run |
| xxh3-128 | 64/64 in every run |
| blake3 | 64/64 in every run |
| gxhash 2 | 64/64 in every run |
| gxhash 3 | 64/64 in every run |
| crc32fast | 64/64 in every run |

### Fed in pieces

| Candidate | Interface | Cut into | Equal to one call | Equal to the interface fed whole |
| --- | --- | --- | --- | --- |
| crc32c | `crc32c_append` | pieces of 1 B | 13/13 in every run | 13/13 in every run |
| crc32c | `crc32c_append` | pieces of 7 B | 13/13 in every run | 13/13 in every run |
| crc32c | `crc32c_append` | pieces of 64 B | 13/13 in every run | 13/13 in every run |
| crc32c | `crc32c_append` | pieces of 4096 B | 13/13 in every run | 13/13 in every run |
| crc32c | `crc32c_append` | pieces of 65536 B | 13/13 in every run | 13/13 in every run |
| crc32c | `crc32c_append` | 100 random cuttings | 1300/1300 in every run | 1300/1300 in every run |
| crc-fast crc32c | `Digest::update` | pieces of 1 B | 13/13 in every run | 13/13 in every run |
| crc-fast crc32c | `Digest::update` | pieces of 7 B | 13/13 in every run | 13/13 in every run |
| crc-fast crc32c | `Digest::update` | pieces of 64 B | 13/13 in every run | 13/13 in every run |
| crc-fast crc32c | `Digest::update` | pieces of 4096 B | 13/13 in every run | 13/13 in every run |
| crc-fast crc32c | `Digest::update` | pieces of 65536 B | 13/13 in every run | 13/13 in every run |
| crc-fast crc32c | `Digest::update` | 100 random cuttings | 1300/1300 in every run | 1300/1300 in every run |
| crc-fast crc64nvme | `Digest::update` | pieces of 1 B | 13/13 in every run | 13/13 in every run |
| crc-fast crc64nvme | `Digest::update` | pieces of 7 B | 13/13 in every run | 13/13 in every run |
| crc-fast crc64nvme | `Digest::update` | pieces of 64 B | 13/13 in every run | 13/13 in every run |
| crc-fast crc64nvme | `Digest::update` | pieces of 4096 B | 13/13 in every run | 13/13 in every run |
| crc-fast crc64nvme | `Digest::update` | pieces of 65536 B | 13/13 in every run | 13/13 in every run |
| crc-fast crc64nvme | `Digest::update` | 100 random cuttings | 1300/1300 in every run | 1300/1300 in every run |
| crc64fast-nvme | `Digest::write` | pieces of 1 B | 13/13 in every run | 13/13 in every run |
| crc64fast-nvme | `Digest::write` | pieces of 7 B | 13/13 in every run | 13/13 in every run |
| crc64fast-nvme | `Digest::write` | pieces of 64 B | 13/13 in every run | 13/13 in every run |
| crc64fast-nvme | `Digest::write` | pieces of 4096 B | 13/13 in every run | 13/13 in every run |
| crc64fast-nvme | `Digest::write` | pieces of 65536 B | 13/13 in every run | 13/13 in every run |
| crc64fast-nvme | `Digest::write` | 100 random cuttings | 1300/1300 in every run | 1300/1300 in every run |
| xxh3-64 | `Xxh3::update` | pieces of 1 B | 13/13 in every run | 13/13 in every run |
| xxh3-64 | `Xxh3::update` | pieces of 7 B | 13/13 in every run | 13/13 in every run |
| xxh3-64 | `Xxh3::update` | pieces of 64 B | 13/13 in every run | 13/13 in every run |
| xxh3-64 | `Xxh3::update` | pieces of 4096 B | 13/13 in every run | 13/13 in every run |
| xxh3-64 | `Xxh3::update` | pieces of 65536 B | 13/13 in every run | 13/13 in every run |
| xxh3-64 | `Xxh3::update` | 100 random cuttings | 1300/1300 in every run | 1300/1300 in every run |
| xxh3-128 | `Xxh3::update` | pieces of 1 B | 13/13 in every run | 13/13 in every run |
| xxh3-128 | `Xxh3::update` | pieces of 7 B | 13/13 in every run | 13/13 in every run |
| xxh3-128 | `Xxh3::update` | pieces of 64 B | 13/13 in every run | 13/13 in every run |
| xxh3-128 | `Xxh3::update` | pieces of 4096 B | 13/13 in every run | 13/13 in every run |
| xxh3-128 | `Xxh3::update` | pieces of 65536 B | 13/13 in every run | 13/13 in every run |
| xxh3-128 | `Xxh3::update` | 100 random cuttings | 1300/1300 in every run | 1300/1300 in every run |
| blake3 | `Hasher::update` | pieces of 1 B | 13/13 in every run | 13/13 in every run |
| blake3 | `Hasher::update` | pieces of 7 B | 13/13 in every run | 13/13 in every run |
| blake3 | `Hasher::update` | pieces of 64 B | 13/13 in every run | 13/13 in every run |
| blake3 | `Hasher::update` | pieces of 4096 B | 13/13 in every run | 13/13 in every run |
| blake3 | `Hasher::update` | pieces of 65536 B | 13/13 in every run | 13/13 in every run |
| blake3 | `Hasher::update` | 100 random cuttings | 1300/1300 in every run | 1300/1300 in every run |
| gxhash 2 | `GxHasher::write` | pieces of 1 B | 0/13 in every run | 2/13 in every run |
| gxhash 2 | `GxHasher::write` | pieces of 7 B | 0/13 in every run | 2/13 in every run |
| gxhash 2 | `GxHasher::write` | pieces of 64 B | 0/13 in every run | 7/13 in every run |
| gxhash 2 | `GxHasher::write` | pieces of 4096 B | 0/13 in every run | 11/13 in every run |
| gxhash 2 | `GxHasher::write` | pieces of 65536 B | 0/13 in every run | 11/13 in every run |
| gxhash 2 | `GxHasher::write` | 100 random cuttings | 0/1300 in every run | 1049/1300 in every run |
| gxhash 3 | `GxHasher::write` | pieces of 1 B | 0/13 in every run | 2/13 in every run |
| gxhash 3 | `GxHasher::write` | pieces of 7 B | 0/13 in every run | 2/13 in every run |
| gxhash 3 | `GxHasher::write` | pieces of 64 B | 0/13 in every run | 7/13 in every run |
| gxhash 3 | `GxHasher::write` | pieces of 4096 B | 0/13 in every run | 11/13 in every run |
| gxhash 3 | `GxHasher::write` | pieces of 65536 B | 0/13 in every run | 11/13 in every run |
| gxhash 3 | `GxHasher::write` | 100 random cuttings | 0/1300 in every run | 1049/1300 in every run |
| crc32fast | `Hasher::update` | pieces of 1 B | 13/13 in every run | 13/13 in every run |
| crc32fast | `Hasher::update` | pieces of 7 B | 13/13 in every run | 13/13 in every run |
| crc32fast | `Hasher::update` | pieces of 64 B | 13/13 in every run | 13/13 in every run |
| crc32fast | `Hasher::update` | pieces of 4096 B | 13/13 in every run | 13/13 in every run |
| crc32fast | `Hasher::update` | pieces of 65536 B | 13/13 in every run | 13/13 in every run |
| crc32fast | `Hasher::update` | 100 random cuttings | 1300/1300 in every run | 1300/1300 in every run |

### A whole from its parts

| Candidate | How | Equal to one call over the whole |
| --- | --- | --- |
| crc32c | 16 units into a chunk, every unit size | 5/5 in every run |
| crc32c | 1,000 random cuts in two, any length | 1000/1000 in every run |
| crc-fast crc32c | 16 units into a chunk, every unit size | 5/5 in every run |
| crc-fast crc32c | 1,000 random cuts in two, any length | 1000/1000 in every run |
| crc-fast crc64nvme | 16 units into a chunk, every unit size | 5/5 in every run |
| crc-fast crc64nvme | 1,000 random cuts in two, any length | 1000/1000 in every run |
| crc32fast | 16 units into a chunk, every unit size | 5/5 in every run |
| crc32fast | 1,000 random cuts in two, any length | 1000/1000 in every run |
| blake3 | 16 units into a chunk through hazmat subtrees, every unit size | 5/5 in every run |
| crc64fast-nvme | 100 random cuts, combined by crc-fast's checksum_combine | 100/100 in every run |
| harness crc32c | 1,000 random cuts in two, any length | 1000/1000 in every run |
| harness crc32c | 16 units into a chunk, the multiplier made once a unit size | 5/5 in every run |
| harness crc64nvme | 1,000 random cuts in two, any length | 1000/1000 in every run |
| harness crc64nvme | 16 units into a chunk, the multiplier made once a unit size | 5/5 in every run |

### Carried to a place without the bytes

| Candidate | Through | Equal to one call over the bytes and a 32-byte identity |
| --- | --- | --- |
| crc32c | `crc32c_append` | 5/5 in every run |
| crc32c | `crc32c_combine` | 5/5 in every run |
| crc-fast crc32c | `checksum_combine with the suffix's checksum` | 5/5 in every run |
| crc-fast crc32c | `checksum_combine` | 5/5 in every run |
| crc-fast crc64nvme | `checksum_combine with the suffix's checksum` | 5/5 in every run |
| crc-fast crc64nvme | `checksum_combine` | 5/5 in every run |
| crc32fast | `Hasher::new_with_initial` | 5/5 in every run |
| crc32fast | `Hasher::combine` | 5/5 in every run |

## Every run at 64 KiB, one call, GiB/s cold / hot

| Candidate | titan znver1 | titan x86-64-v3 +aes | hyperion znver1 | hyperion x86-64-v3 +aes | europa znver1 | europa x86-64-v3 +aes | europa x86-64-v4 +aes | europa native | europa native +gxhash3-hybrid |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| crc32c | 6.47 / 6.82 | 6.51 / 7.09 | 6.49 / 7.13 | 6.51 / 7.12 | 19.6 / 34.4 | 19.7 / 34.4 | 19.6 / 34.2 | 19.7 / 34.4 | 19.7 / 34.3 |
| crc-fast crc32c | 11.1 / 12.4 | 9.64 / 10.9 | 10.9 / 12.4 | 9.73 / 10.9 | 39.5 / 75.1 | 39.4 / 75.3 | 39.2 / 75.1 | 39.5 / 75.5 | 39.4 / 75.3 |
| crc-fast crc64nvme | 11.4 / 13.1 | 11.3 / 13.1 | 11.4 / 13.1 | 11.3 / 13.2 | 40.5 / 75.0 | 37.4 / 75.1 | 37.5 / 75.1 | 38.2 / 75.4 | 38.4 / 75.3 |
| crc64fast-nvme | 11.5 / 13.2 | 11.5 / 13.2 | 11.6 / 13.2 | 11.5 / 13.2 | 17.8 / 19.5 | 17.8 / 19.5 | 17.8 / 19.5 | 17.9 / 19.6 | 17.9 / 19.6 |
| xxh3-64 | 14.0 / 20.9 | 14.1 / 20.9 | 14.0 / 20.9 | 14.1 / 21.0 | 42.9 / 77.8 | 43.2 / 77.3 | 37.9 / 90.5 | 38.7 / 90.2 | 38.9 / 90.2 |
| xxh3-128 | 14.0 / 20.7 | 14.0 / 20.8 | 14.0 / 20.7 | 14.1 / 20.8 | 40.5 / 77.4 | 42.7 / 76.6 | 40.4 / 84.2 | 40.5 / 87.9 | 40.5 / 88.0 |
| blake3 | 1.76 / 1.98 | 1.78 / 1.99 | 1.76 / 1.99 | 1.78 / 1.99 | 5.81 / 8.62 | 5.77 / 8.58 | 5.79 / 8.61 | 5.84 / 8.62 | 5.82 / 8.62 |
| gxhash 2 | 19.0 / 51.0 | 19.0 / 51.0 | 19.0 / 51.6 | 19.0 / 51.7 | 40.6 / 112 | 40.7 / 112 | 40.6 / 112 | 40.7 / 112 | 39.7 / 112 |
| gxhash 3 | 18.9 / 49.2 | 18.8 / 49.1 | 18.8 / 49.6 | 18.7 / 49.1 | 40.5 / 119 | 40.6 / 119 | 40.5 / 118 | 40.5 / 118 | 39.6 / 148 |
| crc32fast | 11.6 / 13.2 | 11.5 / 13.2 | 11.5 / 13.2 | 11.6 / 13.2 | 40.5 / 75.1 | 40.0 / 75.0 | 39.8 / 75.4 | 40.2 / 75.5 | 39.2 / 75.3 |

## Every run at 1 MiB, one call, GiB/s cold / hot

| Candidate | titan znver1 | titan x86-64-v3 +aes | hyperion znver1 | hyperion x86-64-v3 +aes | europa znver1 | europa x86-64-v3 +aes | europa x86-64-v4 +aes | europa native | europa native +gxhash3-hybrid |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| crc32c | 6.66 / 7.46 | 6.66 / 7.48 | 6.66 / 7.52 | 6.66 / 7.47 | 21.0 / 37.2 | 21.1 / 37.3 | 20.9 / 37.1 | 21.1 / 37.3 | 21.0 / 37.1 |
| crc-fast crc32c | 12.1 / 12.7 | 10.8 / 11.2 | 12.1 / 12.7 | 10.8 / 11.2 | 40.9 / 76.5 | 40.1 / 76.6 | 39.9 / 76.3 | 40.7 / 76.9 | 40.6 / 76.9 |
| crc-fast crc64nvme | 11.5 / 13.2 | 11.5 / 13.2 | 11.5 / 13.2 | 11.4 / 13.2 | 40.7 / 76.0 | 39.7 / 76.6 | 40.3 / 76.6 | 40.6 / 76.0 | 40.8 / 76.4 |
| crc64fast-nvme | 11.6 / 13.2 | 11.6 / 13.2 | 11.6 / 13.2 | 11.5 / 13.2 | 17.8 / 19.5 | 17.8 / 19.5 | 17.8 / 19.5 | 17.9 / 19.6 | 17.9 / 19.6 |
| xxh3-64 | 14.1 / 20.9 | 14.1 / 21.0 | 14.1 / 21.0 | 14.1 / 21.1 | 41.2 / 75.6 | 42.4 / 75.2 | 40.1 / 90.0 | 40.7 / 88.4 | 40.7 / 89.0 |
| xxh3-128 | 14.1 / 20.6 | 4.65 / 20.8 | 14.1 / 20.7 | 14.1 / 20.9 | 40.6 / 75.6 | 42.5 / 74.7 | 40.2 / 83.6 | 40.5 / 85.1 | 40.4 / 86.5 |
| blake3 | 1.76 / 1.97 | 1.79 / 1.97 | 1.76 / 1.97 | 1.78 / 1.97 | 5.85 / 8.68 | 5.83 / 8.72 | 5.86 / 8.72 | 5.89 / 8.77 | 5.85 / 8.73 |
| gxhash 2 | 19.2 / 50.5 | 19.2 / 50.6 | 19.2 / 50.5 | 19.2 / 50.5 | 39.8 / 104 | 39.7 / 104 | 40.7 / 104 | 40.7 / 103 | 40.7 / 103 |
| gxhash 3 | 19.0 / 48.8 | 18.9 / 48.5 | 19.0 / 48.9 | 18.9 / 48.6 | 39.7 / 107 | 39.6 / 107 | 40.7 / 106 | 40.6 / 105 | 40.8 / 126 |
| crc32fast | 11.6 / 13.2 | 11.6 / 13.2 | 11.6 / 13.2 | 11.6 / 13.2 | 39.7 / 76.7 | 39.6 / 76.8 | 40.7 / 76.8 | 40.6 / 76.8 | 40.6 / 76.8 |

## Speed, run by run

#### titan znver1, one call over a unit, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 4.92 | 6.13 | 6.47 | 6.60 | 6.66 |
| crc-fast crc32c | 4.79 | 8.79 | 11.1 | 11.8 | 12.1 |
| crc-fast crc64nvme | 11.3 | 11.3 | 11.4 | 11.5 | 11.5 |
| crc64fast-nvme | 11.6 | 11.5 | 11.5 | 11.6 | 11.6 |
| xxh3-64 | 14.0 | 14.0 | 14.0 | 14.1 | 14.1 |
| xxh3-128 | 14.0 | 14.0 | 14.0 | 14.1 | 14.1 |
| blake3 | 1.24 | 1.74 | 1.76 | 1.77 | 1.76 |
| gxhash 2 | 17.9 | 18.5 | 19.0 | 19.1 | 19.2 |
| gxhash 3 | 18.2 | 18.6 | 18.9 | 19.0 | 19.0 |
| crc32fast | 11.4 | 11.6 | 11.6 | 11.5 | 11.6 |

#### titan znver1, one call over a unit, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 6.83 | 6.94 | 6.82 | 7.44 | 7.46 |
| crc-fast crc32c | 9.61 | 11.4 | 12.4 | 12.7 | 12.7 |
| crc-fast crc64nvme | 12.3 | 12.9 | 13.1 | 13.2 | 13.2 |
| crc64fast-nvme | 12.5 | 12.9 | 13.2 | 13.2 | 13.2 |
| xxh3-64 | 21.0 | 21.6 | 20.9 | 20.9 | 20.9 |
| xxh3-128 | 20.6 | 21.2 | 20.7 | 20.8 | 20.6 |
| blake3 | 1.36 | 1.97 | 1.98 | 1.99 | 1.97 |
| gxhash 2 | 45.4 | 52.3 | 51.0 | 51.1 | 50.5 |
| gxhash 3 | 56.6 | 59.4 | 49.2 | 49.4 | 48.8 |
| crc32fast | 12.5 | 13.0 | 13.2 | 13.2 | 13.2 |

#### titan znver1, the unit fed in pieces of 4 KiB, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 4.91 | 5.26 | 5.36 | 5.24 | 5.23 |
| crc-fast crc32c | 4.41 | 4.74 | 4.65 | 4.70 | 4.90 |
| crc-fast crc64nvme | 10.7 | 11.2 | 11.5 | 11.5 | 11.6 |
| crc64fast-nvme | 11.5 | 9.94 | 9.83 | 9.81 | 9.85 |
| xxh3-64 | 12.0 | 12.5 | 12.7 | 12.8 | 12.8 |
| xxh3-128 | 11.9 | 12.4 | 12.7 | 12.8 | 12.8 |
| blake3 | 1.23 | 1.22 | 1.22 | 1.21 | 1.22 |
| gxhash 2 | 17.9 | 18.2 | 18.5 | 18.5 | 18.6 |
| gxhash 3 | 17.8 | 18.3 | 18.5 | 18.6 | 18.6 |
| crc32fast | 11.3 | 11.4 | 11.4 | 11.5 | 11.5 |

#### titan znver1, the unit fed in pieces of 4 KiB, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 6.80 | 6.72 | 6.69 | 6.70 | 6.68 |
| crc-fast crc32c | 9.11 | 9.39 | 9.48 | 9.53 | 9.24 |
| crc-fast crc64nvme | 11.4 | 12.1 | 12.3 | 12.4 | 12.4 |
| crc64fast-nvme | 12.3 | 12.5 | 12.6 | 12.6 | 12.6 |
| xxh3-64 | 17.2 | 18.1 | 18.2 | 18.4 | 18.1 |
| xxh3-128 | 17.0 | 18.0 | 18.2 | 18.2 | 18.1 |
| blake3 | 1.34 | 1.32 | 1.32 | 1.31 | 1.31 |
| gxhash 2 | 43.2 | 47.4 | 46.1 | 46.4 | 45.6 |
| gxhash 3 | 53.9 | 56.8 | 48.5 | 48.7 | 47.9 |
| crc32fast | 12.1 | 12.6 | 12.7 | 12.7 | 12.7 |

#### titan znver1, one call over a unit, cold: microseconds a call

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 0.776 | 2.49 | 9.43 | 37.0 | 147 |
| crc-fast crc32c | 0.797 | 1.74 | 5.50 | 20.8 | 80.9 |
| crc-fast crc64nvme | 0.339 | 1.34 | 5.34 | 21.3 | 84.9 |
| crc64fast-nvme | 0.330 | 1.32 | 5.28 | 21.1 | 84.2 |
| xxh3-64 | 0.272 | 1.09 | 4.35 | 17.4 | 69.5 |
| xxh3-128 | 0.272 | 1.09 | 4.35 | 17.4 | 69.4 |
| blake3 | 3.08 | 8.75 | 34.6 | 138 | 554 |
| gxhash 2 | 0.213 | 0.824 | 3.21 | 12.8 | 50.8 |
| gxhash 3 | 0.210 | 0.821 | 3.23 | 12.9 | 51.3 |
| crc32fast | 0.336 | 1.32 | 5.28 | 21.2 | 84.5 |

#### titan znver1, one combine: nanoseconds, by the second part's length

| Candidate | 4 KiB | 64 KiB | 1 MiB |
| --- | --- | --- | --- |
| crc32c | 25243 | 37261 | 50308 |
| crc-fast crc32c | 28115 | 40825 | 54084 |
| crc-fast crc64nvme | 124206 | 179837 | 236158 |
| crc32fast | 78.8 | 88.8 | 89.9 |
| harness crc32c | 73.2 | 79.1 | 77.4 |
| harness crc32c, fixed length | 38.9 | 38.6 | 39.1 |
| harness crc64nvme | 150 | 155 | 156 |
| harness crc64nvme, fixed length | 77.3 | 78.0 | 79.0 |

#### titan x86-64-v3 +aes, one call over a unit, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 5.14 | 6.20 | 6.51 | 6.62 | 6.66 |
| crc-fast crc32c | 4.75 | 8.09 | 9.64 | 10.4 | 10.8 |
| crc-fast crc64nvme | 11.3 | 11.2 | 11.3 | 10.9 | 11.5 |
| crc64fast-nvme | 11.6 | 11.4 | 11.5 | 11.4 | 11.6 |
| xxh3-64 | 14.1 | 14.1 | 14.1 | 14.0 | 14.1 |
| xxh3-128 | 14.0 | 13.0 | 14.0 | 10.1 | 4.65 |
| blake3 | 1.25 | 1.76 | 1.78 | 1.78 | 1.79 |
| gxhash 2 | 17.8 | 18.5 | 19.0 | 19.1 | 19.2 |
| gxhash 3 | 17.8 | 18.5 | 18.8 | 18.9 | 18.9 |
| crc32fast | 11.6 | 11.6 | 11.5 | 11.5 | 11.6 |

#### titan x86-64-v3 +aes, one call over a unit, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 6.77 | 6.86 | 7.09 | 7.46 | 7.48 |
| crc-fast crc32c | 8.60 | 10.1 | 10.9 | 11.1 | 11.2 |
| crc-fast crc64nvme | 12.2 | 12.8 | 13.1 | 13.2 | 13.2 |
| crc64fast-nvme | 12.3 | 12.9 | 13.2 | 13.2 | 13.2 |
| xxh3-64 | 21.0 | 21.7 | 20.9 | 21.0 | 21.0 |
| xxh3-128 | 8.19 | 4.88 | 20.8 | 20.9 | 20.8 |
| blake3 | 1.36 | 1.97 | 1.99 | 1.99 | 1.97 |
| gxhash 2 | 45.6 | 52.9 | 51.0 | 51.1 | 50.6 |
| gxhash 3 | 54.5 | 59.0 | 49.1 | 48.8 | 48.5 |
| crc32fast | 12.6 | 13.0 | 13.2 | 13.2 | 13.2 |

#### titan x86-64-v3 +aes, the unit fed in pieces of 4 KiB, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 5.23 | 5.84 | 5.92 | 5.91 | 5.91 |
| crc-fast crc32c | 4.52 | 4.98 | 5.22 | 5.24 | 5.01 |
| crc-fast crc64nvme | 10.8 | 11.3 | 11.5 | 11.6 | 11.6 |
| crc64fast-nvme | 11.4 | 11.5 | 11.6 | 11.6 | 11.6 |
| xxh3-64 | 11.9 | 12.4 | 12.6 | 12.7 | 12.7 |
| xxh3-128 | 11.7 | 12.3 | 12.6 | 12.7 | 12.6 |
| blake3 | 1.23 | 1.22 | 1.21 | 1.21 | 1.22 |
| gxhash 2 | 17.9 | 18.2 | 18.5 | 18.5 | 18.6 |
| gxhash 3 | 17.7 | 17.7 | 17.7 | 17.7 | 17.8 |
| crc32fast | 11.4 | 11.5 | 11.5 | 11.5 | 11.5 |

#### titan x86-64-v3 +aes, the unit fed in pieces of 4 KiB, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 6.79 | 6.71 | 6.72 | 6.71 | 6.69 |
| crc-fast crc32c | 8.16 | 8.48 | 8.53 | 8.53 | 8.37 |
| crc-fast crc64nvme | 11.5 | 12.1 | 12.3 | 12.4 | 12.4 |
| crc64fast-nvme | 12.2 | 12.4 | 12.6 | 12.6 | 12.5 |
| xxh3-64 | 17.2 | 18.3 | 18.2 | 18.3 | 18.3 |
| xxh3-128 | 16.9 | 18.2 | 18.2 | 18.3 | 18.3 |
| blake3 | 1.34 | 1.32 | 1.32 | 1.31 | 1.31 |
| gxhash 2 | 42.1 | 47.4 | 46.2 | 46.0 | 45.7 |
| gxhash 3 | 53.3 | 57.2 | 48.4 | 48.3 | 47.7 |
| crc32fast | 12.1 | 12.6 | 12.8 | 12.9 | 12.8 |

#### titan x86-64-v3 +aes, one call over a unit, cold: microseconds a call

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 0.742 | 2.46 | 9.38 | 36.9 | 147 |
| crc-fast crc32c | 0.803 | 1.89 | 6.33 | 23.4 | 90.7 |
| crc-fast crc64nvme | 0.337 | 1.37 | 5.40 | 22.4 | 85.2 |
| crc64fast-nvme | 0.330 | 1.33 | 5.30 | 21.4 | 84.4 |
| xxh3-64 | 0.271 | 1.09 | 4.34 | 17.4 | 69.4 |
| xxh3-128 | 0.272 | 1.18 | 4.34 | 24.1 | 210 |
| blake3 | 3.05 | 8.65 | 34.2 | 137 | 547 |
| gxhash 2 | 0.214 | 0.824 | 3.21 | 12.8 | 50.8 |
| gxhash 3 | 0.215 | 0.825 | 3.25 | 12.9 | 51.6 |
| crc32fast | 0.329 | 1.32 | 5.29 | 21.2 | 84.5 |

#### titan x86-64-v3 +aes, one combine: nanoseconds, by the second part's length

| Candidate | 4 KiB | 64 KiB | 1 MiB |
| --- | --- | --- | --- |
| crc32c | 24942 | 36127 | 48581 |
| crc-fast crc32c | 25759 | 38040 | 51307 |
| crc-fast crc64nvme | 124788 | 184499 | 244720 |
| crc32fast | 82.1 | 88.0 | 90.8 |
| harness crc32c | 71.5 | 76.1 | 77.2 |
| harness crc32c, fixed length | 37.2 | 37.6 | 38.3 |
| harness crc64nvme | 147 | 151 | 153 |
| harness crc64nvme, fixed length | 74.3 | 75.7 | 76.5 |

#### hyperion znver1, one call over a unit, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 4.92 | 6.11 | 6.49 | 6.60 | 6.66 |
| crc-fast crc32c | 4.86 | 8.71 | 10.9 | 11.7 | 12.1 |
| crc-fast crc64nvme | 11.3 | 11.3 | 11.4 | 11.4 | 11.5 |
| crc64fast-nvme | 11.6 | 11.5 | 11.6 | 11.5 | 11.6 |
| xxh3-64 | 14.0 | 14.0 | 14.0 | 14.0 | 14.1 |
| xxh3-128 | 14.0 | 14.0 | 14.0 | 14.0 | 14.1 |
| blake3 | 1.24 | 1.75 | 1.76 | 1.77 | 1.76 |
| gxhash 2 | 17.8 | 18.5 | 19.0 | 19.1 | 19.2 |
| gxhash 3 | 18.2 | 18.5 | 18.8 | 18.9 | 19.0 |
| crc32fast | 11.4 | 11.5 | 11.5 | 11.5 | 11.6 |

#### hyperion znver1, one call over a unit, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 6.84 | 6.99 | 7.13 | 7.40 | 7.52 |
| crc-fast crc32c | 9.61 | 11.4 | 12.4 | 12.7 | 12.7 |
| crc-fast crc64nvme | 12.3 | 12.9 | 13.1 | 13.2 | 13.2 |
| crc64fast-nvme | 12.5 | 13.0 | 13.2 | 13.2 | 13.2 |
| xxh3-64 | 21.0 | 21.6 | 20.9 | 20.8 | 21.0 |
| xxh3-128 | 20.6 | 21.3 | 20.7 | 20.6 | 20.7 |
| blake3 | 1.36 | 1.97 | 1.99 | 1.99 | 1.97 |
| gxhash 2 | 45.4 | 53.2 | 51.6 | 50.9 | 50.5 |
| gxhash 3 | 56.7 | 58.5 | 49.6 | 49.2 | 48.9 |
| crc32fast | 12.5 | 13.1 | 13.2 | 13.2 | 13.2 |

#### hyperion znver1, the unit fed in pieces of 4 KiB, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 4.92 | 5.26 | 5.33 | 5.21 | 5.22 |
| crc-fast crc32c | 4.35 | 4.55 | 4.65 | 4.75 | 4.91 |
| crc-fast crc64nvme | 10.7 | 11.3 | 11.5 | 11.5 | 11.6 |
| crc64fast-nvme | 11.5 | 9.77 | 9.69 | 9.78 | 9.80 |
| xxh3-64 | 12.0 | 12.4 | 12.7 | 12.7 | 12.8 |
| xxh3-128 | 11.8 | 12.4 | 12.6 | 12.7 | 12.8 |
| blake3 | 1.22 | 1.19 | 1.20 | 1.20 | 1.20 |
| gxhash 2 | 17.8 | 18.2 | 18.4 | 18.5 | 18.5 |
| gxhash 3 | 18.0 | 18.2 | 18.5 | 18.5 | 18.6 |
| crc32fast | 11.3 | 11.4 | 11.4 | 11.4 | 11.4 |

#### hyperion znver1, the unit fed in pieces of 4 KiB, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 6.80 | 6.75 | 6.71 | 6.69 | 6.68 |
| crc-fast crc32c | 9.13 | 9.40 | 9.50 | 9.53 | 9.28 |
| crc-fast crc64nvme | 11.4 | 12.1 | 12.3 | 12.4 | 12.4 |
| crc64fast-nvme | 12.3 | 12.5 | 12.6 | 12.6 | 12.6 |
| xxh3-64 | 17.2 | 18.1 | 18.2 | 18.3 | 18.3 |
| xxh3-128 | 17.0 | 18.1 | 18.2 | 18.3 | 18.3 |
| blake3 | 1.34 | 1.32 | 1.32 | 1.31 | 1.31 |
| gxhash 2 | 43.1 | 47.9 | 46.2 | 46.4 | 45.7 |
| gxhash 3 | 53.5 | 56.7 | 48.8 | 48.2 | 47.1 |
| crc32fast | 12.1 | 12.6 | 12.7 | 12.7 | 12.7 |

#### hyperion znver1, one call over a unit, cold: microseconds a call

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 0.775 | 2.50 | 9.40 | 37.0 | 147 |
| crc-fast crc32c | 0.785 | 1.75 | 5.61 | 20.8 | 80.9 |
| crc-fast crc64nvme | 0.338 | 1.34 | 5.34 | 21.3 | 84.8 |
| crc64fast-nvme | 0.330 | 1.32 | 5.28 | 21.2 | 84.3 |
| xxh3-64 | 0.272 | 1.09 | 4.35 | 17.4 | 69.4 |
| xxh3-128 | 0.272 | 1.09 | 4.35 | 17.4 | 69.4 |
| blake3 | 3.07 | 8.74 | 34.7 | 138 | 554 |
| gxhash 2 | 0.214 | 0.826 | 3.21 | 12.8 | 50.8 |
| gxhash 3 | 0.210 | 0.824 | 3.24 | 12.9 | 51.4 |
| crc32fast | 0.336 | 1.32 | 5.29 | 21.2 | 84.5 |

#### hyperion znver1, one combine: nanoseconds, by the second part's length

| Candidate | 4 KiB | 64 KiB | 1 MiB |
| --- | --- | --- | --- |
| crc32c | 25342 | 37423 | 50018 |
| crc-fast crc32c | 28071 | 40854 | 54074 |
| crc-fast crc64nvme | 124235 | 179619 | 236446 |
| crc32fast | 78.8 | 88.6 | 88.8 |
| harness crc32c | 73.2 | 78.9 | 77.4 |
| harness crc32c, fixed length | 38.8 | 38.6 | 39.1 |
| harness crc64nvme | 150 | 156 | 156 |
| harness crc64nvme, fixed length | 77.2 | 77.9 | 78.9 |

#### hyperion x86-64-v3 +aes, one call over a unit, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 5.20 | 6.21 | 6.51 | 6.62 | 6.66 |
| crc-fast crc32c | 4.82 | 8.03 | 9.73 | 10.5 | 10.8 |
| crc-fast crc64nvme | 11.3 | 11.1 | 11.3 | 10.9 | 11.4 |
| crc64fast-nvme | 11.5 | 11.4 | 11.5 | 11.2 | 11.5 |
| xxh3-64 | 14.1 | 14.1 | 14.1 | 14.1 | 14.1 |
| xxh3-128 | 14.0 | 14.1 | 14.1 | 14.1 | 14.1 |
| blake3 | 1.24 | 1.75 | 1.78 | 1.78 | 1.78 |
| gxhash 2 | 17.8 | 18.5 | 19.0 | 19.1 | 19.2 |
| gxhash 3 | 17.7 | 18.4 | 18.7 | 18.8 | 18.9 |
| crc32fast | 11.6 | 11.5 | 11.6 | 11.5 | 11.6 |

#### hyperion x86-64-v3 +aes, one call over a unit, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 6.81 | 6.90 | 7.12 | 7.55 | 7.47 |
| crc-fast crc32c | 8.60 | 10.1 | 10.9 | 11.2 | 11.2 |
| crc-fast crc64nvme | 12.3 | 12.9 | 13.2 | 13.2 | 13.2 |
| crc64fast-nvme | 12.4 | 12.9 | 13.2 | 13.2 | 13.2 |
| xxh3-64 | 21.0 | 21.7 | 21.0 | 21.0 | 21.1 |
| xxh3-128 | 20.6 | 21.4 | 20.8 | 20.9 | 20.9 |
| blake3 | 1.36 | 1.97 | 1.99 | 1.99 | 1.97 |
| gxhash 2 | 45.8 | 53.6 | 51.7 | 51.5 | 50.5 |
| gxhash 3 | 54.6 | 58.7 | 49.1 | 49.0 | 48.6 |
| crc32fast | 12.6 | 13.0 | 13.2 | 13.2 | 13.2 |

#### hyperion x86-64-v3 +aes, the unit fed in pieces of 4 KiB, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 5.22 | 5.88 | 5.92 | 5.92 | 5.93 |
| crc-fast crc32c | 4.34 | 4.40 | 5.05 | 4.93 | 4.91 |
| crc-fast crc64nvme | 10.8 | 11.3 | 11.5 | 11.5 | 11.6 |
| crc64fast-nvme | 11.5 | 11.5 | 11.6 | 11.6 | 11.6 |
| xxh3-64 | 11.9 | 12.4 | 12.7 | 12.7 | 12.8 |
| xxh3-128 | 11.8 | 12.3 | 12.6 | 12.7 | 12.8 |
| blake3 | 1.23 | 1.21 | 1.21 | 1.21 | 1.22 |
| gxhash 2 | 17.8 | 18.2 | 18.4 | 18.5 | 18.5 |
| gxhash 3 | 17.7 | 17.7 | 17.7 | 17.7 | 17.8 |
| crc32fast | 11.4 | 11.5 | 11.6 | 11.5 | 11.6 |

#### hyperion x86-64-v3 +aes, the unit fed in pieces of 4 KiB, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 6.75 | 6.76 | 6.73 | 6.73 | 6.72 |
| crc-fast crc32c | 8.16 | 8.48 | 8.53 | 8.55 | 8.38 |
| crc-fast crc64nvme | 11.5 | 12.1 | 12.3 | 12.4 | 12.4 |
| crc64fast-nvme | 12.2 | 12.4 | 12.6 | 12.6 | 12.6 |
| xxh3-64 | 17.2 | 18.3 | 18.3 | 18.4 | 18.3 |
| xxh3-128 | 16.9 | 18.2 | 18.2 | 18.4 | 18.3 |
| blake3 | 1.34 | 1.32 | 1.32 | 1.32 | 1.31 |
| gxhash 2 | 42.1 | 47.3 | 46.5 | 46.5 | 45.6 |
| gxhash 3 | 53.3 | 58.0 | 48.7 | 48.7 | 47.6 |
| crc32fast | 12.1 | 12.7 | 12.8 | 12.9 | 12.9 |

#### hyperion x86-64-v3 +aes, one call over a unit, cold: microseconds a call

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 0.733 | 2.46 | 9.38 | 36.9 | 147 |
| crc-fast crc32c | 0.791 | 1.90 | 6.27 | 23.3 | 90.8 |
| crc-fast crc64nvme | 0.336 | 1.37 | 5.42 | 22.4 | 85.7 |
| crc64fast-nvme | 0.331 | 1.34 | 5.30 | 21.7 | 84.8 |
| xxh3-64 | 0.271 | 1.08 | 4.34 | 17.3 | 69.3 |
| xxh3-128 | 0.272 | 1.09 | 4.34 | 17.4 | 69.3 |
| blake3 | 3.08 | 8.70 | 34.3 | 137 | 548 |
| gxhash 2 | 0.214 | 0.825 | 3.21 | 12.8 | 50.9 |
| gxhash 3 | 0.215 | 0.828 | 3.26 | 13.0 | 51.7 |
| crc32fast | 0.329 | 1.32 | 5.28 | 21.2 | 84.4 |

#### hyperion x86-64-v3 +aes, one combine: nanoseconds, by the second part's length

| Candidate | 4 KiB | 64 KiB | 1 MiB |
| --- | --- | --- | --- |
| crc32c | 24886 | 35977 | 48626 |
| crc-fast crc32c | 25648 | 38038 | 51243 |
| crc-fast crc64nvme | 124412 | 183719 | 243615 |
| crc32fast | 83.1 | 89.6 | 92.5 |
| harness crc32c | 71.5 | 76.0 | 77.1 |
| harness crc32c, fixed length | 37.2 | 37.6 | 38.3 |
| harness crc64nvme | 147 | 151 | 152 |
| harness crc64nvme, fixed length | 74.3 | 75.7 | 76.4 |

#### europa znver1, one call over a unit, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 24.6 | 26.2 | 19.6 | 20.7 | 21.0 |
| crc-fast crc32c | 40.2 | 39.4 | 39.5 | 39.9 | 40.9 |
| crc-fast crc64nvme | 40.1 | 39.5 | 40.5 | 40.4 | 40.7 |
| crc64fast-nvme | 17.7 | 17.8 | 17.8 | 17.8 | 17.8 |
| xxh3-64 | 43.0 | 42.5 | 42.9 | 41.4 | 41.2 |
| xxh3-128 | 40.3 | 42.4 | 40.5 | 40.5 | 40.6 |
| blake3 | 3.23 | 5.59 | 5.81 | 5.85 | 5.85 |
| gxhash 2 | 39.7 | 40.6 | 40.6 | 40.7 | 39.8 |
| gxhash 3 | 39.8 | 40.6 | 40.5 | 40.6 | 39.7 |
| crc32fast | 40.4 | 40.2 | 40.5 | 40.3 | 39.7 |

#### europa znver1, one call over a unit, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 28.3 | 30.0 | 34.4 | 36.9 | 37.2 |
| crc-fast crc32c | 57.1 | 71.1 | 75.1 | 76.4 | 76.5 |
| crc-fast crc64nvme | 61.6 | 72.5 | 75.0 | 75.9 | 76.0 |
| crc64fast-nvme | 19.0 | 19.3 | 19.5 | 19.5 | 19.5 |
| xxh3-64 | 69.8 | 76.9 | 77.8 | 79.0 | 75.6 |
| xxh3-128 | 68.1 | 76.3 | 77.4 | 78.8 | 75.6 |
| blake3 | 3.89 | 8.24 | 8.62 | 8.73 | 8.68 |
| gxhash 2 | 100 | 112 | 112 | 114 | 104 |
| gxhash 3 | 113 | 120 | 119 | 120 | 107 |
| crc32fast | 56.4 | 70.6 | 75.1 | 76.3 | 76.7 |

#### europa znver1, the unit fed in pieces of 4 KiB, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 24.7 | 24.7 | 24.8 | 24.9 | 24.9 |
| crc-fast crc32c | 41.4 | 39.0 | 39.9 | 39.8 | 40.1 |
| crc-fast crc64nvme | 40.8 | 39.1 | 40.1 | 40.0 | 40.2 |
| crc64fast-nvme | 17.7 | 17.8 | 17.8 | 17.8 | 17.7 |
| xxh3-64 | 42.4 | 42.2 | 42.9 | 40.3 | 40.4 |
| xxh3-128 | 41.5 | 42.1 | 40.1 | 40.1 | 40.4 |
| blake3 | 3.13 | 3.09 | 3.11 | 3.11 | 3.10 |
| gxhash 2 | 39.7 | 40.4 | 40.4 | 40.5 | 39.5 |
| gxhash 3 | 39.5 | 40.5 | 40.4 | 40.6 | 39.6 |
| crc32fast | 41.7 | 41.0 | 41.0 | 40.7 | 39.4 |

#### europa znver1, the unit fed in pieces of 4 KiB, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 28.3 | 28.2 | 28.0 | 28.1 | 27.8 |
| crc-fast crc32c | 53.7 | 57.4 | 57.8 | 58.3 | 57.6 |
| crc-fast crc64nvme | 55.8 | 60.9 | 62.1 | 62.5 | 61.4 |
| crc64fast-nvme | 18.9 | 19.1 | 19.1 | 19.1 | 19.1 |
| xxh3-64 | 58.4 | 67.1 | 68.8 | 69.7 | 67.1 |
| xxh3-128 | 59.2 | 67.5 | 68.6 | 69.5 | 67.1 |
| blake3 | 3.86 | 3.80 | 3.77 | 3.77 | 3.75 |
| gxhash 2 | 98.7 | 105 | 103 | 104 | 97.5 |
| gxhash 3 | 111 | 118 | 117 | 119 | 105 |
| crc32fast | 55.1 | 56.3 | 56.6 | 56.7 | 56.6 |

#### europa znver1, one call over a unit, cold: microseconds a call

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 0.155 | 0.581 | 3.12 | 11.8 | 46.5 |
| crc-fast crc32c | 0.095 | 0.388 | 1.54 | 6.12 | 23.9 |
| crc-fast crc64nvme | 0.095 | 0.386 | 1.51 | 6.05 | 24.0 |
| crc64fast-nvme | 0.215 | 0.859 | 3.42 | 13.7 | 54.8 |
| xxh3-64 | 0.089 | 0.359 | 1.42 | 5.90 | 23.7 |
| xxh3-128 | 0.095 | 0.359 | 1.51 | 6.02 | 24.1 |
| blake3 | 1.18 | 2.73 | 10.5 | 41.7 | 167 |
| gxhash 2 | 0.096 | 0.376 | 1.50 | 6.00 | 24.5 |
| gxhash 3 | 0.096 | 0.376 | 1.51 | 6.01 | 24.6 |
| crc32fast | 0.094 | 0.379 | 1.51 | 6.06 | 24.6 |

#### europa znver1, one combine: nanoseconds, by the second part's length

| Candidate | 4 KiB | 64 KiB | 1 MiB |
| --- | --- | --- | --- |
| crc32c | 6106 | 8021 | 10004 |
| crc-fast crc32c | 6466 | 8540 | 10649 |
| crc-fast crc64nvme | 22729 | 30887 | 39559 |
| crc32fast | 36.0 | 37.2 | 39.3 |
| harness crc32c | 40.0 | 42.1 | 43.5 |
| harness crc32c, fixed length | 24.5 | 24.3 | 25.0 |
| harness crc64nvme | 91.2 | 93.9 | 94.1 |
| harness crc64nvme, fixed length | 50.2 | 50.5 | 50.3 |

#### europa x86-64-v3 +aes, one call over a unit, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 24.6 | 26.2 | 19.7 | 20.8 | 21.1 |
| crc-fast crc32c | 41.3 | 40.1 | 39.4 | 40.2 | 40.1 |
| crc-fast crc64nvme | 41.9 | 40.1 | 37.4 | 39.8 | 39.7 |
| crc64fast-nvme | 17.7 | 17.8 | 17.8 | 17.8 | 17.8 |
| xxh3-64 | 39.4 | 40.4 | 43.2 | 43.5 | 42.4 |
| xxh3-128 | 39.5 | 40.2 | 42.7 | 42.3 | 42.5 |
| blake3 | 3.25 | 5.52 | 5.77 | 5.85 | 5.83 |
| gxhash 2 | 40.1 | 40.5 | 40.7 | 39.7 | 39.7 |
| gxhash 3 | 40.5 | 40.5 | 40.6 | 39.6 | 39.6 |
| crc32fast | 39.8 | 40.0 | 40.0 | 39.4 | 39.6 |

#### europa x86-64-v3 +aes, one call over a unit, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 28.0 | 29.9 | 34.4 | 36.8 | 37.3 |
| crc-fast crc32c | 57.1 | 71.2 | 75.3 | 76.5 | 76.6 |
| crc-fast crc64nvme | 61.6 | 72.5 | 75.1 | 76.3 | 76.6 |
| crc64fast-nvme | 18.9 | 19.4 | 19.5 | 19.5 | 19.5 |
| xxh3-64 | 68.8 | 75.6 | 77.3 | 78.1 | 75.2 |
| xxh3-128 | 68.3 | 75.3 | 76.6 | 77.9 | 74.7 |
| blake3 | 3.87 | 8.21 | 8.58 | 8.71 | 8.72 |
| gxhash 2 | 100 | 112 | 112 | 114 | 104 |
| gxhash 3 | 110 | 119 | 119 | 120 | 107 |
| crc32fast | 55.2 | 70.0 | 75.0 | 76.2 | 76.8 |

#### europa x86-64-v3 +aes, the unit fed in pieces of 4 KiB, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 24.7 | 24.9 | 24.9 | 24.9 | 24.9 |
| crc-fast crc32c | 41.2 | 39.7 | 39.7 | 39.7 | 38.9 |
| crc-fast crc64nvme | 41.3 | 39.6 | 39.3 | 39.7 | 39.1 |
| crc64fast-nvme | 17.7 | 17.7 | 17.7 | 17.8 | 17.7 |
| xxh3-64 | 39.2 | 39.9 | 42.2 | 42.8 | 42.1 |
| xxh3-128 | 39.0 | 39.8 | 42.3 | 41.8 | 42.2 |
| blake3 | 3.24 | 3.15 | 3.13 | 3.12 | 3.12 |
| gxhash 2 | 40.3 | 40.4 | 40.4 | 39.4 | 39.5 |
| gxhash 3 | 40.5 | 40.4 | 40.5 | 39.5 | 39.6 |
| crc32fast | 41.2 | 39.7 | 40.0 | 38.9 | 39.1 |

#### europa x86-64-v3 +aes, the unit fed in pieces of 4 KiB, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 27.8 | 28.3 | 27.8 | 27.6 | 27.7 |
| crc-fast crc32c | 54.0 | 57.5 | 58.0 | 58.2 | 57.7 |
| crc-fast crc64nvme | 55.1 | 60.8 | 62.1 | 62.5 | 62.4 |
| crc64fast-nvme | 18.9 | 19.1 | 19.1 | 19.1 | 19.1 |
| xxh3-64 | 58.1 | 65.4 | 66.7 | 67.9 | 65.0 |
| xxh3-128 | 58.7 | 65.8 | 65.9 | 67.7 | 65.0 |
| blake3 | 3.85 | 3.80 | 3.76 | 3.76 | 3.74 |
| gxhash 2 | 98.5 | 105 | 103 | 104 | 95.6 |
| gxhash 3 | 110 | 117 | 115 | 117 | 104 |
| crc32fast | 51.0 | 55.5 | 56.5 | 56.8 | 56.9 |

#### europa x86-64-v3 +aes, one call over a unit, cold: microseconds a call

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 0.155 | 0.583 | 3.10 | 11.7 | 46.3 |
| crc-fast crc32c | 0.092 | 0.381 | 1.55 | 6.07 | 24.4 |
| crc-fast crc64nvme | 0.091 | 0.381 | 1.63 | 6.14 | 24.6 |
| crc64fast-nvme | 0.215 | 0.857 | 3.43 | 13.7 | 54.8 |
| xxh3-64 | 0.097 | 0.378 | 1.41 | 5.62 | 23.0 |
| xxh3-128 | 0.097 | 0.380 | 1.43 | 5.78 | 23.0 |
| blake3 | 1.17 | 2.77 | 10.6 | 41.7 | 168 |
| gxhash 2 | 0.095 | 0.377 | 1.50 | 6.15 | 24.6 |
| gxhash 3 | 0.094 | 0.376 | 1.50 | 6.17 | 24.6 |
| crc32fast | 0.096 | 0.381 | 1.52 | 6.20 | 24.6 |

#### europa x86-64-v3 +aes, one combine: nanoseconds, by the second part's length

| Candidate | 4 KiB | 64 KiB | 1 MiB |
| --- | --- | --- | --- |
| crc32c | 7054 | 9269 | 11549 |
| crc-fast crc32c | 7396 | 9734 | 12098 |
| crc-fast crc64nvme | 26126 | 35402 | 44891 |
| crc32fast | 43.8 | 46.6 | 50.5 |
| harness crc32c | 39.1 | 41.4 | 42.3 |
| harness crc32c, fixed length | 24.3 | 24.3 | 24.8 |
| harness crc64nvme | 90.7 | 92.3 | 92.0 |
| harness crc64nvme, fixed length | 49.7 | 50.5 | 50.4 |

#### europa x86-64-v4 +aes, one call over a unit, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 24.7 | 26.1 | 19.6 | 20.7 | 20.9 |
| crc-fast crc32c | 42.4 | 40.2 | 39.2 | 40.2 | 39.9 |
| crc-fast crc64nvme | 40.5 | 40.2 | 37.5 | 39.8 | 40.3 |
| crc64fast-nvme | 17.6 | 17.8 | 17.8 | 17.8 | 17.8 |
| xxh3-64 | 40.2 | 40.3 | 37.9 | 39.6 | 40.1 |
| xxh3-128 | 40.3 | 40.4 | 40.4 | 40.6 | 40.2 |
| blake3 | 3.21 | 5.61 | 5.79 | 5.83 | 5.86 |
| gxhash 2 | 40.6 | 40.5 | 40.6 | 40.1 | 40.7 |
| gxhash 3 | 40.5 | 40.5 | 40.5 | 40.1 | 40.7 |
| crc32fast | 39.8 | 40.2 | 39.8 | 39.4 | 40.7 |

#### europa x86-64-v4 +aes, one call over a unit, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 28.0 | 29.8 | 34.2 | 36.8 | 37.1 |
| crc-fast crc32c | 57.4 | 71.2 | 75.1 | 76.4 | 76.3 |
| crc-fast crc64nvme | 62.4 | 72.7 | 75.1 | 76.1 | 76.6 |
| crc64fast-nvme | 18.9 | 19.4 | 19.5 | 19.5 | 19.5 |
| xxh3-64 | 80.2 | 89.4 | 90.5 | 91.5 | 90.0 |
| xxh3-128 | 74.9 | 83.3 | 84.2 | 85.1 | 83.6 |
| blake3 | 3.89 | 8.26 | 8.61 | 8.69 | 8.72 |
| gxhash 2 | 100 | 112 | 112 | 114 | 104 |
| gxhash 3 | 112 | 119 | 118 | 120 | 106 |
| crc32fast | 59.1 | 71.5 | 75.4 | 76.3 | 76.8 |

#### europa x86-64-v4 +aes, the unit fed in pieces of 4 KiB, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 24.6 | 24.8 | 24.8 | 24.9 | 24.8 |
| crc-fast crc32c | 42.1 | 39.6 | 39.7 | 39.7 | 39.6 |
| crc-fast crc64nvme | 39.7 | 39.8 | 39.6 | 39.9 | 39.9 |
| crc64fast-nvme | 17.6 | 17.7 | 17.8 | 17.8 | 17.7 |
| xxh3-64 | 39.7 | 40.1 | 40.1 | 40.1 | 39.9 |
| xxh3-128 | 39.5 | 39.9 | 40.1 | 40.1 | 39.9 |
| blake3 | 3.15 | 3.09 | 3.09 | 3.09 | 3.09 |
| gxhash 2 | 40.5 | 40.3 | 40.4 | 40.1 | 40.5 |
| gxhash 3 | 40.6 | 40.5 | 40.5 | 39.6 | 40.6 |
| crc32fast | 39.9 | 40.0 | 40.0 | 39.0 | 40.2 |

#### europa x86-64-v4 +aes, the unit fed in pieces of 4 KiB, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 27.9 | 28.2 | 27.7 | 27.8 | 27.6 |
| crc-fast crc32c | 54.0 | 57.6 | 58.2 | 58.4 | 58.0 |
| crc-fast crc64nvme | 56.2 | 61.9 | 63.1 | 63.5 | 62.9 |
| crc64fast-nvme | 18.9 | 19.1 | 19.1 | 19.2 | 19.1 |
| xxh3-64 | 61.9 | 72.0 | 74.2 | 73.0 | 70.4 |
| xxh3-128 | 61.1 | 71.2 | 74.1 | 72.8 | 70.3 |
| blake3 | 3.87 | 3.80 | 3.77 | 3.77 | 3.75 |
| gxhash 2 | 98.3 | 105 | 103 | 104 | 97.4 |
| gxhash 3 | 109 | 117 | 115 | 117 | 104 |
| crc32fast | 57.4 | 58.6 | 58.3 | 58.4 | 58.1 |

#### europa x86-64-v4 +aes, one call over a unit, cold: microseconds a call

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 0.155 | 0.585 | 3.11 | 11.8 | 46.7 |
| crc-fast crc32c | 0.090 | 0.380 | 1.56 | 6.08 | 24.5 |
| crc-fast crc64nvme | 0.094 | 0.379 | 1.63 | 6.13 | 24.2 |
| crc64fast-nvme | 0.217 | 0.857 | 3.43 | 13.7 | 54.8 |
| xxh3-64 | 0.095 | 0.378 | 1.61 | 6.17 | 24.3 |
| xxh3-128 | 0.095 | 0.377 | 1.51 | 6.02 | 24.3 |
| blake3 | 1.19 | 2.72 | 10.5 | 41.9 | 167 |
| gxhash 2 | 0.094 | 0.377 | 1.50 | 6.09 | 24.0 |
| gxhash 3 | 0.094 | 0.377 | 1.51 | 6.10 | 24.0 |
| crc32fast | 0.096 | 0.380 | 1.53 | 6.20 | 24.0 |

#### europa x86-64-v4 +aes, one combine: nanoseconds, by the second part's length

| Candidate | 4 KiB | 64 KiB | 1 MiB |
| --- | --- | --- | --- |
| crc32c | 7481 | 9871 | 12272 |
| crc-fast crc32c | 7325 | 9635 | 12006 |
| crc-fast crc64nvme | 26010 | 35156 | 44508 |
| crc32fast | 30.0 | 31.1 | 32.5 |
| harness crc32c | 39.6 | 41.5 | 42.0 |
| harness crc32c, fixed length | 24.3 | 24.3 | 24.8 |
| harness crc64nvme | 90.7 | 92.3 | 92.0 |
| harness crc64nvme, fixed length | 49.7 | 50.5 | 50.3 |

#### europa native, one call over a unit, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 24.8 | 26.3 | 19.7 | 20.8 | 21.1 |
| crc-fast crc32c | 41.4 | 39.6 | 39.5 | 40.2 | 40.7 |
| crc-fast crc64nvme | 39.2 | 40.1 | 38.2 | 39.9 | 40.6 |
| crc64fast-nvme | 17.7 | 17.9 | 17.9 | 17.9 | 17.9 |
| xxh3-64 | 39.4 | 40.3 | 38.7 | 39.8 | 40.7 |
| xxh3-128 | 39.4 | 40.4 | 40.5 | 40.3 | 40.5 |
| blake3 | 3.27 | 5.65 | 5.84 | 5.85 | 5.89 |
| gxhash 2 | 40.6 | 40.6 | 40.7 | 40.7 | 40.7 |
| gxhash 3 | 40.5 | 40.5 | 40.5 | 40.6 | 40.6 |
| crc32fast | 40.1 | 40.4 | 40.2 | 40.4 | 40.6 |

#### europa native, one call over a unit, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 28.5 | 30.0 | 34.4 | 37.1 | 37.3 |
| crc-fast crc32c | 57.3 | 71.3 | 75.5 | 76.7 | 76.9 |
| crc-fast crc64nvme | 62.6 | 73.0 | 75.4 | 76.5 | 76.0 |
| crc64fast-nvme | 19.0 | 19.6 | 19.6 | 19.6 | 19.6 |
| xxh3-64 | 77.8 | 88.1 | 90.2 | 91.7 | 88.4 |
| xxh3-128 | 77.6 | 86.7 | 87.9 | 88.7 | 85.1 |
| blake3 | 3.90 | 8.27 | 8.62 | 8.74 | 8.77 |
| gxhash 2 | 100 | 112 | 112 | 114 | 103 |
| gxhash 3 | 113 | 119 | 118 | 120 | 105 |
| crc32fast | 58.0 | 70.9 | 75.5 | 76.7 | 76.8 |

#### europa native, the unit fed in pieces of 4 KiB, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 24.6 | 24.8 | 24.9 | 24.9 | 24.9 |
| crc-fast crc32c | 40.7 | 39.6 | 39.9 | 39.8 | 40.1 |
| crc-fast crc64nvme | 38.7 | 39.8 | 39.9 | 40.0 | 40.4 |
| crc64fast-nvme | 17.7 | 17.8 | 17.9 | 17.8 | 17.8 |
| xxh3-64 | 39.1 | 40.1 | 40.2 | 40.2 | 40.3 |
| xxh3-128 | 38.6 | 39.9 | 40.0 | 40.1 | 40.4 |
| blake3 | 3.19 | 3.10 | 3.09 | 3.06 | 3.06 |
| gxhash 2 | 40.5 | 40.5 | 40.5 | 40.6 | 40.6 |
| gxhash 3 | 40.5 | 40.4 | 40.4 | 40.6 | 40.5 |
| crc32fast | 40.0 | 40.0 | 40.2 | 40.2 | 40.2 |

#### europa native, the unit fed in pieces of 4 KiB, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 28.1 | 28.3 | 28.1 | 28.2 | 28.0 |
| crc-fast crc32c | 54.2 | 57.9 | 58.5 | 58.7 | 57.9 |
| crc-fast crc64nvme | 55.5 | 62.0 | 63.2 | 63.6 | 62.5 |
| crc64fast-nvme | 19.0 | 19.2 | 19.2 | 19.2 | 19.2 |
| xxh3-64 | 57.4 | 69.2 | 72.4 | 71.0 | 69.7 |
| xxh3-128 | 53.0 | 67.7 | 72.1 | 70.9 | 69.7 |
| blake3 | 3.80 | 3.73 | 3.69 | 3.69 | 3.67 |
| gxhash 2 | 98.5 | 105 | 103 | 104 | 95.8 |
| gxhash 3 | 111 | 118 | 117 | 119 | 103 |
| crc32fast | 58.3 | 58.7 | 58.3 | 58.3 | 58.2 |

#### europa native, one call over a unit, cold: microseconds a call

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 0.154 | 0.580 | 3.11 | 11.8 | 46.3 |
| crc-fast crc32c | 0.092 | 0.386 | 1.55 | 6.08 | 24.0 |
| crc-fast crc64nvme | 0.097 | 0.380 | 1.60 | 6.11 | 24.0 |
| crc64fast-nvme | 0.216 | 0.853 | 3.41 | 13.6 | 54.6 |
| xxh3-64 | 0.097 | 0.379 | 1.58 | 6.14 | 24.0 |
| xxh3-128 | 0.097 | 0.378 | 1.51 | 6.05 | 24.1 |
| blake3 | 1.17 | 2.70 | 10.4 | 41.7 | 166 |
| gxhash 2 | 0.094 | 0.376 | 1.50 | 5.99 | 24.0 |
| gxhash 3 | 0.094 | 0.377 | 1.51 | 6.01 | 24.0 |
| crc32fast | 0.095 | 0.378 | 1.52 | 6.04 | 24.1 |

#### europa native, one combine: nanoseconds, by the second part's length

| Candidate | 4 KiB | 64 KiB | 1 MiB |
| --- | --- | --- | --- |
| crc32c | 7577 | 10100 | 12641 |
| crc-fast crc32c | 7325 | 9709 | 12119 |
| crc-fast crc64nvme | 26118 | 35449 | 45310 |
| crc32fast | 28.5 | 29.2 | 29.8 |
| harness crc32c | 38.7 | 40.7 | 41.8 |
| harness crc32c, fixed length | 23.4 | 23.6 | 24.2 |
| harness crc64nvme | 88.8 | 91.5 | 91.1 |
| harness crc64nvme, fixed length | 48.0 | 48.9 | 48.7 |

#### europa native +gxhash3-hybrid, one call over a unit, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 24.8 | 26.3 | 19.7 | 20.7 | 21.0 |
| crc-fast crc32c | 42.5 | 40.3 | 39.4 | 39.3 | 40.6 |
| crc-fast crc64nvme | 40.2 | 40.4 | 38.4 | 39.1 | 40.8 |
| crc64fast-nvme | 17.6 | 17.8 | 17.9 | 17.9 | 17.9 |
| xxh3-64 | 40.5 | 40.3 | 38.9 | 38.9 | 40.7 |
| xxh3-128 | 40.3 | 40.4 | 40.5 | 39.5 | 40.4 |
| blake3 | 3.24 | 5.64 | 5.82 | 5.80 | 5.85 |
| gxhash 2 | 40.7 | 40.6 | 39.7 | 39.7 | 40.7 |
| gxhash 3 | 40.6 | 40.5 | 39.6 | 39.5 | 40.8 |
| crc32fast | 38.8 | 40.4 | 39.2 | 40.4 | 40.6 |

#### europa native +gxhash3-hybrid, one call over a unit, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 28.3 | 30.0 | 34.3 | 36.9 | 37.1 |
| crc-fast crc32c | 57.2 | 71.1 | 75.3 | 76.5 | 76.9 |
| crc-fast crc64nvme | 62.6 | 72.8 | 75.3 | 76.2 | 76.4 |
| crc64fast-nvme | 18.6 | 19.5 | 19.6 | 19.6 | 19.6 |
| xxh3-64 | 77.2 | 88.0 | 90.2 | 91.6 | 89.0 |
| xxh3-128 | 78.4 | 87.1 | 88.0 | 88.7 | 86.5 |
| blake3 | 3.90 | 8.22 | 8.62 | 8.70 | 8.73 |
| gxhash 2 | 100 | 112 | 112 | 114 | 103 |
| gxhash 3 | 161 | 157 | 148 | 150 | 126 |
| crc32fast | 57.5 | 70.5 | 75.3 | 76.5 | 76.8 |

#### europa native +gxhash3-hybrid, the unit fed in pieces of 4 KiB, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 24.5 | 24.8 | 24.8 | 24.9 | 24.9 |
| crc-fast crc32c | 42.2 | 39.7 | 39.9 | 38.9 | 40.1 |
| crc-fast crc64nvme | 39.8 | 40.0 | 39.8 | 39.1 | 40.4 |
| crc64fast-nvme | 17.7 | 17.8 | 17.8 | 17.8 | 17.8 |
| xxh3-64 | 40.1 | 39.8 | 40.3 | 39.3 | 40.4 |
| xxh3-128 | 39.9 | 40.0 | 40.2 | 39.3 | 40.4 |
| blake3 | 3.17 | 3.11 | 3.09 | 3.06 | 3.05 |
| gxhash 2 | 40.4 | 40.5 | 39.5 | 39.5 | 40.6 |
| gxhash 3 | 40.5 | 40.5 | 39.5 | 39.5 | 40.7 |
| crc32fast | 40.1 | 40.1 | 39.3 | 39.2 | 40.2 |

#### europa native +gxhash3-hybrid, the unit fed in pieces of 4 KiB, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 28.0 | 28.3 | 28.1 | 28.2 | 27.8 |
| crc-fast crc32c | 54.1 | 57.9 | 58.4 | 58.7 | 58.4 |
| crc-fast crc64nvme | 55.1 | 61.7 | 63.0 | 63.5 | 63.3 |
| crc64fast-nvme | 18.6 | 19.2 | 19.2 | 19.2 | 19.2 |
| xxh3-64 | 58.0 | 69.3 | 71.8 | 73.1 | 70.3 |
| xxh3-128 | 59.1 | 69.5 | 71.2 | 73.0 | 70.4 |
| blake3 | 3.79 | 3.72 | 3.69 | 3.68 | 3.66 |
| gxhash 2 | 98.3 | 105 | 102 | 103 | 96.9 |
| gxhash 3 | 158 | 173 | 147 | 149 | 124 |
| crc32fast | 58.4 | 58.2 | 58.2 | 58.2 | 58.0 |

#### europa native +gxhash3-hybrid, one call over a unit, cold: microseconds a call

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 0.154 | 0.581 | 3.09 | 11.8 | 46.5 |
| crc-fast crc32c | 0.090 | 0.378 | 1.55 | 6.21 | 24.0 |
| crc-fast crc64nvme | 0.095 | 0.378 | 1.59 | 6.24 | 23.9 |
| crc64fast-nvme | 0.216 | 0.857 | 3.41 | 13.6 | 54.6 |
| xxh3-64 | 0.094 | 0.378 | 1.57 | 6.28 | 24.0 |
| xxh3-128 | 0.095 | 0.378 | 1.51 | 6.17 | 24.2 |
| blake3 | 1.18 | 2.71 | 10.5 | 42.1 | 167 |
| gxhash 2 | 0.094 | 0.376 | 1.54 | 6.15 | 24.0 |
| gxhash 3 | 0.094 | 0.376 | 1.54 | 6.18 | 24.0 |
| crc32fast | 0.098 | 0.378 | 1.56 | 6.05 | 24.1 |

#### europa native +gxhash3-hybrid, one combine: nanoseconds, by the second part's length

| Candidate | 4 KiB | 64 KiB | 1 MiB |
| --- | --- | --- | --- |
| crc32c | 7374 | 9775 | 12211 |
| crc-fast crc32c | 7550 | 10047 | 12516 |
| crc-fast crc64nvme | 27037 | 36761 | 47214 |
| crc32fast | 28.6 | 29.2 | 29.9 |
| harness crc32c | 38.8 | 40.4 | 41.3 |
| harness crc32c, fixed length | 23.5 | 23.7 | 24.3 |
| harness crc64nvme | 87.9 | 92.2 | 90.7 |
| harness crc64nvme, fixed length | 48.2 | 49.3 | 49.0 |

