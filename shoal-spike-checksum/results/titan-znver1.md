## Runs

| Run | CPU | Build | Compiled for | Detected | Governor | Core | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| titan znver1 | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | `znver1` | sse4.2, pclmulqdq, aes, avx2 | sse4.2, pclmulqdq, aes, avx2 | performance | 2 | 3 × 200 ms | 2026-10-03T21:28:20Z |

rustc 1.100.0-nightly (0ed41eb41 2026-09-04)

## Facts

| Candidate | Bits | Kernel, by run | Threads started | Allocations, one call at 64 KiB | Allocations, fed in 4 KiB pieces |
| --- | --- | --- | --- | --- | --- |
| crc32c | 32 | not said | 0 | 0 / 0 B | 0 / 0 B |
| crc-fast crc32c | 32 | titan znver1: `x86-sse-pclmulqdq` | 0 | 0 / 0 B | 0 / 0 B |
| crc-fast crc64nvme | 64 | titan znver1: `x86-sse-pclmulqdq` | 0 | 0 / 0 B | 0 / 0 B |
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
| crc32c | "123456789" | `e3069283` | CRC-32/ISCSI check, RevEng catalogue (crc-catalog `algorithm.rs:2270`) | all 1 |
| crc32c | "Hello world!" | `7b98e751` | crc32c's own doc test (`lib.rs:8-12`) | all 1 |
| crc-fast crc32c | "123456789" | `e3069283` | CRC-32/ISCSI check, RevEng catalogue (crc-catalog `algorithm.rs:2270`) | all 1 |
| crc-fast crc32c | "Hello world!" | `7b98e751` | crc32c's own doc test (`lib.rs:8-12`) | all 1 |
| crc-fast crc64nvme | "123456789" | `ae8b14860a799888` | CRC-64/NVME check, RevEng catalogue (crc-catalog `algorithm.rs:2500`) | all 1 |
| crc-fast crc64nvme | 4,096 zero bytes | `6482d367eb22b64e` | crc64fast-nvme's tests (`lib.rs:202`) | all 1 |
| crc64fast-nvme | "123456789" | `ae8b14860a799888` | CRC-64/NVME check, RevEng catalogue (crc-catalog `algorithm.rs:2500`) | all 1 |
| crc64fast-nvme | 4,096 zero bytes | `6482d367eb22b64e` | crc64fast-nvme's tests (`lib.rs:202`) | all 1 |
| crc32fast | "123456789" | `cbf43926` | CRC-32/ISO-HDLC check, RevEng catalogue | all 1 |
| xxh3-64 | empty | `2d06800538d394c2` | twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) | all 1 |
| xxh3-64 | 1,024 bytes of the 251 pattern | `e5d78bafa45b2aa5` | twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) | all 1 |
| xxh3-64 | 10,240 bytes of the 251 pattern | `bcd63266df6e2244` | twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) | all 1 |
| xxh3-128 | empty | `99aa06d3014798d86001c324468d497f` | twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) | all 1 |
| xxh3-128 | 1,024 bytes of the 251 pattern | `d0ac1f7b93bf57b9e5d78bafa45b2aa5` | twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) | all 1 |
| xxh3-128 | 10,240 bytes of the 251 pattern | `4f6375cca7ece1e1bcd63266df6e2244` | twox-hash 2.1.4's tests (`xxhash3_64.rs:382`, `:567-572`; `xxhash3_128.rs:452`, `:641-642`) | all 1 |
| blake3 | empty | `af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262` | BLAKE3 `test_vectors/test_vectors.json` at tag 1.8.7, not shipped in the crate | all 1 |
| blake3 | 1 byte of the 251 pattern | `2d3adedff11b61f14c886e35afa036736dcd87a74d27b5c1510225d0f592e213` | BLAKE3 `test_vectors/test_vectors.json` at tag 1.8.7, not shipped in the crate | all 1 |
| blake3 | 1,024 bytes of the 251 pattern | `42214739f095a406f3fc83deb889744ac00df831c10daa55189b5d121c855af7` | BLAKE3 `test_vectors/test_vectors.json` at tag 1.8.7, not shipped in the crate | all 1 |
| blake3 | 1,025 bytes of the 251 pattern | `d00278ae47eb27b34faecf67b4fe263f82d5412916c1ffd97c8cb7fb814b8444` | BLAKE3 `test_vectors/test_vectors.json` at tag 1.8.7, not shipped in the crate | all 1 |
| blake3 | 102,400 bytes of the 251 pattern | `bc3e3d41a1146b069abffad3c0d44860cf664390afce4d9661f7902e7943e085` | BLAKE3 `test_vectors/test_vectors.json` at tag 1.8.7, not shipped in the crate | all 1 |

### The same input across runs

Every set's output compared across all 1 runs.

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

