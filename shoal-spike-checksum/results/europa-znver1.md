## Runs

| Run | CPU | Build | Compiled for | Detected | Governor | Core | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| europa znver1 | AMD Ryzen 9 7945HX with Radeon Graphics | `znver1` | sse4.2, pclmulqdq, aes, avx2 | sse4.2, pclmulqdq, aes, avx2, avx512f, avx512vl, vpclmulqdq, vaes | performance | 8 | 3 × 200 ms | 2026-10-03T21:30:40Z |

rustc 1.100.0-nightly (0ed41eb41 2026-09-04)

## Facts

| Candidate | Bits | Kernel, by run | Threads started | Allocations, one call at 64 KiB | Allocations, fed in 4 KiB pieces |
| --- | --- | --- | --- | --- | --- |
| crc32c | 32 | not said | 0 | 0 / 0 B | 0 / 0 B |
| crc-fast crc32c | 32 | europa znver1: `x86_64-avx512-vpclmulqdq` | 0 | 0 / 0 B | 0 / 0 B |
| crc-fast crc64nvme | 64 | europa znver1: `x86_64-avx512-vpclmulqdq` | 0 | 0 / 0 B | 0 / 0 B |
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

### Outputs, from europa znver1

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

