## Runs

| Run | CPU | Build | Compiled for | Detected | Governor | Core | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| titan x86-64-v3 +aes | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | `x86-64-v3 +aes` | sse4.2, aes, avx2 | sse4.2, pclmulqdq, aes, avx2 | performance | 2 | 3 × 200 ms | 2026-10-03T21:34:12Z |

rustc 1.100.0-nightly (0ed41eb41 2026-09-04)

## Facts

| Candidate | Bits | Kernel, by run | Threads started | Allocations, one call at 64 KiB | Allocations, fed in 4 KiB pieces |
| --- | --- | --- | --- | --- | --- |

## Speed, run by run

#### titan x86-64-v3 +aes, one call over a unit, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 5.14 | 6.21 | 6.53 | 6.62 | 6.66 |
| crc-fast crc32c | 4.75 | 8.10 | 9.81 | 10.5 | 10.8 |
| crc-fast crc64nvme | 11.3 | 11.2 | 11.2 | 11.0 | 11.5 |
| crc64fast-nvme | 11.5 | 11.5 | 11.5 | 11.5 | 11.6 |
| xxh3-64 | 14.0 | 14.1 | 14.1 | 14.1 | 14.1 |
| xxh3-128 | 14.0 | 14.1 | 14.0 | 14.1 | 14.1 |
| blake3 | 1.25 | 1.76 | 1.78 | 1.78 | 1.78 |
| gxhash 2 | 17.8 | 18.5 | 19.0 | 19.1 | 19.2 |
| gxhash 3 | 17.8 | 18.4 | 18.7 | 18.8 | 18.9 |
| crc32fast | 11.6 | 11.6 | 11.6 | 11.5 | 11.6 |

#### titan x86-64-v3 +aes, one call over a unit, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 6.78 | 6.90 | 7.11 | 7.45 | 7.46 |
| crc-fast crc32c | 8.60 | 10.1 | 10.9 | 11.1 | 11.2 |
| crc-fast crc64nvme | 12.2 | 12.8 | 13.1 | 13.2 | 13.2 |
| crc64fast-nvme | 12.3 | 12.9 | 13.2 | 13.2 | 13.2 |
| xxh3-64 | 21.0 | 21.9 | 20.9 | 21.0 | 21.0 |
| xxh3-128 | 20.6 | 21.4 | 20.8 | 20.9 | 20.8 |
| blake3 | 1.36 | 1.97 | 1.98 | 1.99 | 1.98 |
| gxhash 2 | 45.7 | 53.2 | 51.2 | 51.2 | 50.3 |
| gxhash 3 | 54.6 | 59.1 | 49.5 | 48.9 | 48.4 |
| crc32fast | 12.6 | 13.0 | 13.2 | 13.2 | 13.2 |

#### titan x86-64-v3 +aes, the unit fed in pieces of 4 KiB, cold (units in turn, out of cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 5.21 | 5.86 | 5.93 | 5.92 | 5.93 |
| crc-fast crc32c | 4.43 | 4.88 | 5.12 | 4.91 | 4.95 |
| crc-fast crc64nvme | 10.8 | 11.3 | 11.5 | 11.6 | 11.6 |
| crc64fast-nvme | 11.5 | 11.5 | 11.6 | 11.6 | 11.6 |
| xxh3-64 | 11.9 | 12.4 | 12.6 | 12.6 | 12.7 |
| xxh3-128 | 11.7 | 12.3 | 12.6 | 12.7 | 12.7 |
| blake3 | 1.23 | 1.20 | 1.21 | 1.21 | 1.21 |
| gxhash 2 | 17.9 | 18.2 | 18.4 | 18.5 | 18.5 |
| gxhash 3 | 17.7 | 17.7 | 17.8 | 17.7 | 17.8 |
| crc32fast | 11.4 | 11.5 | 11.5 | 11.5 | 11.5 |

#### titan x86-64-v3 +aes, the unit fed in pieces of 4 KiB, hot (one unit, in cache): GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 6.74 | 6.72 | 6.73 | 6.71 | 6.70 |
| crc-fast crc32c | 8.15 | 8.48 | 8.52 | 8.53 | 8.36 |
| crc-fast crc64nvme | 11.5 | 12.1 | 12.4 | 12.4 | 12.4 |
| crc64fast-nvme | 12.2 | 12.4 | 12.6 | 12.6 | 12.5 |
| xxh3-64 | 17.2 | 18.3 | 18.2 | 18.4 | 18.2 |
| xxh3-128 | 16.9 | 18.2 | 18.2 | 18.4 | 18.2 |
| blake3 | 1.34 | 1.32 | 1.32 | 1.31 | 1.31 |
| gxhash 2 | 42.1 | 47.5 | 46.3 | 46.3 | 45.5 |
| gxhash 3 | 53.3 | 56.7 | 48.4 | 48.6 | 47.7 |
| crc32fast | 12.1 | 12.6 | 12.8 | 12.8 | 12.8 |

#### titan x86-64-v3 +aes, one call over a unit, cold: microseconds a call

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| crc32c | 0.742 | 2.46 | 9.35 | 36.9 | 147 |
| crc-fast crc32c | 0.803 | 1.88 | 6.22 | 23.1 | 90.7 |
| crc-fast crc64nvme | 0.337 | 1.36 | 5.47 | 22.3 | 84.9 |
| crc64fast-nvme | 0.330 | 1.33 | 5.29 | 21.2 | 84.4 |
| xxh3-64 | 0.272 | 1.08 | 4.34 | 17.3 | 69.3 |
| xxh3-128 | 0.272 | 1.09 | 4.34 | 17.4 | 69.3 |
| blake3 | 3.05 | 8.67 | 34.4 | 137 | 548 |
| gxhash 2 | 0.214 | 0.825 | 3.21 | 12.8 | 50.8 |
| gxhash 3 | 0.215 | 0.829 | 3.26 | 13.0 | 51.6 |
| crc32fast | 0.329 | 1.32 | 5.28 | 21.2 | 84.4 |

#### titan x86-64-v3 +aes, one combine: nanoseconds, by the second part's length

| Candidate | 4 KiB | 64 KiB | 1 MiB |
| --- | --- | --- | --- |
| crc32c | 24931 | 36259 | 48537 |
| crc-fast crc32c | 25744 | 38034 | 51290 |
| crc-fast crc64nvme | 124922 | 184212 | 244456 |
| crc32fast | 82.5 | 89.7 | 92.6 |
| harness crc32c | 71.5 | 76.0 | 77.2 |
| harness crc32c, fixed length | 37.2 | 37.6 | 38.3 |
| harness crc64nvme | 147 | 151 | 153 |
| harness crc64nvme, fixed length | 74.3 | 75.7 | 76.5 |

