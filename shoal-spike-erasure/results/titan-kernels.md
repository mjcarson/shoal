# X4 results: titan znver1, kernel sets

Every speed cell of one run of `shoal-spike-erasure`. The figure is the median of 3 measurements, each at least 200 ms of whole calls; encode and decode are counted by the stripe data (k × unit), rebuild by the chunk rebuilt and update by the bytes changed. `n/a` is a cell the candidate cannot run, with the reason below the table.

| Run | Host | CPU | Build | rse C | Governor | Core | Detected | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| titan znver1, kernel sets | titan | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | `target-cpu=znver1` | `-march=haswell` | performance | 2 | ssse3, avx2 | 3 × 200 ms | 2026-10-03T18:25:41Z |

## 4+2, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.56 | 0.56 | 0.56 | 0.56 | 0.57 |
| rusty_erasure [ssse3] | 5.89 | 7.18 | 7.66 | 7.86 | 7.89 |
| rusty_erasure [avx2] | 6.00 | 7.40 | 7.68 | 7.82 | 7.83 |

## 4+2, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.57 | 0.58 | 0.58 | 0.57 | 0.56 |
| rusty_erasure [ssse3] | 9.93 | 9.74 | 9.76 | 9.97 | 8.19 |
| rusty_erasure [avx2] | 10.3 | 9.50 | 9.80 | 9.90 | 8.41 |

## 4+2, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 1.08 | 1.11 | 1.12 | 1.13 | 1.13 |
| rusty_erasure [ssse3] | 7.62 | 10.8 | 12.1 | 12.6 | 12.6 |
| rusty_erasure [avx2] | 7.68 | 10.7 | 12.2 | 12.7 | 12.8 |

## 4+2, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 1.12 | 1.14 | 1.14 | 1.14 | 1.14 |
| rusty_erasure [ssse3] | 12.4 | 15.5 | 16.3 | 16.5 | 14.4 |
| rusty_erasure [avx2] | 13.4 | 15.8 | 16.2 | 16.5 | 15.1 |

## 4+2, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.55 | 0.56 | 0.56 | 0.56 | 0.56 |
| rusty_erasure [ssse3] | 5.86 | 7.12 | 7.72 | 7.87 | 7.88 |
| rusty_erasure [avx2] | 5.78 | 7.14 | 7.63 | 7.77 | 7.83 |

## 4+2, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.56 | 0.57 | 0.57 | 0.57 | 0.56 |
| rusty_erasure [ssse3] | 8.60 | 9.43 | 9.73 | 9.90 | 8.25 |
| rusty_erasure [avx2] | 9.30 | 9.30 | 9.74 | 9.85 | 8.43 |

## 4+2, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.27 | 0.28 | 0.28 | 0.28 | 0.28 |
| rusty_erasure [ssse3] | 1.91 | 2.71 | 3.04 | 3.15 | 3.15 |
| rusty_erasure [avx2] | 1.92 | 2.68 | 3.05 | 3.17 | 3.19 |

## 4+2, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.28 | 0.28 | 0.29 | 0.29 | 0.29 |
| rusty_erasure [ssse3] | 3.11 | 3.87 | 4.08 | 4.13 | 3.61 |
| rusty_erasure [avx2] | 3.34 | 3.94 | 4.06 | 4.13 | 3.76 |

## 4+2, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.18 | 0.19 | 0.19 | 0.18 | 0.18 |
| rusty_erasure [ssse3] | 2.49 | 3.01 | 3.16 | 3.16 | 3.00 |
| rusty_erasure [avx2] | 2.52 | 2.98 | 3.24 | 3.18 | 3.01 |

## 4+2, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.33 | 0.33 | 0.33 | 0.33 | 0.18 |
| rusty_erasure [ssse3] | 5.38 | 6.09 | 6.46 | 6.50 | 3.61 |
| rusty_erasure [avx2] | 5.48 | 6.23 | 6.65 | 6.66 | 3.64 |

## 10+4, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.29 | 0.29 | 0.29 | 0.29 | 0.29 |
| rusty_erasure [ssse3] | 5.10 | 5.20 | 5.20 | 5.39 | 5.33 |
| rusty_erasure [avx2] | 5.23 | 5.27 | 5.39 | 5.40 | 5.52 |

## 10+4, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.29 | 0.29 | 0.29 | 0.29 | 0.29 |
| rusty_erasure [ssse3] | 5.47 | 5.79 | 5.74 | 5.74 | 5.33 |
| rusty_erasure [avx2] | 5.95 | 5.95 | 5.95 | 5.89 | 5.52 |

## 10+4, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 1.12 | 1.14 | 1.14 | 1.14 | 1.14 |
| rusty_erasure [ssse3] | 9.35 | 10.7 | 10.8 | 11.0 | 11.0 |
| rusty_erasure [avx2] | 9.47 | 11.6 | 11.8 | 11.9 | 12.0 |

## 10+4, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 1.13 | 1.14 | 1.15 | 1.15 | 1.15 |
| rusty_erasure [ssse3] | 11.7 | 13.0 | 13.2 | 13.3 | 11.2 |
| rusty_erasure [avx2] | 14.0 | 15.4 | 15.7 | 15.8 | 12.0 |

## 10+4, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.56 | 0.57 | 0.57 | 0.57 | 0.57 |
| rusty_erasure [ssse3] | 6.92 | 8.23 | 8.48 | 8.59 | 8.60 |
| rusty_erasure [avx2] | 7.19 | 8.74 | 9.05 | 9.16 | 9.18 |

## 10+4, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.57 | 0.57 | 0.57 | 0.57 | 0.57 |
| rusty_erasure [ssse3] | 8.80 | 9.31 | 9.43 | 9.45 | 8.61 |
| rusty_erasure [avx2] | 9.74 | 10.3 | 10.6 | 10.6 | 9.24 |

## 10+4, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.38 | 0.38 | 0.38 | 0.38 | 0.38 |
| rusty_erasure [ssse3] | 5.82 | 6.61 | 6.76 | 6.83 | 6.84 |
| rusty_erasure [avx2] | 5.99 | 6.68 | 6.82 | 6.92 | 6.93 |

## 10+4, decode-3, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.38 | 0.38 | 0.38 | 0.38 | 0.38 |
| rusty_erasure [ssse3] | 7.03 | 7.29 | 7.36 | 7.36 | 6.85 |
| rusty_erasure [avx2] | 7.29 | 7.66 | 7.76 | 7.74 | 6.95 |

## 10+4, decode-4, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.28 | 0.29 | 0.29 | 0.29 | 0.29 |
| rusty_erasure [ssse3] | 4.82 | 5.14 | 5.28 | 5.30 | 5.42 |
| rusty_erasure [avx2] | 4.85 | 5.26 | 5.33 | 5.44 | 5.42 |

## 10+4, decode-4, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.29 | 0.29 | 0.29 | 0.29 | 0.29 |
| rusty_erasure [ssse3] | 5.29 | 5.72 | 5.74 | 5.70 | 5.39 |
| rusty_erasure [avx2] | 5.61 | 5.90 | 5.93 | 5.92 | 5.43 |

## 10+4, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.11 | 0.11 | 0.11 | 0.11 | 0.11 |
| rusty_erasure [ssse3] | 0.94 | 1.07 | 1.09 | 1.10 | 1.10 |
| rusty_erasure [avx2] | 0.95 | 1.16 | 1.18 | 1.19 | 1.20 |

## 10+4, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.11 | 0.11 | 0.11 | 0.12 | 0.11 |
| rusty_erasure [ssse3] | 1.18 | 1.31 | 1.32 | 1.33 | 1.12 |
| rusty_erasure [avx2] | 1.40 | 1.55 | 1.57 | 1.58 | 1.20 |

## 10+4, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.09 | 0.09 | 0.09 | 0.09 | 0.09 |
| rusty_erasure [ssse3] | 1.54 | 1.85 | 1.90 | 1.89 | 1.68 |
| rusty_erasure [avx2] | 1.61 | 1.85 | 1.91 | 1.86 | 1.71 |

## 10+4, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.18 | 0.18 | 0.10 | 0.10 | 0.09 |
| rusty_erasure [ssse3] | 3.48 | 4.04 | 4.17 | 4.22 | 1.74 |
| rusty_erasure [avx2] | 3.58 | 4.11 | 4.23 | 4.27 | 1.77 |

## Cells not measured

