# X4 results: europa native, kernel sets

Every speed cell of one run of `shoal-spike-erasure`. The figure is the median of 3 measurements, each at least 200 ms of whole calls; encode and decode are counted by the stripe data (k × unit), rebuild by the chunk rebuilt and update by the bytes changed. `n/a` is a cell the candidate cannot run, with the reason below the table.

| Run | Host | CPU | Build | rse C | Governor | Core | Detected | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| europa native, kernel sets | europa | AMD Ryzen 9 7945HX with Radeon Graphics | `target-cpu=native` | `-march=native` | performance | 8 | ssse3, avx2, avx512f, avx512bw, avx512vl, gfni | 3 × 200 ms | 2026-10-03T18:54:59Z |

## 4+2, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.96 | 0.96 | 0.96 | 0.95 | 0.96 |
| rusty_erasure [ssse3] | 12.2 | 14.9 | 17.2 | 17.9 | 18.8 |
| rusty_erasure [avx2] | 14.5 | 16.5 | 19.3 | 20.1 | 20.4 |
| rusty_erasure [gfni] | 15.4 | 17.6 | 20.4 | 21.3 | 21.5 |

## 4+2, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.97 | 0.96 | 0.97 | 0.97 | 0.97 |
| rusty_erasure [ssse3] | 23.1 | 23.9 | 25.3 | 25.0 | 25.1 |
| rusty_erasure [avx2] | 42.5 | 44.2 | 50.1 | 41.9 | 37.6 |
| rusty_erasure [gfni] | 88.1 | 87.1 | 97.2 | 75.5 | 74.4 |

## 4+2, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 1.86 | 1.88 | 1.90 | 1.91 | 1.91 |
| rusty_erasure [ssse3] | 14.2 | 18.2 | 23.7 | 25.5 | 26.0 |
| rusty_erasure [avx2] | 16.2 | 20.0 | 25.5 | 27.1 | 27.7 |
| rusty_erasure [gfni] | 18.2 | 21.7 | 26.7 | 27.7 | 28.6 |

## 4+2, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 1.92 | 1.93 | 1.93 | 1.93 | 1.93 |
| rusty_erasure [ssse3] | 30.5 | 36.5 | 38.5 | 37.3 | 37.4 |
| rusty_erasure [avx2] | 50.0 | 64.7 | 70.6 | 65.8 | 69.6 |
| rusty_erasure [gfni] | 80.5 | 94.5 | 112 | 93.2 | 92.8 |

## 4+2, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.94 | 0.95 | 0.95 | 0.95 | 0.96 |
| rusty_erasure [ssse3] | 11.5 | 14.3 | 16.9 | 18.2 | 18.3 |
| rusty_erasure [avx2] | 13.2 | 15.8 | 19.0 | 20.0 | 20.2 |
| rusty_erasure [gfni] | 14.2 | 16.8 | 20.0 | 21.1 | 21.4 |

## 4+2, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.97 | 0.97 | 0.97 | 0.96 | 0.96 |
| rusty_erasure [ssse3] | 20.7 | 23.7 | 24.3 | 23.8 | 24.1 |
| rusty_erasure [avx2] | 35.9 | 43.5 | 45.9 | 39.0 | 34.5 |
| rusty_erasure [gfni] | 67.8 | 81.0 | 94.4 | 76.4 | 79.4 |

## 4+2, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.47 | 0.47 | 0.48 | 0.48 | 0.48 |
| rusty_erasure [ssse3] | 3.55 | 4.54 | 5.93 | 6.35 | 6.50 |
| rusty_erasure [avx2] | 4.05 | 4.96 | 6.37 | 6.74 | 6.89 |
| rusty_erasure [gfni] | 4.57 | 5.43 | 6.63 | 7.02 | 7.16 |

## 4+2, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.48 | 0.48 | 0.48 | 0.48 | 0.48 |
| rusty_erasure [ssse3] | 7.80 | 9.12 | 9.61 | 9.31 | 9.34 |
| rusty_erasure [avx2] | 12.7 | 16.2 | 17.6 | 16.4 | 17.4 |
| rusty_erasure [gfni] | 20.7 | 23.7 | 28.0 | 23.2 | 23.2 |

## 4+2, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.69 | 0.72 | 0.73 | 0.73 | 0.73 |
| rusty_erasure [ssse3] | 3.75 | 4.73 | 5.76 | 6.51 | 7.00 |
| rusty_erasure [avx2] | 3.99 | 5.03 | 5.66 | 6.50 | 7.21 |
| rusty_erasure [gfni] | 4.36 | 5.37 | 5.99 | 6.90 | 7.49 |

## 4+2, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.78 | 0.78 | 0.76 | 0.75 | 0.75 |
| rusty_erasure [ssse3] | 16.2 | 16.3 | 17.1 | 16.7 | 16.6 |
| rusty_erasure [avx2] | 22.7 | 20.9 | 16.9 | 17.0 | 20.3 |
| rusty_erasure [gfni] | 25.0 | 22.3 | 18.5 | 20.5 | 21.1 |

## 10+4, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.48 | 0.48 | 0.48 | 0.48 | 0.48 |
| rusty_erasure [ssse3] | 9.78 | 9.67 | 9.81 | 9.80 | 9.06 |
| rusty_erasure [avx2] | 14.5 | 14.9 | 14.1 | 14.5 | 12.8 |
| rusty_erasure [gfni] | 15.0 | 16.3 | 16.9 | 17.1 | 17.4 |

## 10+4, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.48 | 0.49 | 0.49 | 0.49 | 0.49 |
| rusty_erasure [ssse3] | 10.6 | 11.4 | 10.8 | 10.8 | 10.8 |
| rusty_erasure [avx2] | 23.0 | 24.3 | 23.4 | 23.5 | 22.7 |
| rusty_erasure [gfni] | 31.6 | 35.3 | 35.1 | 34.8 | 34.3 |

## 10+4, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 1.91 | 1.91 | 1.92 | 1.92 | 1.93 |
| rusty_erasure [ssse3] | 16.5 | 20.5 | 22.0 | 22.5 | 23.1 |
| rusty_erasure [avx2] | 20.5 | 25.9 | 28.3 | 29.0 | 29.2 |
| rusty_erasure [gfni] | 21.4 | 26.8 | 32.0 | 33.7 | 34.5 |

## 10+4, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 1.94 | 1.95 | 1.95 | 1.94 | 1.94 |
| rusty_erasure [ssse3] | 25.0 | 26.0 | 26.2 | 26.2 | 26.3 |
| rusty_erasure [avx2] | 47.4 | 50.0 | 51.6 | 51.2 | 51.8 |
| rusty_erasure [gfni] | 73.8 | 91.3 | 92.2 | 94.9 | 95.6 |

## 10+4, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.96 | 0.96 | 0.96 | 0.96 | 0.96 |
| rusty_erasure [ssse3] | 12.9 | 15.1 | 16.1 | 16.5 | 16.7 |
| rusty_erasure [avx2] | 18.2 | 21.1 | 23.0 | 23.7 | 24.2 |
| rusty_erasure [gfni] | 19.3 | 22.5 | 24.8 | 25.6 | 25.8 |

## 10+4, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.97 | 0.97 | 0.97 | 0.97 | 0.97 |
| rusty_erasure [ssse3] | 17.0 | 18.0 | 18.0 | 17.9 | 18.0 |
| rusty_erasure [avx2] | 33.8 | 36.5 | 36.6 | 35.9 | 36.0 |
| rusty_erasure [gfni] | 52.4 | 55.4 | 56.5 | 56.3 | 56.6 |

## 10+4, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.64 | 0.64 | 0.64 | 0.64 | 0.64 |
| rusty_erasure [ssse3] | 10.5 | 12.2 | 13.1 | 13.5 | 13.9 |
| rusty_erasure [avx2] | 14.9 | 17.6 | 19.1 | 20.1 | 20.8 |
| rusty_erasure [gfni] | 15.9 | 18.5 | 19.8 | 20.7 | 21.2 |

## 10+4, decode-3, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.65 | 0.65 | 0.65 | 0.65 | 0.65 |
| rusty_erasure [ssse3] | 12.6 | 14.4 | 14.6 | 14.7 | 14.7 |
| rusty_erasure [avx2] | 26.7 | 30.2 | 30.5 | 30.3 | 30.3 |
| rusty_erasure [gfni] | 34.8 | 40.9 | 40.2 | 39.8 | 39.7 |

## 10+4, decode-4, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.48 | 0.48 | 0.48 | 0.48 | 0.48 |
| rusty_erasure [ssse3] | 9.57 | 9.16 | 9.38 | 9.30 | 8.69 |
| rusty_erasure [avx2] | 13.2 | 14.7 | 13.3 | 13.9 | 12.2 |
| rusty_erasure [gfni] | 14.1 | 16.0 | 16.5 | 16.7 | 16.8 |

## 10+4, decode-4, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.49 | 0.49 | 0.49 | 0.49 | 0.48 |
| rusty_erasure [ssse3] | 10.6 | 11.2 | 11.0 | 10.9 | 10.6 |
| rusty_erasure [avx2] | 21.5 | 23.8 | 23.5 | 23.2 | 22.7 |
| rusty_erasure [gfni] | 27.9 | 33.2 | 34.3 | 34.0 | 34.1 |

## 10+4, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.19 | 0.19 | 0.19 | 0.19 | 0.19 |
| rusty_erasure [ssse3] | 1.65 | 2.05 | 2.19 | 2.24 | 2.30 |
| rusty_erasure [avx2] | 2.04 | 2.59 | 2.83 | 2.89 | 2.92 |
| rusty_erasure [gfni] | 2.14 | 2.68 | 3.20 | 3.38 | 3.46 |

## 10+4, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.19 | 0.19 | 0.19 | 0.19 | 0.19 |
| rusty_erasure [ssse3] | 2.50 | 2.60 | 2.61 | 2.61 | 2.62 |
| rusty_erasure [avx2] | 4.75 | 5.00 | 5.16 | 5.12 | 5.17 |
| rusty_erasure [gfni] | 7.40 | 9.14 | 9.25 | 9.51 | 9.57 |

## 10+4, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.40 | 0.41 | 0.41 | 0.42 | 0.42 |
| rusty_erasure [ssse3] | 3.18 | 3.75 | 4.19 | 4.66 | 4.91 |
| rusty_erasure [avx2] | 3.30 | 3.84 | 4.38 | 4.93 | 5.13 |
| rusty_erasure [gfni] | 3.43 | 3.97 | 4.51 | 4.99 | 5.25 |

## 10+4, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure [scalar] | 0.44 | 0.44 | 0.43 | 0.43 | 0.42 |
| rusty_erasure [ssse3] | 10.5 | 11.2 | 9.65 | 11.2 | 11.0 |
| rusty_erasure [avx2] | 15.2 | 15.0 | 15.1 | 14.6 | 15.1 |
| rusty_erasure [gfni] | 17.4 | 16.7 | 12.0 | 14.8 | 14.6 |

## Cells not measured

