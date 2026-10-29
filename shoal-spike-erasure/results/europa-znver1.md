# X4 results: europa znver1

Every speed cell of one run of `shoal-spike-erasure`. The figure is the median of 3 measurements, each at least 200 ms of whole calls; encode and decode are counted by the stripe data (k × unit), rebuild by the chunk rebuilt and update by the bytes changed. `n/a` is a cell the candidate cannot run, with the reason below the table.

| Run | Host | CPU | Build | rse C | Governor | Core | Detected | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| europa znver1 | europa | AMD Ryzen 9 7945HX with Radeon Graphics | `target-cpu=znver1` | `-march=haswell` | performance | 8 | ssse3, avx2, avx512f, avx512bw, avx512vl, gfni | 3 × 200 ms | 2026-10-03T18:09:17Z |

## 2+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 13.7 | 18.7 | 17.8 | 18.3 | 18.9 |
| rusty_erasure | 14.2 | 17.7 | 20.1 | 21.1 | 20.8 |
| isa-l | 13.2 | 15.9 | 19.6 | 20.7 | 20.9 |
| reed-solomon-erasure | 22.0 | 14.8 | 15.5 | 16.2 | 17.2 |
| reed-solomon-simd | 11.8 | 10.6 | 11.0 | 12.4 | 15.6 |
| raptorq | 0.29 | 0.36 | 0.31 | 0.27 | 0.31 |
| rlnc | 4.70 | 5.61 | 6.14 | 6.35 | 6.59 |
| rlnc, systematic | 5.24 | 7.14 | 7.59 | 8.06 | 8.12 |

## 2+1, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 4.07 | 5.56 | 6.10 | 7.03 | 7.36 |

## 2+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.7 | 16.4 | 17.3 | 18.2 | 18.6 |
| rusty_erasure | 12.6 | 16.0 | 19.5 | 20.8 | 21.1 |
| isa-l | 12.6 | 15.1 | 19.0 | 20.3 | 20.9 |
| reed-solomon-erasure | 10.8 | 11.6 | 14.3 | 15.5 | 17.2 |
| reed-solomon-simd | 0.07 | 0.26 | 0.90 | 2.32 | 3.62 |
| raptorq | 0.22 | 0.33 | 0.29 | 0.25 | 0.30 |
| rlnc | 4.06 | 5.61 | 6.11 | 7.08 | 7.41 |
| rlnc, systematic | 1.95 | 2.21 | 2.28 | 2.32 | 2.36 |

## 2+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 6.01 | 8.46 | 8.61 | 9.05 | 9.40 |
| rusty_erasure | 6.14 | 8.01 | 9.75 | 10.4 | 10.5 |
| isa-l | 5.87 | 7.54 | 9.50 | 10.3 | 10.4 |
| reed-solomon-erasure | 5.03 | 5.69 | 7.11 | 7.83 | 8.61 |
| reed-solomon-simd | 0.03 | 0.13 | 0.45 | 1.16 | 1.81 |
| raptorq | 0.11 | 0.16 | 0.15 | 0.13 | 0.15 |
| rlnc | 2.92 | 4.05 | 4.83 | 5.64 | 5.70 |
| rlnc, systematic | 0.98 | 1.11 | 1.14 | 1.16 | 1.19 |

## 2+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 8.78 | 10.8 | 13.4 | 13.0 | 11.0 |
| rusty_erasure | 7.70 | 7.46 | 7.76 | 8.62 | 10.9 |
| isa-l | 7.82 | 7.30 | 7.61 | 8.36 | 10.7 |
| reed-solomon-erasure | 7.89 | 6.98 | 7.28 | 7.94 | 10.4 |

## 4+2, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.3 | 17.2 | 20.2 | 21.0 | 21.2 |
| isa-l | 14.5 | 16.9 | 19.6 | 20.3 | 20.6 |
| reed-solomon-erasure | 13.8 | 10.9 | 11.8 | 11.4 | 13.9 |
| reed-solomon-simd | 7.74 | 8.59 | 9.81 | 11.6 | 11.7 |
| raptorq | 0.55 | 0.68 | 0.51 | 0.59 | 0.51 |
| rlnc | 3.72 | 4.91 | 4.76 | 4.87 | 4.88 |
| rlnc, systematic | 5.68 | 7.23 | 7.37 | 7.62 | 7.46 |

## 4+2, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 80.6 | 87.2 | 97.0 | 75.2 | 75.4 |
| isa-l | 31.1 | 34.0 | 35.0 | 34.0 | 32.7 |
| reed-solomon-erasure | 29.7 | 30.5 | 33.4 | 29.3 | 30.7 |
| reed-solomon-simd | 25.6 | 20.9 | 21.3 | 20.9 | 19.1 |
| raptorq | 0.58 | 0.64 | 0.53 | 0.60 | 0.53 |
| rlnc | 7.71 | 9.37 | 8.06 | 7.13 | 7.27 |
| rlnc, systematic | 11.3 | 12.6 | 10.8 | 9.65 | 9.88 |

## 4+2, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 4.09 | 4.83 | 5.29 | 5.92 | 5.79 |

## 4+2, decode-0, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 8.80 | 9.37 | 8.93 | 8.35 | 8.13 |

## 4+2, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 17.8 | 21.7 | 26.4 | 28.0 | 28.4 |
| isa-l | 17.0 | 21.0 | 25.9 | 27.4 | 27.9 |
| reed-solomon-erasure | 9.66 | 14.1 | 15.7 | 17.6 | 20.8 |
| reed-solomon-simd | 0.13 | 0.49 | 1.42 | 2.56 | 3.70 |
| raptorq | 0.42 | 0.55 | 0.48 | 0.55 | 0.49 |
| rlnc | 4.06 | 4.94 | 5.28 | 5.95 | 5.81 |
| rlnc, systematic | 3.17 | 3.63 | 3.72 | 3.78 | 3.81 |

## 4+2, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 86.3 | 94.0 | 112 | 92.1 | 96.6 |
| isa-l | 45.5 | 56.4 | 61.3 | 61.3 | 62.2 |
| reed-solomon-erasure | 36.4 | 51.0 | 62.8 | 56.2 | 60.7 |
| reed-solomon-simd | 0.14 | 0.50 | 1.54 | 2.80 | 4.13 |
| raptorq | 0.44 | 0.57 | 0.50 | 0.57 | 0.50 |
| rlnc | 8.79 | 9.59 | 8.91 | 8.36 | 8.13 |
| rlnc, systematic | 4.42 | 4.56 | 4.42 | 4.29 | 4.22 |

## 4+2, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 14.4 | 16.6 | 20.1 | 21.1 | 21.4 |
| isa-l | 13.5 | 15.9 | 19.1 | 20.2 | 20.5 |
| reed-solomon-erasure | 7.74 | 10.0 | 11.3 | 11.3 | 14.0 |
| reed-solomon-simd | 0.13 | 0.48 | 1.41 | 2.51 | 3.60 |
| raptorq | 0.43 | 0.62 | 0.49 | 0.55 | 0.49 |
| rlnc | 4.10 | 4.95 | 5.31 | 5.95 | 5.81 |
| rlnc, systematic | 2.69 | 3.07 | 3.18 | 3.28 | 3.25 |

## 4+2, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 64.6 | 80.1 | 93.7 | 76.8 | 80.2 |
| isa-l | 28.5 | 33.2 | 34.7 | 33.9 | 33.0 |
| reed-solomon-erasure | 24.2 | 28.3 | 32.3 | 28.9 | 30.1 |
| reed-solomon-simd | 0.14 | 0.50 | 1.54 | 2.79 | 4.09 |
| raptorq | 0.44 | 0.58 | 0.50 | 0.57 | 0.51 |
| rlnc | 8.75 | 9.38 | 8.93 | 8.36 | 8.46 |
| rlnc, systematic | 3.83 | 3.86 | 3.77 | 3.70 | 3.60 |

## 4+2, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.39 | 5.42 | 6.61 | 7.00 | 7.16 |
| isa-l | 4.10 | 5.24 | 6.46 | 6.81 | 6.97 |
| reed-solomon-erasure | 2.36 | 3.46 | 3.93 | 4.39 | 5.24 |
| reed-solomon-simd | 0.03 | 0.12 | 0.35 | 0.64 | 0.93 |
| raptorq | 0.10 | 0.14 | 0.12 | 0.14 | 0.12 |
| rlnc | 1.81 | 2.42 | 2.87 | 3.32 | 3.16 |
| rlnc, systematic | 0.80 | 0.91 | 0.93 | 0.94 | 0.95 |

## 4+2, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 20.7 | 23.3 | 28.0 | 23.0 | 24.1 |
| isa-l | 11.1 | 14.1 | 15.3 | 15.3 | 15.5 |
| reed-solomon-erasure | 8.33 | 12.3 | 15.4 | 14.0 | 15.2 |
| reed-solomon-simd | 0.03 | 0.13 | 0.39 | 0.70 | 1.03 |
| raptorq | 0.11 | 0.14 | 0.12 | 0.14 | 0.13 |
| rlnc | 4.45 | 5.52 | 5.24 | 4.72 | 4.68 |
| rlnc, systematic | 1.11 | 1.14 | 1.10 | 1.07 | 1.05 |

## 4+2, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.39 | 5.49 | 6.03 | 6.97 | 7.76 |
| isa-l | 4.44 | 5.36 | 5.94 | 6.96 | 7.50 |
| reed-solomon-erasure | 6.17 | 4.78 | 4.83 | 5.15 | 6.82 |

## 4+2, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 25.1 | 20.8 | 24.1 | 20.4 | 21.3 |
| isa-l | 22.3 | 21.8 | 23.2 | 20.3 | 20.7 |
| reed-solomon-erasure | 21.3 | 9.07 | 19.6 | 17.2 | 18.3 |

## 6+3, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 14.0 | 16.0 | 17.3 | 17.7 | 17.7 |
| isa-l | 13.0 | 15.2 | 15.9 | 16.0 | 16.0 |
| reed-solomon-erasure | 10.6 | 9.43 | 9.78 | 9.96 | 11.5 |
| reed-solomon-simd | 5.94 | 7.03 | 7.74 | 7.87 | 8.00 |
| raptorq | 0.98 | 0.88 | 0.85 | 0.74 | 0.84 |
| rlnc | 3.22 | 4.08 | 3.94 | 3.85 | 3.86 |
| rlnc, systematic | 5.22 | 6.47 | 6.77 | 6.80 | 6.43 |

## 6+3, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 3.68 | 3.96 | 4.63 | 4.84 | 4.71 |

## 6+3, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 20.5 | 24.9 | 30.4 | 31.7 | 32.5 |
| isa-l | 20.6 | 24.6 | 28.4 | 29.4 | 29.9 |
| reed-solomon-erasure | 13.9 | 15.7 | 16.6 | 18.8 | 25.5 |
| reed-solomon-simd | 0.19 | 0.64 | 1.44 | 2.29 | 2.60 |
| raptorq | 0.69 | 0.76 | 0.77 | 0.68 | 0.77 |
| rlnc | 3.70 | 3.97 | 4.64 | 4.83 | 4.74 |
| rlnc, systematic | 3.92 | 4.61 | 4.66 | 4.74 | 4.78 |

## 6+3, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 16.0 | 19.7 | 23.2 | 24.1 | 24.9 |
| isa-l | 16.7 | 19.9 | 20.9 | 21.0 | 21.2 |
| reed-solomon-erasure | 9.32 | 10.8 | 12.2 | 12.3 | 15.5 |
| reed-solomon-simd | 0.19 | 0.63 | 1.44 | 2.25 | 2.57 |
| raptorq | 0.70 | 0.78 | 0.78 | 0.69 | 0.78 |
| rlnc | 3.70 | 3.98 | 4.64 | 4.84 | 4.75 |
| rlnc, systematic | 3.28 | 3.74 | 3.88 | 3.94 | 3.90 |

## 6+3, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 13.5 | 15.8 | 17.3 | 17.5 | 17.7 |
| isa-l | 12.7 | 14.9 | 15.8 | 15.8 | 16.0 |
| reed-solomon-erasure | 7.36 | 8.93 | 9.49 | 9.92 | 11.5 |
| reed-solomon-simd | 0.19 | 0.63 | 1.43 | 2.23 | 2.54 |
| raptorq | 0.72 | 0.78 | 0.79 | 0.69 | 0.78 |
| rlnc | 3.70 | 3.98 | 4.66 | 4.85 | 4.75 |
| rlnc, systematic | 3.02 | 3.37 | 3.61 | 3.66 | 3.61 |

## 6+3, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.44 | 4.17 | 5.08 | 5.27 | 5.42 |
| isa-l | 3.40 | 4.08 | 4.72 | 4.90 | 4.99 |
| reed-solomon-erasure | 2.28 | 2.63 | 2.77 | 3.13 | 4.26 |
| reed-solomon-simd | 0.03 | 0.11 | 0.24 | 0.38 | 0.44 |
| raptorq | 0.12 | 0.13 | 0.13 | 0.11 | 0.13 |
| rlnc | 1.35 | 1.73 | 2.09 | 2.27 | 2.17 |
| rlnc, systematic | 0.66 | 0.77 | 0.78 | 0.79 | 0.80 |

## 6+3, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.75 | 4.46 | 5.07 | 5.88 | 6.13 |
| isa-l | 3.76 | 4.49 | 5.11 | 5.80 | 6.04 |
| reed-solomon-erasure | 4.66 | 3.67 | 3.60 | 3.80 | 5.07 |

## 8+3, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.6 | 18.2 | 19.6 | 19.9 | 20.0 |
| isa-l | 14.9 | 17.4 | 18.4 | 18.8 | 18.8 |
| reed-solomon-erasure | 10.9 | 9.93 | 10.4 | 10.6 | 12.3 |
| reed-solomon-simd | 7.01 | 8.15 | 9.66 | 9.98 | 10.0 |
| raptorq | 1.03 | 1.14 | 1.10 | 0.96 | 1.10 |
| rlnc | 3.08 | 3.76 | 3.55 | 3.42 | 3.42 |
| rlnc, systematic | 5.58 | 7.22 | 7.13 | 7.24 | 6.24 |

## 8+3, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 3.32 | 3.70 | 4.14 | 4.03 | 3.95 |

## 8+3, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 22.8 | 27.0 | 31.3 | 32.4 | 33.2 |
| isa-l | 22.1 | 26.2 | 28.1 | 28.5 | 28.6 |
| reed-solomon-erasure | 16.8 | 17.3 | 17.4 | 19.4 | 27.9 |
| reed-solomon-simd | 0.26 | 0.84 | 1.88 | 2.96 | 3.35 |
| raptorq | 0.76 | 0.99 | 1.00 | 0.88 | 1.00 |
| rlnc | 3.33 | 3.70 | 4.14 | 4.03 | 3.97 |
| rlnc, systematic | 4.46 | 5.29 | 5.35 | 5.44 | 5.23 |

## 8+3, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 18.2 | 21.5 | 24.1 | 24.6 | 24.7 |
| isa-l | 17.4 | 20.7 | 22.0 | 22.2 | 22.2 |
| reed-solomon-erasure | 11.3 | 11.5 | 12.9 | 13.1 | 16.5 |
| reed-solomon-simd | 0.26 | 0.84 | 1.87 | 2.92 | 3.32 |
| raptorq | 0.77 | 1.00 | 1.01 | 0.89 | 1.02 |
| rlnc | 3.34 | 3.70 | 4.14 | 4.04 | 3.95 |
| rlnc, systematic | 3.69 | 4.21 | 4.29 | 4.41 | 4.19 |

## 8+3, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.3 | 17.9 | 19.4 | 19.8 | 19.9 |
| isa-l | 14.3 | 16.8 | 17.9 | 18.3 | 18.5 |
| reed-solomon-erasure | 8.39 | 9.58 | 10.1 | 10.6 | 12.4 |
| reed-solomon-simd | 0.26 | 0.84 | 1.87 | 2.88 | 3.27 |
| raptorq | 0.78 | 1.01 | 1.01 | 0.90 | 1.02 |
| rlnc | 3.36 | 3.71 | 4.15 | 4.05 | 3.97 |
| rlnc, systematic | 3.38 | 3.86 | 4.01 | 4.05 | 3.87 |

## 8+3, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.85 | 3.37 | 3.91 | 4.06 | 4.15 |
| isa-l | 2.74 | 3.26 | 3.51 | 3.56 | 3.58 |
| reed-solomon-erasure | 2.04 | 2.15 | 2.16 | 2.44 | 3.47 |
| reed-solomon-simd | 0.03 | 0.10 | 0.24 | 0.37 | 0.42 |
| raptorq | 0.09 | 0.12 | 0.12 | 0.11 | 0.13 |
| rlnc | 1.05 | 1.36 | 1.67 | 1.75 | 1.58 |
| rlnc, systematic | 0.56 | 0.67 | 0.67 | 0.68 | 0.65 |

## 8+3, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.97 | 4.77 | 5.45 | 6.33 | 6.61 |
| isa-l | 3.98 | 4.77 | 5.41 | 6.25 | 6.41 |
| reed-solomon-erasure | 5.01 | 3.84 | 3.75 | 3.99 | 5.33 |

## 10+4, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.0 | 16.3 | 16.8 | 17.2 | 17.3 |
| isa-l | 13.2 | 14.2 | 13.5 | 13.7 | 12.8 |
| reed-solomon-erasure | 8.12 | 8.49 | 8.77 | 9.31 | 10.2 |
| reed-solomon-simd | 7.37 | 7.53 | 9.15 | 9.71 | 9.75 |
| raptorq | 1.22 | 1.38 | 1.16 | 1.16 | 1.15 |
| rlnc | 2.64 | 3.26 | 2.96 | 2.86 | 2.87 |
| rlnc, systematic | 4.85 | 6.56 | 6.40 | 6.21 | 4.49 |

## 10+4, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 32.2 | 36.1 | 36.3 | 35.6 | 35.4 |
| isa-l | 18.0 | 18.4 | 18.3 | 18.4 | 18.1 |
| reed-solomon-erasure | 16.8 | 15.5 | 16.3 | 14.2 | 15.4 |
| reed-solomon-simd | 13.5 | 13.3 | 14.1 | 13.7 | 11.9 |
| raptorq | 1.35 | 1.47 | 1.41 | 1.23 | 1.40 |
| rlnc | 4.40 | 4.77 | 3.83 | 3.44 | 3.07 |
| rlnc, systematic | 8.07 | 9.58 | 7.89 | 7.55 | 4.96 |

## 10+4, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 2.95 | 3.33 | 3.62 | 3.49 | 3.33 |

## 10+4, decode-0, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 4.78 | 4.83 | 4.55 | 4.20 | 3.33 |

## 10+4, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 23.2 | 27.0 | 31.9 | 33.2 | 34.0 |
| isa-l | 22.3 | 26.8 | 29.7 | 30.0 | 30.5 |
| reed-solomon-erasure | 19.3 | 18.1 | 17.6 | 19.8 | 29.3 |
| reed-solomon-simd | 0.31 | 1.01 | 2.17 | 3.05 | 3.31 |
| raptorq | 0.91 | 1.19 | 1.06 | 1.07 | 1.06 |
| rlnc | 2.95 | 3.34 | 3.63 | 3.49 | 3.31 |
| rlnc, systematic | 4.81 | 5.84 | 5.82 | 5.98 | 5.28 |

## 10+4, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 72.7 | 91.2 | 96.1 | 96.0 | 96.8 |
| isa-l | 49.4 | 52.8 | 56.2 | 57.5 | 57.9 |
| reed-solomon-erasure | 48.5 | 56.2 | 60.0 | 55.6 | 61.4 |
| reed-solomon-simd | 0.33 | 1.07 | 2.34 | 3.29 | 3.31 |
| raptorq | 1.19 | 1.29 | 1.31 | 1.13 | 1.30 |
| rlnc | 4.76 | 4.83 | 4.53 | 4.20 | 3.35 |
| rlnc, systematic | 7.40 | 8.03 | 7.22 | 7.20 | 5.10 |

## 10+4, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 19.4 | 22.6 | 25.1 | 25.6 | 25.9 |
| isa-l | 19.0 | 22.0 | 23.5 | 24.0 | 24.5 |
| reed-solomon-erasure | 12.5 | 11.8 | 13.2 | 13.3 | 17.1 |
| reed-solomon-simd | 0.31 | 1.01 | 2.16 | 3.01 | 3.28 |
| raptorq | 0.92 | 1.20 | 1.07 | 1.07 | 1.07 |
| rlnc | 2.95 | 3.34 | 3.63 | 3.49 | 3.33 |
| rlnc, systematic | 3.94 | 4.60 | 4.69 | 4.74 | 4.23 |

## 10+4, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 54.4 | 57.1 | 58.6 | 57.9 | 58.4 |
| isa-l | 31.0 | 34.1 | 34.8 | 34.9 | 35.0 |
| reed-solomon-erasure | 29.0 | 29.5 | 31.5 | 29.1 | 30.6 |
| reed-solomon-simd | 0.33 | 1.07 | 2.34 | 3.28 | 3.30 |
| raptorq | 1.20 | 1.29 | 1.30 | 1.13 | 1.30 |
| rlnc | 4.76 | 4.80 | 4.55 | 4.21 | 3.36 |
| rlnc, systematic | 5.67 | 5.98 | 5.60 | 5.54 | 4.08 |

## 10+4, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 16.4 | 18.9 | 20.4 | 21.0 | 21.3 |
| isa-l | 15.1 | 17.8 | 19.1 | 19.6 | 20.1 |
| reed-solomon-erasure | 8.64 | 9.73 | 10.4 | 11.0 | 12.9 |
| reed-solomon-simd | 0.31 | 1.00 | 2.15 | 2.98 | 3.25 |
| raptorq | 0.93 | 1.22 | 1.07 | 1.09 | 1.07 |
| rlnc | 2.96 | 3.32 | 3.62 | 3.50 | 3.33 |
| rlnc, systematic | 3.63 | 4.23 | 4.34 | 4.33 | 3.91 |

## 10+4, decode-3, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 35.5 | 40.6 | 41.4 | 40.7 | 40.7 |
| isa-l | 22.3 | 23.9 | 24.2 | 24.2 | 24.3 |
| reed-solomon-erasure | 19.8 | 20.1 | 21.3 | 19.0 | 20.4 |
| reed-solomon-simd | 0.33 | 1.07 | 2.34 | 3.27 | 3.28 |
| raptorq | 1.21 | 1.32 | 1.32 | 1.14 | 1.30 |
| rlnc | 4.79 | 4.85 | 4.57 | 4.24 | 3.35 |
| rlnc, systematic | 5.22 | 5.49 | 5.16 | 5.03 | 3.79 |

## 10+4, decode-4, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 14.2 | 16.0 | 16.8 | 16.5 | 16.9 |
| isa-l | 12.7 | 13.5 | 13.2 | 13.3 | 12.2 |
| reed-solomon-erasure | 7.27 | 8.20 | 8.58 | 9.33 | 10.2 |
| reed-solomon-simd | 0.31 | 1.00 | 2.14 | 2.96 | 3.22 |
| raptorq | 0.95 | 1.23 | 1.09 | 1.10 | 1.09 |
| rlnc | 2.95 | 3.34 | 3.63 | 3.50 | 3.33 |
| rlnc, systematic | 3.31 | 3.80 | 3.98 | 3.91 | 3.56 |

## 10+4, decode-4, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 28.6 | 35.6 | 35.8 | 35.5 | 35.5 |
| isa-l | 17.3 | 18.3 | 18.3 | 18.4 | 18.4 |
| reed-solomon-erasure | 15.2 | 15.3 | 16.0 | 14.3 | 15.2 |
| reed-solomon-simd | 0.33 | 1.07 | 2.33 | 3.27 | 3.26 |
| raptorq | 1.04 | 1.33 | 1.33 | 1.16 | 1.33 |
| rlnc | 4.76 | 4.82 | 4.55 | 4.21 | 3.33 |
| rlnc, systematic | 4.78 | 4.95 | 4.66 | 4.51 | 3.46 |

## 10+4, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.31 | 2.70 | 3.20 | 3.32 | 3.41 |
| isa-l | 2.21 | 2.69 | 2.97 | 3.00 | 3.04 |
| reed-solomon-erasure | 1.90 | 1.82 | 1.76 | 1.98 | 2.91 |
| reed-solomon-simd | 0.03 | 0.10 | 0.22 | 0.31 | 0.33 |
| raptorq | 0.09 | 0.12 | 0.11 | 0.11 | 0.11 |
| rlnc | 0.97 | 1.13 | 1.40 | 1.42 | 1.12 |
| rlnc, systematic | 0.49 | 0.59 | 0.58 | 0.60 | 0.53 |

## 10+4, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 7.26 | 9.13 | 9.60 | 9.57 | 9.68 |
| isa-l | 4.95 | 5.29 | 5.62 | 5.76 | 5.79 |
| reed-solomon-erasure | 4.58 | 5.52 | 5.96 | 5.55 | 6.14 |
| reed-solomon-simd | 0.03 | 0.11 | 0.23 | 0.33 | 0.33 |
| raptorq | 0.12 | 0.13 | 0.13 | 0.11 | 0.13 |
| rlnc | 1.96 | 2.29 | 1.93 | 1.91 | 1.22 |
| rlnc, systematic | 0.74 | 0.80 | 0.72 | 0.72 | 0.51 |

## 10+4, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.55 | 4.01 | 4.54 | 5.12 | 5.40 |
| isa-l | 3.49 | 3.95 | 4.49 | 5.07 | 5.31 |
| reed-solomon-erasure | 4.09 | 2.99 | 2.97 | 3.14 | 4.26 |

## 10+4, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 16.8 | 16.4 | 18.0 | 14.5 | 14.5 |
| isa-l | 15.6 | 15.9 | 16.9 | 14.6 | 14.8 |
| reed-solomon-erasure | 12.0 | 11.0 | 12.8 | 10.6 | 11.4 |

## 4+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 17.4 | 21.7 | 25.6 | 26.2 | 26.3 |
| rusty_erasure | 19.7 | 23.2 | 27.3 | 28.7 | 28.4 |

## 4+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 15.7 | 19.5 | 24.7 | 25.5 | 25.6 |
| rusty_erasure | 18.4 | 21.8 | 26.6 | 28.3 | 28.7 |

## 4+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 3.99 | 4.88 | 6.17 | 6.38 | 6.41 |
| rusty_erasure | 4.57 | 5.43 | 6.65 | 7.04 | 7.18 |

## 4+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 9.96 | 11.8 | 15.5 | 15.9 | 11.5 |
| rusty_erasure | 8.41 | 8.43 | 8.43 | 9.22 | 12.0 |

## 6+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 18.9 | 22.7 | 27.7 | 29.0 | 29.9 |
| rusty_erasure | 22.7 | 26.6 | 31.4 | 32.4 | 33.4 |

## 6+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 17.5 | 21.8 | 27.5 | 28.5 | 29.0 |
| rusty_erasure | 21.4 | 25.4 | 30.6 | 32.1 | 32.8 |

## 6+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 2.96 | 3.64 | 4.53 | 4.74 | 4.83 |
| rusty_erasure | 3.55 | 4.23 | 5.12 | 5.34 | 5.47 |

## 6+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.9 | 13.7 | 18.0 | 18.7 | 17.3 |
| rusty_erasure | 9.59 | 9.38 | 9.41 | 10.3 | 13.5 |

## 8+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 19.8 | 23.3 | 28.6 | 30.6 | 31.2 |
| rusty_erasure | 23.9 | 27.5 | 31.6 | 32.6 | 33.4 |

## 8+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 18.6 | 22.6 | 28.6 | 30.4 | 30.7 |
| rusty_erasure | 23.2 | 26.9 | 31.3 | 32.5 | 33.3 |

## 8+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 2.33 | 2.81 | 3.54 | 3.79 | 3.83 |
| rusty_erasure | 2.89 | 3.36 | 3.91 | 4.06 | 4.17 |

## 8+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 15.4 | 16.6 | 20.8 | 22.1 | 20.9 |
| rusty_erasure | 12.2 | 11.9 | 12.4 | 13.2 | 16.1 |

## 10+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 19.9 | 22.7 | 28.1 | 30.5 | 31.7 |
| rusty_erasure | 24.3 | 28.0 | 32.3 | 33.5 | 34.2 |

## 10+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 18.9 | 22.3 | 28.9 | 30.7 | 31.7 |
| rusty_erasure | 23.6 | 27.3 | 32.0 | 33.3 | 34.2 |

## 10+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 1.90 | 2.23 | 2.85 | 3.08 | 3.17 |
| rusty_erasure | 2.37 | 2.73 | 3.20 | 3.32 | 3.44 |

## 10+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 20.6 | 19.4 | 24.7 | 26.2 | 26.4 |
| rusty_erasure | 16.3 | 15.2 | 16.3 | 17.3 | 20.5 |

## Cells not measured

- raptorq update 10+4: no update in the API; a repair symbol depends on the block's intermediate symbols
- raptorq update 2+1: no update in the API; a repair symbol depends on the block's intermediate symbols
- raptorq update 4+2: no update in the API; a repair symbol depends on the block's intermediate symbols
- raptorq update 6+3: no update in the API; a repair symbol depends on the block's intermediate symbols
- raptorq update 8+3: no update in the API; a repair symbol depends on the block's intermediate symbols
- reed-solomon-simd update 10+4: no update in the API; the FFT form exposes no coefficient to fold a change with
- reed-solomon-simd update 2+1: no update in the API; the FFT form exposes no coefficient to fold a change with
- reed-solomon-simd update 4+2: no update in the API; the FFT form exposes no coefficient to fold a change with
- reed-solomon-simd update 6+3: no update in the API; the FFT form exposes no coefficient to fold a change with
- reed-solomon-simd update 8+3: no update in the API; the FFT form exposes no coefficient to fold a change with
- rlnc update 10+4: no update in the API; every piece mixes every data byte, so a write rewrites k + m chunks
- rlnc update 2+1: no update in the API; every piece mixes every data byte, so a write rewrites k + m chunks
- rlnc update 4+2: no update in the API; every piece mixes every data byte, so a write rewrites k + m chunks
- rlnc update 6+3: no update in the API; every piece mixes every data byte, so a write rewrites k + m chunks
- rlnc update 8+3: no update in the API; every piece mixes every data byte, so a write rewrites k + m chunks
- rlnc, systematic update 10+4: no update in the API; its GF(2^8) kernels are in a private module
- rlnc, systematic update 2+1: no update in the API; its GF(2^8) kernels are in a private module
- rlnc, systematic update 4+2: no update in the API; its GF(2^8) kernels are in a private module
- rlnc, systematic update 6+3: no update in the API; its GF(2^8) kernels are in a private module
- rlnc, systematic update 8+3: no update in the API; its GF(2^8) kernels are in a private module
- xor encode 10+4: one parity chunk only
- xor encode 4+2: one parity chunk only
- xor encode 6+3: one parity chunk only
- xor encode 8+3: one parity chunk only
