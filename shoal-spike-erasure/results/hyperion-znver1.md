# X4 results: hyperion znver1

Every speed cell of one run of `shoal-spike-erasure`. The figure is the median of 3 measurements, each at least 200 ms of whole calls; encode and decode are counted by the stripe data (k × unit), rebuild by the chunk rebuilt and update by the bytes changed. `n/a` is a cell the candidate cannot run, with the reason below the table.

| Run | Host | CPU | Build | rse C | Governor | Core | Detected | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| hyperion znver1 | hyperion | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | `target-cpu=znver1` | `-march=haswell` | performance | 2 | ssse3, avx2 | 3 × 200 ms | 2026-10-03T18:09:17Z |

## 2+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 9.42 | 10.2 | 10.4 | 10.4 | 10.5 |
| rusty_erasure | 7.60 | 9.52 | 10.1 | 10.2 | 10.3 |
| isa-l | 5.64 | 7.75 | 8.72 | 8.97 | 9.00 |
| reed-solomon-erasure | 7.24 | 8.04 | 8.32 | 8.24 | 8.20 |
| reed-solomon-simd | 6.06 | 6.25 | 6.73 | 6.66 | 5.79 |
| raptorq | 0.08 | 0.08 | 0.08 | 0.08 | 0.08 |
| rlnc | 1.78 | 1.92 | 1.96 | 2.15 | 1.85 |
| rlnc, systematic | 2.25 | 2.56 | 2.76 | 2.76 | 1.42 |

## 2+1, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 2.04 | 2.40 | 2.53 | 2.62 | 1.63 |

## 2+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 8.42 | 9.96 | 10.4 | 10.5 | 10.6 |
| rusty_erasure | 6.57 | 9.25 | 9.92 | 10.1 | 10.3 |
| isa-l | 5.26 | 7.55 | 8.70 | 9.01 | 9.14 |
| reed-solomon-erasure | 5.40 | 7.59 | 8.21 | 8.26 | 8.24 |
| reed-solomon-simd | 0.02 | 0.10 | 0.33 | 0.85 | 0.95 |
| raptorq | 0.07 | 0.08 | 0.08 | 0.08 | 0.08 |
| rlnc | 2.05 | 2.41 | 2.54 | 2.62 | 1.63 |
| rlnc, systematic | 1.02 | 1.13 | 1.17 | 1.17 | 0.79 |

## 2+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.35 | 5.04 | 5.23 | 5.28 | 5.31 |
| rusty_erasure | 3.28 | 4.61 | 4.96 | 5.07 | 5.16 |
| isa-l | 2.56 | 3.76 | 4.34 | 4.47 | 4.56 |
| reed-solomon-erasure | 2.52 | 3.63 | 4.08 | 4.13 | 4.12 |
| reed-solomon-simd | 0.01 | 0.05 | 0.17 | 0.42 | 0.48 |
| raptorq | 0.03 | 0.04 | 0.04 | 0.04 | 0.04 |
| rlnc | 1.40 | 1.86 | 2.01 | 2.12 | 1.17 |
| rlnc, systematic | 0.50 | 0.57 | 0.58 | 0.58 | 0.40 |

## 2+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.48 | 5.04 | 5.28 | 5.66 | 4.70 |
| rusty_erasure | 3.80 | 4.47 | 4.86 | 4.59 | 4.48 |
| isa-l | 3.58 | 4.40 | 4.82 | 4.63 | 4.49 |
| reed-solomon-erasure | 3.95 | 4.50 | 4.75 | 4.59 | 4.51 |

## 4+2, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.87 | 7.35 | 7.64 | 7.80 | 7.81 |
| isa-l | 4.62 | 4.88 | 4.97 | 5.02 | 5.02 |
| reed-solomon-erasure | 5.49 | 5.61 | 5.62 | 5.81 | 4.61 |
| reed-solomon-simd | 4.51 | 4.61 | 4.77 | 4.75 | 2.89 |
| raptorq | 0.16 | 0.16 | 0.16 | 0.16 | 0.16 |
| rlnc | 1.46 | 1.45 | 1.49 | 1.57 | 1.23 |
| rlnc, systematic | 2.05 | 2.31 | 2.44 | 2.08 | 1.40 |

## 4+2, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 8.95 | 9.20 | 9.78 | 9.87 | 8.42 |
| isa-l | 4.79 | 4.83 | 5.28 | 5.32 | 5.12 |
| reed-solomon-erasure | 7.38 | 7.78 | 8.07 | 8.12 | 5.82 |
| reed-solomon-simd | 7.21 | 7.44 | 7.42 | 7.31 | 2.93 |
| raptorq | 0.16 | 0.16 | 0.16 | 0.16 | 0.16 |
| rlnc | 2.04 | 2.07 | 2.09 | 1.89 | 1.19 |
| rlnc, systematic | 2.63 | 2.90 | 2.95 | 2.12 | 1.40 |

## 4+2, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 1.51 | 1.74 | 1.78 | 1.72 | 1.23 |

## 4+2, decode-0, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 2.22 | 2.35 | 2.30 | 1.93 | 1.21 |

## 4+2, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 7.28 | 10.8 | 12.3 | 12.7 | 12.8 |
| isa-l | 6.34 | 8.32 | 9.03 | 9.34 | 9.37 |
| reed-solomon-erasure | 7.07 | 9.52 | 10.1 | 10.2 | 10.1 |
| reed-solomon-simd | 0.05 | 0.18 | 0.51 | 0.93 | 0.57 |
| raptorq | 0.13 | 0.15 | 0.15 | 0.15 | 0.15 |
| rlnc | 1.51 | 1.74 | 1.78 | 1.72 | 1.21 |
| rlnc, systematic | 1.55 | 1.75 | 1.80 | 1.58 | 1.14 |

## 4+2, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 11.6 | 15.4 | 16.2 | 16.5 | 15.0 |
| isa-l | 6.97 | 8.47 | 9.80 | 10.2 | 9.89 |
| reed-solomon-erasure | 10.8 | 14.3 | 15.7 | 16.1 | 12.8 |
| reed-solomon-simd | 0.05 | 0.18 | 0.53 | 0.97 | 0.57 |
| raptorq | 0.13 | 0.15 | 0.15 | 0.15 | 0.15 |
| rlnc | 2.22 | 2.35 | 2.30 | 1.93 | 1.22 |
| rlnc, systematic | 1.87 | 2.01 | 1.99 | 1.60 | 1.14 |

## 4+2, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.73 | 7.17 | 7.64 | 7.84 | 7.82 |
| isa-l | 4.27 | 4.79 | 4.95 | 5.01 | 5.00 |
| reed-solomon-erasure | 4.84 | 5.48 | 5.60 | 5.80 | 4.61 |
| reed-solomon-simd | 0.05 | 0.18 | 0.50 | 0.91 | 0.56 |
| raptorq | 0.13 | 0.15 | 0.15 | 0.15 | 0.15 |
| rlnc | 1.51 | 1.74 | 1.78 | 1.72 | 1.23 |
| rlnc, systematic | 1.20 | 1.33 | 1.35 | 1.21 | 0.88 |

## 4+2, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 8.25 | 9.80 | 9.75 | 9.85 | 8.52 |
| isa-l | 4.33 | 5.02 | 5.30 | 5.32 | 5.11 |
| reed-solomon-erasure | 6.38 | 7.50 | 7.99 | 8.10 | 5.86 |
| reed-solomon-simd | 0.05 | 0.18 | 0.52 | 0.96 | 0.56 |
| raptorq | 0.13 | 0.15 | 0.15 | 0.15 | 0.15 |
| rlnc | 2.21 | 2.35 | 2.30 | 1.94 | 1.22 |
| rlnc, systematic | 1.43 | 1.51 | 1.50 | 1.22 | 0.88 |

## 4+2, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.85 | 2.71 | 3.06 | 3.18 | 3.21 |
| isa-l | 1.58 | 2.09 | 2.26 | 2.34 | 2.37 |
| reed-solomon-erasure | 1.68 | 2.33 | 2.51 | 2.55 | 2.52 |
| reed-solomon-simd | 0.01 | 0.04 | 0.13 | 0.23 | 0.14 |
| raptorq | 0.03 | 0.04 | 0.04 | 0.04 | 0.04 |
| rlnc | 0.83 | 1.07 | 1.17 | 1.16 | 0.58 |
| rlnc, systematic | 0.39 | 0.44 | 0.45 | 0.40 | 0.29 |

## 4+2, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.88 | 3.83 | 4.06 | 4.11 | 3.74 |
| isa-l | 1.72 | 2.13 | 2.45 | 2.56 | 2.47 |
| reed-solomon-erasure | 2.48 | 3.48 | 3.90 | 4.01 | 3.21 |
| reed-solomon-simd | 0.01 | 0.04 | 0.13 | 0.24 | 0.14 |
| raptorq | 0.03 | 0.04 | 0.04 | 0.04 | 0.04 |
| rlnc | 1.44 | 1.62 | 1.64 | 1.34 | 0.58 |
| rlnc, systematic | 0.47 | 0.49 | 0.50 | 0.40 | 0.29 |

## 4+2, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.52 | 2.98 | 3.25 | 3.12 | 3.02 |
| isa-l | 2.58 | 3.01 | 3.13 | 3.09 | 2.97 |
| reed-solomon-erasure | 2.59 | 2.74 | 2.83 | 2.77 | 2.68 |

## 4+2, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.49 | 6.22 | 6.66 | 6.62 | 3.62 |
| isa-l | 5.82 | 6.46 | 6.85 | 6.78 | 3.68 |
| reed-solomon-erasure | 5.01 | 5.55 | 5.88 | 5.82 | 3.13 |

## 6+3, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.40 | 6.34 | 6.49 | 6.58 | 6.66 |
| isa-l | 3.66 | 3.67 | 3.71 | 3.74 | 3.73 |
| reed-solomon-erasure | 4.25 | 4.15 | 4.19 | 4.30 | 2.70 |
| reed-solomon-simd | 2.69 | 2.81 | 2.88 | 2.71 | 1.50 |
| raptorq | 0.23 | 0.23 | 0.23 | 0.23 | 0.23 |
| rlnc | 1.00 | 1.00 | 1.05 | 1.05 | 0.88 |
| rlnc, systematic | 1.77 | 1.98 | 2.10 | 1.52 | 1.30 |

## 6+3, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 1.22 | 1.36 | 1.43 | 1.23 | 0.94 |

## 6+3, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 8.77 | 11.0 | 11.8 | 12.1 | 12.1 |
| isa-l | 6.69 | 7.61 | 7.91 | 8.03 | 8.03 |
| reed-solomon-erasure | 8.42 | 10.3 | 11.0 | 11.0 | 10.9 |
| reed-solomon-simd | 0.07 | 0.23 | 0.52 | 0.59 | 0.38 |
| raptorq | 0.18 | 0.21 | 0.22 | 0.22 | 0.22 |
| rlnc | 1.22 | 1.36 | 1.43 | 1.24 | 0.94 |
| rlnc, systematic | 1.86 | 2.12 | 2.20 | 1.59 | 1.34 |

## 6+3, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 6.25 | 7.90 | 8.35 | 8.56 | 8.57 |
| isa-l | 4.51 | 5.01 | 5.15 | 5.21 | 5.22 |
| reed-solomon-erasure | 5.24 | 5.82 | 6.04 | 6.22 | 4.94 |
| reed-solomon-simd | 0.07 | 0.23 | 0.51 | 0.59 | 0.38 |
| raptorq | 0.18 | 0.21 | 0.22 | 0.22 | 0.22 |
| rlnc | 1.22 | 1.36 | 1.43 | 1.26 | 0.94 |
| rlnc, systematic | 1.35 | 1.51 | 1.56 | 1.16 | 0.97 |

## 6+3, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.06 | 6.26 | 6.50 | 6.60 | 6.62 |
| isa-l | 3.42 | 3.65 | 3.69 | 3.73 | 3.74 |
| reed-solomon-erasure | 3.97 | 4.12 | 4.17 | 4.29 | 2.70 |
| reed-solomon-simd | 0.07 | 0.22 | 0.51 | 0.58 | 0.38 |
| raptorq | 0.18 | 0.21 | 0.22 | 0.22 | 0.22 |
| rlnc | 1.22 | 1.37 | 1.43 | 1.27 | 0.94 |
| rlnc, systematic | 1.21 | 1.35 | 1.40 | 1.08 | 0.88 |

## 6+3, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.46 | 1.84 | 1.96 | 2.01 | 2.02 |
| isa-l | 1.11 | 1.27 | 1.32 | 1.34 | 1.34 |
| reed-solomon-erasure | 1.34 | 1.69 | 1.82 | 1.83 | 1.81 |
| reed-solomon-simd | 0.01 | 0.04 | 0.09 | 0.10 | 0.06 |
| raptorq | 0.03 | 0.03 | 0.04 | 0.04 | 0.04 |
| rlnc | 0.57 | 0.71 | 0.77 | 0.59 | 0.40 |
| rlnc, systematic | 0.31 | 0.35 | 0.37 | 0.27 | 0.22 |

## 6+3, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.97 | 2.34 | 2.47 | 2.45 | 2.26 |
| isa-l | 1.86 | 2.21 | 2.36 | 2.18 | 2.19 |
| reed-solomon-erasure | 1.84 | 1.95 | 2.02 | 1.99 | 1.93 |

## 8+3, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.92 | 6.63 | 6.67 | 6.82 | 6.89 |
| isa-l | 3.72 | 3.71 | 3.74 | 3.75 | 3.76 |
| reed-solomon-erasure | 4.44 | 4.31 | 4.35 | 4.43 | 2.73 |
| reed-solomon-simd | 3.45 | 3.61 | 3.68 | 3.49 | 1.95 |
| raptorq | 0.30 | 0.30 | 0.30 | 0.30 | 0.30 |
| rlnc | 1.02 | 0.99 | 1.05 | 1.00 | 0.81 |
| rlnc, systematic | 1.94 | 2.13 | 2.21 | 1.42 | 1.37 |

## 8+3, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 1.02 | 1.12 | 1.17 | 0.94 | 0.76 |

## 8+3, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 8.91 | 11.3 | 11.8 | 12.0 | 12.1 |
| isa-l | 7.07 | 8.14 | 8.47 | 8.56 | 8.55 |
| reed-solomon-erasure | 8.91 | 10.8 | 11.4 | 11.4 | 11.2 |
| reed-solomon-simd | 0.09 | 0.30 | 0.67 | 0.77 | 0.49 |
| raptorq | 0.23 | 0.28 | 0.28 | 0.28 | 0.28 |
| rlnc | 1.02 | 1.12 | 1.17 | 0.96 | 0.76 |
| rlnc, systematic | 2.09 | 2.34 | 2.44 | 1.51 | 1.48 |

## 8+3, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 6.66 | 8.47 | 8.90 | 9.08 | 9.09 |
| isa-l | 4.68 | 5.16 | 5.26 | 5.30 | 5.31 |
| reed-solomon-erasure | 5.59 | 6.08 | 6.26 | 6.40 | 5.10 |
| reed-solomon-simd | 0.09 | 0.30 | 0.67 | 0.76 | 0.48 |
| raptorq | 0.24 | 0.28 | 0.29 | 0.29 | 0.29 |
| rlnc | 1.02 | 1.12 | 1.17 | 0.97 | 0.76 |
| rlnc, systematic | 1.51 | 1.66 | 1.73 | 1.17 | 1.07 |

## 8+3, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.43 | 6.57 | 6.78 | 6.78 | 6.87 |
| isa-l | 3.53 | 3.68 | 3.72 | 3.76 | 3.76 |
| reed-solomon-erasure | 4.14 | 4.28 | 4.34 | 4.42 | 2.73 |
| reed-solomon-simd | 0.09 | 0.30 | 0.67 | 0.76 | 0.48 |
| raptorq | 0.24 | 0.28 | 0.29 | 0.29 | 0.29 |
| rlnc | 1.02 | 1.12 | 1.18 | 0.97 | 0.76 |
| rlnc, systematic | 1.30 | 1.44 | 1.50 | 1.06 | 0.93 |

## 8+3, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.11 | 1.41 | 1.48 | 1.50 | 1.51 |
| isa-l | 0.89 | 1.02 | 1.06 | 1.07 | 1.07 |
| reed-solomon-erasure | 1.07 | 1.33 | 1.42 | 1.42 | 1.40 |
| reed-solomon-simd | 0.01 | 0.04 | 0.08 | 0.10 | 0.06 |
| raptorq | 0.03 | 0.03 | 0.04 | 0.04 | 0.04 |
| rlnc | 0.46 | 0.58 | 0.63 | 0.36 | 0.31 |
| rlnc, systematic | 0.26 | 0.29 | 0.31 | 0.19 | 0.19 |

## 8+3, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.00 | 2.34 | 2.46 | 2.40 | 2.20 |
| isa-l | 1.89 | 2.21 | 2.32 | 2.20 | 2.19 |
| reed-solomon-erasure | 1.85 | 1.95 | 2.04 | 1.98 | 1.93 |

## 10+4, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.31 | 5.19 | 5.26 | 5.32 | 5.37 |
| isa-l | 3.45 | 3.31 | 3.38 | 3.39 | 3.41 |
| reed-solomon-erasure | 3.43 | 3.29 | 3.35 | 3.37 | 1.81 |
| reed-solomon-simd | 3.02 | 3.19 | 3.23 | 2.63 | 1.62 |
| raptorq | 0.36 | 0.37 | 0.37 | 0.37 | 0.37 |
| rlnc | 0.73 | 0.73 | 0.76 | 0.70 | 0.62 |
| rlnc, systematic | 1.63 | 1.81 | 1.87 | 1.21 | 1.22 |

## 10+4, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.52 | 5.85 | 5.94 | 5.90 | 5.37 |
| isa-l | 3.22 | 3.38 | 3.47 | 3.48 | 3.33 |
| reed-solomon-erasure | 3.91 | 3.93 | 3.95 | 3.78 | 1.81 |
| reed-solomon-simd | 4.19 | 4.21 | 4.25 | 2.55 | 1.63 |
| raptorq | 0.37 | 0.38 | 0.38 | 0.37 | 0.37 |
| rlnc | 0.84 | 0.83 | 0.84 | 0.70 | 0.61 |
| rlnc, systematic | 1.90 | 2.07 | 2.13 | 1.20 | 1.22 |

## 10+4, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 0.87 | 0.96 | 0.99 | 0.77 | 0.64 |

## 10+4, decode-0, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 1.03 | 1.10 | 1.13 | 0.77 | 0.64 |

## 10+4, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 9.33 | 11.5 | 11.8 | 12.0 | 12.1 |
| isa-l | 7.25 | 8.29 | 8.53 | 8.63 | 8.63 |
| reed-solomon-erasure | 9.56 | 11.0 | 11.7 | 11.6 | 11.4 |
| reed-solomon-simd | 0.11 | 0.36 | 0.77 | 0.84 | 0.53 |
| raptorq | 0.29 | 0.34 | 0.35 | 0.35 | 0.35 |
| rlnc | 0.87 | 0.96 | 0.99 | 0.79 | 0.64 |
| rlnc, systematic | 2.21 | 2.54 | 2.62 | 1.47 | 1.55 |

## 10+4, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 13.5 | 15.4 | 15.7 | 15.8 | 12.1 |
| isa-l | 7.70 | 8.99 | 9.21 | 9.31 | 8.63 |
| reed-solomon-erasure | 13.0 | 14.7 | 15.5 | 15.1 | 12.3 |
| reed-solomon-simd | 0.11 | 0.37 | 0.80 | 0.84 | 0.54 |
| raptorq | 0.29 | 0.35 | 0.36 | 0.35 | 0.35 |
| rlnc | 1.03 | 1.10 | 1.12 | 0.79 | 0.64 |
| rlnc, systematic | 2.62 | 2.87 | 2.98 | 1.47 | 1.55 |

## 10+4, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 7.21 | 8.79 | 9.14 | 9.27 | 9.29 |
| isa-l | 4.70 | 5.15 | 5.26 | 5.31 | 5.31 |
| reed-solomon-erasure | 5.84 | 6.23 | 6.39 | 6.50 | 5.20 |
| reed-solomon-simd | 0.11 | 0.36 | 0.76 | 0.84 | 0.54 |
| raptorq | 0.29 | 0.34 | 0.35 | 0.35 | 0.35 |
| rlnc | 0.87 | 0.96 | 0.99 | 0.80 | 0.64 |
| rlnc, systematic | 1.54 | 1.72 | 1.74 | 1.11 | 1.10 |

## 10+4, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 9.30 | 10.2 | 10.6 | 10.7 | 9.32 |
| isa-l | 5.06 | 5.38 | 5.51 | 5.54 | 5.32 |
| reed-solomon-erasure | 7.17 | 7.56 | 7.86 | 7.56 | 5.75 |
| reed-solomon-simd | 0.11 | 0.36 | 0.80 | 0.83 | 0.54 |
| raptorq | 0.30 | 0.35 | 0.36 | 0.35 | 0.35 |
| rlnc | 1.03 | 1.11 | 1.11 | 0.80 | 0.64 |
| rlnc, systematic | 1.77 | 1.87 | 1.91 | 1.10 | 1.10 |

## 10+4, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.90 | 6.73 | 6.89 | 7.00 | 7.03 |
| isa-l | 3.60 | 3.72 | 3.77 | 3.80 | 3.80 |
| reed-solomon-erasure | 4.23 | 4.32 | 4.40 | 4.50 | 2.76 |
| reed-solomon-simd | 0.11 | 0.36 | 0.76 | 0.83 | 0.53 |
| raptorq | 0.29 | 0.34 | 0.35 | 0.35 | 0.35 |
| rlnc | 0.87 | 0.96 | 0.99 | 0.80 | 0.64 |
| rlnc, systematic | 1.35 | 1.50 | 1.54 | 1.02 | 0.95 |

## 10+4, decode-3, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 6.85 | 7.62 | 7.76 | 7.78 | 7.04 |
| isa-l | 3.65 | 3.83 | 3.89 | 3.90 | 3.80 |
| reed-solomon-erasure | 4.78 | 5.12 | 5.25 | 5.03 | 2.89 |
| reed-solomon-simd | 0.11 | 0.36 | 0.80 | 0.82 | 0.53 |
| raptorq | 0.30 | 0.35 | 0.36 | 0.35 | 0.35 |
| rlnc | 1.03 | 1.11 | 1.11 | 0.79 | 0.64 |
| rlnc, systematic | 1.56 | 1.65 | 1.69 | 1.02 | 0.95 |

## 10+4, decode-4, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.88 | 5.18 | 5.22 | 5.45 | 5.37 |
| isa-l | 3.25 | 3.28 | 3.32 | 3.40 | 3.38 |
| reed-solomon-erasure | 3.27 | 3.30 | 3.35 | 3.36 | 1.82 |
| reed-solomon-simd | 0.11 | 0.36 | 0.76 | 0.82 | 0.53 |
| raptorq | 0.30 | 0.34 | 0.36 | 0.36 | 0.36 |
| rlnc | 0.87 | 0.96 | 0.99 | 0.80 | 0.64 |
| rlnc, systematic | 1.18 | 1.32 | 1.35 | 0.94 | 0.81 |

## 10+4, decode-4, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.47 | 5.86 | 5.91 | 5.92 | 5.38 |
| isa-l | 3.28 | 3.42 | 3.48 | 3.49 | 3.40 |
| reed-solomon-erasure | 3.61 | 3.87 | 3.94 | 3.76 | 1.83 |
| reed-solomon-simd | 0.11 | 0.36 | 0.80 | 0.82 | 0.53 |
| raptorq | 0.31 | 0.35 | 0.36 | 0.36 | 0.36 |
| rlnc | 1.03 | 1.10 | 1.10 | 0.79 | 0.65 |
| rlnc, systematic | 1.36 | 1.44 | 1.48 | 0.94 | 0.82 |

## 10+4, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 0.93 | 1.15 | 1.18 | 1.20 | 1.21 |
| isa-l | 0.72 | 0.83 | 0.85 | 0.86 | 0.86 |
| reed-solomon-erasure | 0.92 | 1.09 | 1.16 | 1.16 | 1.14 |
| reed-solomon-simd | 0.01 | 0.04 | 0.08 | 0.08 | 0.05 |
| raptorq | 0.03 | 0.03 | 0.03 | 0.04 | 0.03 |
| rlnc | 0.36 | 0.44 | 0.48 | 0.25 | 0.24 |
| rlnc, systematic | 0.22 | 0.25 | 0.26 | 0.15 | 0.16 |

## 10+4, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.35 | 1.54 | 1.56 | 1.58 | 1.21 |
| isa-l | 0.77 | 0.90 | 0.92 | 0.93 | 0.86 |
| reed-solomon-erasure | 1.25 | 1.46 | 1.55 | 1.51 | 1.23 |
| reed-solomon-simd | 0.01 | 0.04 | 0.08 | 0.08 | 0.05 |
| raptorq | 0.03 | 0.03 | 0.04 | 0.03 | 0.03 |
| rlnc | 0.56 | 0.60 | 0.61 | 0.25 | 0.24 |
| rlnc, systematic | 0.26 | 0.29 | 0.30 | 0.15 | 0.16 |

## 10+4, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.62 | 1.86 | 1.93 | 1.95 | 1.75 |
| isa-l | 1.49 | 1.78 | 1.86 | 1.83 | 1.74 |
| reed-solomon-erasure | 1.46 | 1.53 | 1.56 | 1.55 | 1.53 |

## 10+4, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.57 | 4.10 | 4.23 | 4.28 | 1.80 |
| isa-l | 2.73 | 3.11 | 3.21 | 3.19 | 1.74 |
| reed-solomon-erasure | 2.99 | 3.24 | 3.34 | 3.33 | 1.55 |

## 4+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.0 | 13.7 | 14.4 | 14.6 | 14.7 |
| rusty_erasure | 8.77 | 11.2 | 12.4 | 12.7 | 12.8 |

## 4+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 9.77 | 13.3 | 14.2 | 14.6 | 14.7 |
| rusty_erasure | 8.22 | 11.1 | 12.3 | 12.7 | 12.8 |

## 4+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 2.51 | 3.36 | 3.57 | 3.65 | 3.66 |
| rusty_erasure | 2.06 | 2.77 | 3.07 | 3.18 | 3.20 |

## 4+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.52 | 5.06 | 5.28 | 5.66 | 4.69 |
| rusty_erasure | 3.80 | 4.47 | 4.82 | 4.68 | 4.40 |

## 6+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.5 | 15.2 | 16.3 | 16.5 | 16.6 |
| rusty_erasure | 9.50 | 11.4 | 11.8 | 12.1 | 12.1 |

## 6+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 10.8 | 14.8 | 16.1 | 16.4 | 16.5 |
| rusty_erasure | 8.85 | 11.1 | 11.7 | 12.1 | 12.1 |

## 6+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 1.83 | 2.48 | 2.70 | 2.74 | 2.76 |
| rusty_erasure | 1.47 | 1.85 | 1.96 | 2.01 | 2.02 |

## 6+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.47 | 4.97 | 5.28 | 5.66 | 4.70 |
| rusty_erasure | 3.73 | 4.42 | 4.84 | 4.68 | 4.41 |

## 8+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 12.1 | 15.5 | 16.9 | 17.1 | 17.2 |
| rusty_erasure | 9.58 | 11.6 | 11.8 | 12.1 | 12.0 |

## 8+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.3 | 15.4 | 16.7 | 17.0 | 17.1 |
| rusty_erasure | 9.18 | 11.3 | 11.8 | 12.0 | 12.0 |

## 8+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 1.44 | 1.92 | 2.10 | 2.14 | 2.14 |
| rusty_erasure | 1.15 | 1.42 | 1.48 | 1.51 | 1.50 |

## 8+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.48 | 4.92 | 5.26 | 5.64 | 4.70 |
| rusty_erasure | 3.70 | 4.41 | 4.83 | 4.67 | 4.41 |

## 10+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 12.3 | 15.3 | 16.8 | 17.1 | 17.2 |
| rusty_erasure | 10.0 | 11.7 | 11.9 | 12.1 | 12.1 |

## 10+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.6 | 15.1 | 16.5 | 16.8 | 17.1 |
| rusty_erasure | 9.47 | 11.5 | 11.9 | 12.0 | 12.0 |

## 10+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 1.18 | 1.51 | 1.66 | 1.70 | 1.72 |
| rusty_erasure | 0.95 | 1.15 | 1.18 | 1.20 | 1.20 |

## 10+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.47 | 4.92 | 5.26 | 5.66 | 4.70 |
| rusty_erasure | 3.64 | 4.40 | 4.82 | 4.68 | 4.39 |

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
