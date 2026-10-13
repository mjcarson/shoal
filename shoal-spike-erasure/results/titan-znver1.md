# X4 results: titan znver1

Every speed cell of one run of `shoal-spike-erasure`. The figure is the median of 3 measurements, each at least 200 ms of whole calls; encode and decode are counted by the stripe data (k × unit), rebuild by the chunk rebuilt and update by the bytes changed. `n/a` is a cell the candidate cannot run, with the reason below the table.

| Run | Host | CPU | Build | rse C | Governor | Core | Detected | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| titan znver1 | titan | AMD Ryzen Embedded V1756B with Radeon Vega Gfx | `target-cpu=znver1` | `-march=haswell` | performance | 2 | ssse3, avx2 | 3 × 200 ms | 2026-10-03T18:09:17Z |

## 2+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 9.47 | 10.1 | 10.4 | 10.5 | 10.5 |
| rusty_erasure | 7.64 | 9.44 | 10.0 | 10.2 | 10.3 |
| isa-l | 5.58 | 7.78 | 8.69 | 8.86 | 8.80 |
| reed-solomon-erasure | 7.25 | 7.96 | 8.33 | 8.24 | 8.18 |
| reed-solomon-simd | 5.05 | 6.35 | 6.70 | 6.64 | 5.75 |
| raptorq | 0.08 | 0.08 | 0.08 | 0.08 | 0.08 |
| rlnc | 1.78 | 1.92 | 1.96 | 2.14 | 1.87 |
| rlnc, systematic | 2.26 | 2.56 | 2.76 | 2.77 | 1.41 |

## 2+1, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 2.06 | 2.28 | 2.53 | 2.58 | 1.61 |

## 2+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 8.55 | 9.89 | 10.4 | 10.5 | 10.6 |
| rusty_erasure | 6.59 | 9.13 | 9.94 | 10.2 | 10.3 |
| isa-l | 5.19 | 7.51 | 8.68 | 8.91 | 8.78 |
| reed-solomon-erasure | 5.38 | 7.61 | 8.23 | 8.27 | 8.22 |
| reed-solomon-simd | 0.02 | 0.10 | 0.33 | 0.85 | 0.95 |
| raptorq | 0.07 | 0.08 | 0.08 | 0.08 | 0.08 |
| rlnc | 2.06 | 2.28 | 2.54 | 2.61 | 1.62 |
| rlnc, systematic | 1.02 | 1.14 | 1.15 | 1.13 | 0.79 |

## 2+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.48 | 5.00 | 5.23 | 5.27 | 5.30 |
| rusty_erasure | 3.31 | 4.56 | 4.97 | 5.09 | 5.14 |
| isa-l | 2.60 | 3.78 | 4.29 | 4.49 | 4.53 |
| reed-solomon-erasure | 2.50 | 3.66 | 4.08 | 4.13 | 4.11 |
| reed-solomon-simd | 0.01 | 0.05 | 0.16 | 0.42 | 0.48 |
| raptorq | 0.03 | 0.04 | 0.04 | 0.04 | 0.04 |
| rlnc | 1.41 | 1.84 | 2.01 | 2.09 | 1.16 |
| rlnc, systematic | 0.51 | 0.57 | 0.58 | 0.57 | 0.40 |

## 2+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.47 | 5.06 | 5.28 | 5.65 | 4.66 |
| rusty_erasure | 3.84 | 4.53 | 4.85 | 4.62 | 4.39 |
| isa-l | 3.67 | 4.47 | 4.85 | 4.61 | 4.43 |
| reed-solomon-erasure | 3.93 | 4.50 | 4.75 | 4.60 | 4.42 |

## 4+2, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.93 | 7.35 | 7.62 | 7.77 | 7.79 |
| isa-l | 4.65 | 4.85 | 4.98 | 5.03 | 5.03 |
| reed-solomon-erasure | 5.57 | 5.73 | 5.63 | 5.81 | 4.61 |
| reed-solomon-simd | 4.47 | 4.62 | 4.75 | 4.74 | 2.92 |
| raptorq | 0.16 | 0.16 | 0.16 | 0.16 | 0.16 |
| rlnc | 1.45 | 1.45 | 1.49 | 1.57 | 1.22 |
| rlnc, systematic | 2.07 | 2.30 | 2.44 | 2.08 | 1.36 |

## 4+2, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 10.3 | 9.46 | 9.87 | 9.88 | 8.30 |
| isa-l | 5.61 | 5.27 | 5.37 | 5.33 | 5.11 |
| reed-solomon-erasure | 7.45 | 7.83 | 8.11 | 8.12 | 5.87 |
| reed-solomon-simd | 7.13 | 7.46 | 7.51 | 7.21 | 2.87 |
| raptorq | 0.16 | 0.16 | 0.16 | 0.16 | 0.16 |
| rlnc | 2.03 | 2.07 | 2.10 | 1.91 | 1.21 |
| rlnc, systematic | 2.62 | 2.89 | 2.96 | 2.15 | 1.39 |

## 4+2, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 1.55 | 1.73 | 1.77 | 1.73 | 1.20 |

## 4+2, decode-0, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 2.11 | 2.25 | 2.30 | 2.00 | 1.20 |

## 4+2, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 7.41 | 10.4 | 12.1 | 12.6 | 12.7 |
| isa-l | 6.32 | 8.08 | 8.94 | 9.23 | 9.22 |
| reed-solomon-erasure | 7.19 | 9.38 | 10.1 | 10.2 | 10.1 |
| reed-solomon-simd | 0.05 | 0.18 | 0.51 | 0.93 | 0.57 |
| raptorq | 0.13 | 0.15 | 0.15 | 0.15 | 0.15 |
| rlnc | 1.55 | 1.73 | 1.77 | 1.73 | 1.20 |
| rlnc, systematic | 1.55 | 1.75 | 1.77 | 1.56 | 1.12 |

## 4+2, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 13.3 | 15.8 | 16.1 | 16.4 | 15.0 |
| isa-l | 8.66 | 9.87 | 10.1 | 10.4 | 9.84 |
| reed-solomon-erasure | 11.1 | 14.4 | 15.8 | 16.1 | 13.0 |
| reed-solomon-simd | 0.05 | 0.18 | 0.52 | 0.96 | 0.57 |
| raptorq | 0.13 | 0.15 | 0.15 | 0.15 | 0.15 |
| rlnc | 2.11 | 2.25 | 2.30 | 2.01 | 1.20 |
| rlnc, systematic | 1.85 | 1.96 | 1.96 | 1.62 | 1.14 |

## 4+2, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.79 | 7.24 | 7.62 | 7.82 | 7.85 |
| isa-l | 4.29 | 4.82 | 4.94 | 5.02 | 5.01 |
| reed-solomon-erasure | 4.89 | 5.38 | 5.60 | 5.81 | 4.62 |
| reed-solomon-simd | 0.05 | 0.17 | 0.50 | 0.91 | 0.56 |
| raptorq | 0.13 | 0.15 | 0.15 | 0.15 | 0.15 |
| rlnc | 1.56 | 1.72 | 1.77 | 1.73 | 1.20 |
| rlnc, systematic | 1.19 | 1.32 | 1.35 | 1.20 | 0.88 |

## 4+2, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 9.24 | 9.59 | 9.80 | 9.86 | 8.16 |
| isa-l | 5.34 | 5.29 | 5.36 | 5.33 | 5.09 |
| reed-solomon-erasure | 6.62 | 7.62 | 8.04 | 8.11 | 5.74 |
| reed-solomon-simd | 0.05 | 0.18 | 0.52 | 0.95 | 0.56 |
| raptorq | 0.13 | 0.15 | 0.15 | 0.15 | 0.15 |
| rlnc | 2.11 | 2.25 | 2.30 | 1.99 | 1.20 |
| rlnc, systematic | 1.39 | 1.49 | 1.50 | 1.24 | 0.88 |

## 4+2, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.87 | 2.60 | 3.03 | 3.15 | 3.17 |
| isa-l | 1.59 | 1.96 | 2.24 | 2.32 | 2.33 |
| reed-solomon-erasure | 1.71 | 2.30 | 2.51 | 2.55 | 2.53 |
| reed-solomon-simd | 0.01 | 0.04 | 0.13 | 0.23 | 0.14 |
| raptorq | 0.03 | 0.04 | 0.04 | 0.04 | 0.04 |
| rlnc | 0.83 | 1.07 | 1.17 | 1.15 | 0.58 |
| rlnc, systematic | 0.39 | 0.44 | 0.44 | 0.39 | 0.28 |

## 4+2, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.38 | 3.99 | 4.03 | 4.11 | 3.74 |
| isa-l | 2.13 | 2.47 | 2.53 | 2.58 | 2.47 |
| reed-solomon-erasure | 2.57 | 3.52 | 3.93 | 4.03 | 3.26 |
| reed-solomon-simd | 0.01 | 0.04 | 0.13 | 0.24 | 0.14 |
| raptorq | 0.03 | 0.04 | 0.04 | 0.04 | 0.04 |
| rlnc | 1.43 | 1.60 | 1.64 | 1.47 | 0.59 |
| rlnc, systematic | 0.46 | 0.49 | 0.49 | 0.41 | 0.28 |

## 4+2, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.54 | 2.99 | 3.25 | 3.15 | 2.98 |
| isa-l | 2.59 | 3.08 | 3.19 | 3.07 | 2.93 |
| reed-solomon-erasure | 2.53 | 2.75 | 2.85 | 2.77 | 2.65 |

## 4+2, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.35 | 6.25 | 6.67 | 6.60 | 3.65 |
| isa-l | 5.67 | 6.48 | 6.86 | 6.74 | 3.67 |
| reed-solomon-erasure | 5.01 | 5.58 | 5.86 | 5.82 | 3.22 |

## 6+3, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.42 | 6.32 | 6.43 | 6.58 | 6.60 |
| isa-l | 3.58 | 3.67 | 3.71 | 3.72 | 3.73 |
| reed-solomon-erasure | 4.28 | 4.20 | 4.18 | 4.30 | 2.69 |
| reed-solomon-simd | 2.69 | 2.80 | 2.88 | 2.71 | 1.50 |
| raptorq | 0.22 | 0.23 | 0.23 | 0.23 | 0.23 |
| rlnc | 1.00 | 1.00 | 1.04 | 1.04 | 0.87 |
| rlnc, systematic | 1.84 | 2.01 | 2.11 | 1.53 | 1.29 |

## 6+3, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 1.22 | 1.37 | 1.42 | 1.23 | 0.93 |

## 6+3, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 8.83 | 11.1 | 11.8 | 12.1 | 12.1 |
| isa-l | 6.70 | 7.58 | 7.91 | 8.03 | 8.01 |
| reed-solomon-erasure | 8.31 | 10.2 | 11.0 | 11.0 | 10.8 |
| reed-solomon-simd | 0.07 | 0.23 | 0.51 | 0.59 | 0.38 |
| raptorq | 0.18 | 0.21 | 0.22 | 0.22 | 0.22 |
| rlnc | 1.22 | 1.37 | 1.42 | 1.25 | 0.93 |
| rlnc, systematic | 1.87 | 2.12 | 2.17 | 1.62 | 1.34 |

## 6+3, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 6.19 | 7.97 | 8.41 | 8.62 | 8.64 |
| isa-l | 4.54 | 5.04 | 5.16 | 5.22 | 5.22 |
| reed-solomon-erasure | 5.24 | 5.91 | 6.03 | 6.22 | 4.93 |
| reed-solomon-simd | 0.07 | 0.23 | 0.51 | 0.59 | 0.37 |
| raptorq | 0.18 | 0.21 | 0.22 | 0.22 | 0.22 |
| rlnc | 1.22 | 1.36 | 1.42 | 1.27 | 0.93 |
| rlnc, systematic | 1.35 | 1.51 | 1.52 | 1.20 | 0.98 |

## 6+3, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.03 | 6.26 | 6.47 | 6.59 | 6.64 |
| isa-l | 3.42 | 3.63 | 3.70 | 3.73 | 3.72 |
| reed-solomon-erasure | 3.99 | 4.09 | 4.17 | 4.28 | 2.69 |
| reed-solomon-simd | 0.07 | 0.22 | 0.51 | 0.58 | 0.37 |
| raptorq | 0.18 | 0.21 | 0.22 | 0.22 | 0.22 |
| rlnc | 1.23 | 1.37 | 1.42 | 1.28 | 0.93 |
| rlnc, systematic | 1.21 | 1.34 | 1.37 | 1.10 | 0.89 |

## 6+3, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.46 | 1.84 | 1.96 | 2.01 | 2.01 |
| isa-l | 1.12 | 1.26 | 1.31 | 1.34 | 1.34 |
| reed-solomon-erasure | 1.32 | 1.68 | 1.82 | 1.83 | 1.80 |
| reed-solomon-simd | 0.01 | 0.04 | 0.09 | 0.10 | 0.06 |
| raptorq | 0.03 | 0.03 | 0.04 | 0.04 | 0.04 |
| rlnc | 0.56 | 0.71 | 0.77 | 0.58 | 0.39 |
| rlnc, systematic | 0.31 | 0.35 | 0.36 | 0.27 | 0.22 |

## 6+3, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.99 | 2.34 | 2.47 | 2.34 | 2.26 |
| isa-l | 1.84 | 2.19 | 2.24 | 2.19 | 2.16 |
| reed-solomon-erasure | 1.84 | 1.96 | 2.01 | 1.98 | 1.90 |

## 8+3, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 6.02 | 6.59 | 6.63 | 6.72 | 6.82 |
| isa-l | 3.71 | 3.70 | 3.73 | 3.75 | 3.75 |
| reed-solomon-erasure | 4.47 | 4.37 | 4.34 | 4.44 | 2.74 |
| reed-solomon-simd | 3.16 | 3.62 | 3.69 | 3.50 | 1.94 |
| raptorq | 0.30 | 0.30 | 0.30 | 0.30 | 0.30 |
| rlnc | 1.02 | 0.99 | 1.05 | 1.01 | 0.81 |
| rlnc, systematic | 1.93 | 2.12 | 2.22 | 1.42 | 1.36 |

## 8+3, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 1.02 | 1.12 | 1.17 | 0.93 | 0.76 |

## 8+3, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 9.10 | 11.3 | 11.8 | 12.0 | 12.0 |
| isa-l | 7.15 | 8.09 | 8.40 | 8.56 | 8.52 |
| reed-solomon-erasure | 9.04 | 10.7 | 11.4 | 11.4 | 11.2 |
| reed-solomon-simd | 0.09 | 0.30 | 0.67 | 0.78 | 0.49 |
| raptorq | 0.23 | 0.28 | 0.28 | 0.28 | 0.28 |
| rlnc | 1.02 | 1.12 | 1.17 | 0.96 | 0.76 |
| rlnc, systematic | 2.08 | 2.37 | 2.44 | 1.51 | 1.47 |

## 8+3, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 6.80 | 8.46 | 8.81 | 8.99 | 8.99 |
| isa-l | 4.69 | 5.13 | 5.23 | 5.29 | 5.29 |
| reed-solomon-erasure | 5.62 | 6.17 | 6.26 | 6.40 | 5.08 |
| reed-solomon-simd | 0.09 | 0.30 | 0.67 | 0.77 | 0.48 |
| raptorq | 0.24 | 0.28 | 0.29 | 0.29 | 0.29 |
| rlnc | 1.02 | 1.12 | 1.17 | 0.97 | 0.76 |
| rlnc, systematic | 1.51 | 1.68 | 1.71 | 1.17 | 1.05 |

## 8+3, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.51 | 6.51 | 6.72 | 6.76 | 6.78 |
| isa-l | 3.53 | 3.67 | 3.73 | 3.75 | 3.75 |
| reed-solomon-erasure | 4.16 | 4.27 | 4.34 | 4.41 | 2.72 |
| reed-solomon-simd | 0.09 | 0.30 | 0.67 | 0.76 | 0.48 |
| raptorq | 0.24 | 0.28 | 0.29 | 0.29 | 0.29 |
| rlnc | 1.02 | 1.12 | 1.17 | 0.97 | 0.76 |
| rlnc, systematic | 1.30 | 1.44 | 1.48 | 1.05 | 0.92 |

## 8+3, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.14 | 1.42 | 1.47 | 1.50 | 1.50 |
| isa-l | 0.89 | 1.01 | 1.05 | 1.07 | 1.07 |
| reed-solomon-erasure | 1.09 | 1.32 | 1.43 | 1.42 | 1.40 |
| reed-solomon-simd | 0.01 | 0.04 | 0.08 | 0.10 | 0.06 |
| raptorq | 0.03 | 0.03 | 0.04 | 0.04 | 0.04 |
| rlnc | 0.46 | 0.58 | 0.63 | 0.36 | 0.30 |
| rlnc, systematic | 0.26 | 0.30 | 0.30 | 0.19 | 0.18 |

## 8+3, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.00 | 2.32 | 2.46 | 2.36 | 2.22 |
| isa-l | 1.86 | 2.20 | 2.34 | 2.19 | 2.15 |
| reed-solomon-erasure | 1.83 | 1.96 | 2.03 | 1.98 | 1.90 |

## 10+4, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.30 | 5.19 | 5.29 | 5.35 | 5.35 |
| isa-l | 3.45 | 3.32 | 3.34 | 3.39 | 3.40 |
| reed-solomon-erasure | 3.43 | 3.34 | 3.35 | 3.37 | 1.81 |
| reed-solomon-simd | 2.83 | 3.19 | 3.24 | 2.64 | 1.61 |
| raptorq | 0.36 | 0.37 | 0.37 | 0.37 | 0.37 |
| rlnc | 0.73 | 0.72 | 0.76 | 0.71 | 0.62 |
| rlnc, systematic | 1.61 | 1.81 | 1.87 | 1.20 | 1.22 |

## 10+4, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.98 | 5.96 | 5.95 | 5.92 | 5.46 |
| isa-l | 3.48 | 3.48 | 3.48 | 3.49 | 3.41 |
| reed-solomon-erasure | 3.96 | 3.94 | 3.96 | 3.79 | 1.84 |
| reed-solomon-simd | 4.18 | 4.23 | 4.26 | 2.57 | 1.62 |
| raptorq | 0.37 | 0.38 | 0.38 | 0.37 | 0.37 |
| rlnc | 0.84 | 0.83 | 0.84 | 0.70 | 0.61 |
| rlnc, systematic | 1.90 | 2.09 | 2.11 | 1.20 | 1.21 |

## 10+4, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 0.87 | 0.95 | 0.99 | 0.76 | 0.64 |

## 10+4, decode-0, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 1.03 | 1.11 | 1.11 | 0.77 | 0.64 |

## 10+4, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 9.49 | 11.4 | 11.8 | 12.0 | 12.0 |
| isa-l | 7.28 | 8.21 | 8.51 | 8.62 | 8.62 |
| reed-solomon-erasure | 9.54 | 11.0 | 11.7 | 11.6 | 11.4 |
| reed-solomon-simd | 0.11 | 0.36 | 0.77 | 0.84 | 0.54 |
| raptorq | 0.29 | 0.34 | 0.35 | 0.35 | 0.35 |
| rlnc | 0.87 | 0.96 | 0.99 | 0.79 | 0.64 |
| rlnc, systematic | 2.10 | 2.53 | 2.61 | 1.46 | 1.55 |

## 10+4, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 14.0 | 15.5 | 15.8 | 15.7 | 12.0 |
| isa-l | 8.73 | 9.17 | 9.24 | 9.29 | 8.60 |
| reed-solomon-erasure | 13.3 | 14.9 | 15.6 | 15.1 | 12.4 |
| reed-solomon-simd | 0.11 | 0.36 | 0.80 | 0.84 | 0.54 |
| raptorq | 0.29 | 0.35 | 0.35 | 0.35 | 0.35 |
| rlnc | 1.03 | 1.10 | 1.11 | 0.78 | 0.64 |
| rlnc, systematic | 2.65 | 2.92 | 2.94 | 1.47 | 1.55 |

## 10+4, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 7.26 | 8.74 | 8.94 | 9.03 | 9.06 |
| isa-l | 4.69 | 5.15 | 5.25 | 5.30 | 5.31 |
| reed-solomon-erasure | 5.85 | 6.35 | 6.38 | 6.50 | 5.20 |
| reed-solomon-simd | 0.11 | 0.36 | 0.76 | 0.83 | 0.54 |
| raptorq | 0.29 | 0.34 | 0.35 | 0.35 | 0.35 |
| rlnc | 0.87 | 0.96 | 0.99 | 0.79 | 0.64 |
| rlnc, systematic | 1.47 | 1.71 | 1.74 | 1.10 | 1.09 |

## 10+4, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 9.74 | 10.3 | 10.6 | 10.6 | 9.08 |
| isa-l | 5.37 | 5.52 | 5.52 | 5.53 | 5.31 |
| reed-solomon-erasure | 7.30 | 7.66 | 7.88 | 7.55 | 5.67 |
| reed-solomon-simd | 0.11 | 0.36 | 0.80 | 0.83 | 0.54 |
| raptorq | 0.30 | 0.35 | 0.36 | 0.35 | 0.35 |
| rlnc | 1.03 | 1.11 | 1.12 | 0.79 | 0.62 |
| rlnc, systematic | 1.80 | 1.89 | 1.90 | 1.10 | 1.08 |

## 10+4, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.98 | 6.61 | 6.71 | 6.83 | 6.86 |
| isa-l | 3.63 | 3.69 | 3.68 | 3.72 | 3.73 |
| reed-solomon-erasure | 4.23 | 4.33 | 4.41 | 4.50 | 2.74 |
| reed-solomon-simd | 0.11 | 0.36 | 0.76 | 0.82 | 0.53 |
| raptorq | 0.29 | 0.34 | 0.35 | 0.35 | 0.35 |
| rlnc | 0.87 | 0.96 | 0.99 | 0.79 | 0.64 |
| rlnc, systematic | 1.31 | 1.49 | 1.53 | 1.02 | 0.94 |

## 10+4, decode-3, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 7.32 | 7.68 | 7.76 | 7.65 | 6.87 |
| isa-l | 3.85 | 3.88 | 3.88 | 3.87 | 3.74 |
| reed-solomon-erasure | 5.01 | 5.16 | 5.26 | 5.00 | 2.84 |
| reed-solomon-simd | 0.11 | 0.36 | 0.80 | 0.82 | 0.53 |
| raptorq | 0.30 | 0.35 | 0.36 | 0.35 | 0.35 |
| rlnc | 1.04 | 1.11 | 1.12 | 0.79 | 0.63 |
| rlnc, systematic | 1.56 | 1.66 | 1.67 | 1.02 | 0.94 |

## 10+4, decode-4, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.87 | 5.22 | 5.33 | 5.42 | 5.50 |
| isa-l | 3.24 | 3.25 | 3.33 | 3.37 | 3.39 |
| reed-solomon-erasure | 3.28 | 3.30 | 3.35 | 3.36 | 1.81 |
| reed-solomon-simd | 0.11 | 0.35 | 0.76 | 0.81 | 0.53 |
| raptorq | 0.30 | 0.34 | 0.35 | 0.35 | 0.36 |
| rlnc | 0.87 | 0.96 | 1.00 | 0.79 | 0.64 |
| rlnc, systematic | 1.16 | 1.31 | 1.35 | 0.93 | 0.81 |

## 10+4, decode-4, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 5.63 | 5.91 | 5.93 | 5.82 | 5.48 |
| isa-l | 3.37 | 3.47 | 3.48 | 3.48 | 3.40 |
| reed-solomon-erasure | 3.82 | 3.90 | 3.95 | 3.78 | 1.84 |
| reed-solomon-simd | 0.11 | 0.36 | 0.80 | 0.81 | 0.53 |
| raptorq | 0.30 | 0.35 | 0.36 | 0.36 | 0.36 |
| rlnc | 1.03 | 1.11 | 1.12 | 0.79 | 0.64 |
| rlnc, systematic | 1.36 | 1.45 | 1.46 | 0.93 | 0.81 |

## 10+4, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 0.95 | 1.14 | 1.18 | 1.20 | 1.20 |
| isa-l | 0.73 | 0.82 | 0.85 | 0.86 | 0.86 |
| reed-solomon-erasure | 0.92 | 1.09 | 1.16 | 1.16 | 1.14 |
| reed-solomon-simd | 0.01 | 0.04 | 0.08 | 0.08 | 0.05 |
| raptorq | 0.03 | 0.03 | 0.03 | 0.03 | 0.03 |
| rlnc | 0.36 | 0.44 | 0.47 | 0.25 | 0.24 |
| rlnc, systematic | 0.21 | 0.25 | 0.26 | 0.15 | 0.15 |

## 10+4, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.40 | 1.55 | 1.58 | 1.57 | 1.20 |
| isa-l | 0.87 | 0.92 | 0.92 | 0.93 | 0.86 |
| reed-solomon-erasure | 1.27 | 1.48 | 1.55 | 1.51 | 1.24 |
| reed-solomon-simd | 0.01 | 0.04 | 0.08 | 0.08 | 0.05 |
| raptorq | 0.03 | 0.03 | 0.04 | 0.03 | 0.03 |
| rlnc | 0.56 | 0.60 | 0.63 | 0.25 | 0.24 |
| rlnc, systematic | 0.26 | 0.29 | 0.29 | 0.15 | 0.15 |

## 10+4, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 1.60 | 1.86 | 1.94 | 1.90 | 1.75 |
| isa-l | 1.46 | 1.76 | 1.84 | 1.82 | 1.71 |
| reed-solomon-erasure | 1.44 | 1.53 | 1.56 | 1.55 | 1.50 |

## 10+4, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.55 | 4.11 | 4.26 | 4.26 | 1.81 |
| isa-l | 2.72 | 3.12 | 3.21 | 3.16 | 1.67 |
| reed-solomon-erasure | 3.00 | 3.24 | 3.34 | 3.32 | 1.52 |

## 4+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.2 | 13.7 | 14.5 | 14.7 | 14.7 |
| rusty_erasure | 8.81 | 11.5 | 12.5 | 12.7 | 12.8 |

## 4+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 10.0 | 13.4 | 14.3 | 14.7 | 14.7 |
| rusty_erasure | 8.44 | 10.9 | 12.4 | 12.8 | 12.8 |

## 4+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 2.56 | 3.37 | 3.59 | 3.67 | 3.68 |
| rusty_erasure | 2.13 | 2.72 | 3.08 | 3.19 | 3.20 |

## 4+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.51 | 5.05 | 5.27 | 5.67 | 4.67 |
| rusty_erasure | 3.79 | 4.44 | 4.84 | 4.61 | 4.44 |

## 6+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.5 | 15.2 | 16.3 | 16.5 | 16.6 |
| rusty_erasure | 9.55 | 11.5 | 11.8 | 12.1 | 12.1 |

## 6+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 10.8 | 15.0 | 16.2 | 16.5 | 16.6 |
| rusty_erasure | 8.89 | 11.1 | 11.8 | 12.1 | 12.1 |

## 6+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 1.83 | 2.50 | 2.70 | 2.75 | 2.77 |
| rusty_erasure | 1.48 | 1.86 | 1.96 | 2.01 | 2.02 |

## 6+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.46 | 4.96 | 5.27 | 5.66 | 4.68 |
| rusty_erasure | 3.74 | 4.37 | 4.87 | 4.62 | 4.44 |

## 8+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.8 | 15.3 | 16.9 | 17.1 | 17.2 |
| rusty_erasure | 9.75 | 11.7 | 11.8 | 12.0 | 12.0 |

## 8+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.4 | 15.4 | 16.7 | 17.1 | 17.2 |
| rusty_erasure | 9.32 | 11.4 | 11.7 | 11.9 | 12.0 |

## 8+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 1.44 | 1.92 | 2.09 | 2.14 | 2.15 |
| rusty_erasure | 1.17 | 1.42 | 1.47 | 1.49 | 1.49 |

## 8+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.50 | 4.95 | 5.26 | 5.67 | 4.66 |
| rusty_erasure | 3.75 | 4.36 | 4.86 | 4.61 | 4.43 |

## 10+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 12.3 | 15.2 | 16.7 | 17.1 | 17.2 |
| rusty_erasure | 10.2 | 11.7 | 11.9 | 12.0 | 12.0 |

## 10+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.7 | 15.2 | 16.7 | 17.0 | 17.1 |
| rusty_erasure | 9.56 | 11.5 | 11.8 | 12.0 | 12.0 |

## 10+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 1.19 | 1.52 | 1.67 | 1.70 | 1.71 |
| rusty_erasure | 0.96 | 1.15 | 1.18 | 1.20 | 1.20 |

## 10+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.44 | 4.92 | 5.26 | 5.66 | 4.67 |
| rusty_erasure | 3.74 | 4.35 | 4.86 | 4.60 | 4.44 |

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
