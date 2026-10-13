# X4 results: europa native

Every speed cell of one run of `shoal-spike-erasure`. The figure is the median of 3 measurements, each at least 200 ms of whole calls; encode and decode are counted by the stripe data (k × unit), rebuild by the chunk rebuilt and update by the bytes changed. `n/a` is a cell the candidate cannot run, with the reason below the table.

| Run | Host | CPU | Build | rse C | Governor | Core | Detected | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| europa native | europa | AMD Ryzen 9 7945HX with Radeon Graphics | `target-cpu=native` | `-march=native` | performance | 8 | ssse3, avx2, avx512f, avx512bw, avx512vl, gfni | 3 × 200 ms | 2026-10-03T18:24:31Z |

## 2+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 12.7 | 17.2 | 16.8 | 16.9 | 17.4 |
| rusty_erasure | 14.1 | 17.3 | 20.1 | 20.9 | 20.8 |
| isa-l | 13.6 | 16.4 | 19.5 | 20.7 | 20.9 |
| reed-solomon-erasure | 21.1 | 15.5 | 16.7 | 17.5 | 17.6 |
| reed-solomon-simd | 11.0 | 10.5 | 10.8 | 12.8 | 15.4 |
| raptorq | 0.29 | 0.28 | 0.27 | 0.31 | 0.32 |
| rlnc | 5.16 | 5.87 | 6.22 | 6.29 | 6.57 |
| rlnc, systematic | 5.44 | 7.49 | 7.67 | 8.07 | 8.13 |

## 2+1, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 4.06 | 5.55 | 6.14 | 7.18 | 7.34 |

## 2+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.9 | 15.3 | 17.2 | 16.9 | 17.6 |
| rusty_erasure | 12.6 | 15.8 | 19.3 | 20.7 | 21.0 |
| isa-l | 12.3 | 14.8 | 18.9 | 20.3 | 20.8 |
| reed-solomon-erasure | 10.8 | 13.0 | 15.6 | 17.0 | 17.6 |
| reed-solomon-simd | 0.06 | 0.25 | 0.86 | 2.18 | 3.63 |
| raptorq | 0.23 | 0.26 | 0.25 | 0.29 | 0.30 |
| rlnc | 4.05 | 5.56 | 6.17 | 7.23 | 7.38 |
| rlnc, systematic | 1.95 | 2.24 | 2.29 | 2.35 | 2.36 |

## 2+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 6.05 | 7.81 | 8.77 | 8.65 | 8.82 |
| rusty_erasure | 6.29 | 7.97 | 9.72 | 10.3 | 10.6 |
| isa-l | 6.12 | 7.44 | 9.48 | 10.2 | 10.4 |
| reed-solomon-erasure | 4.71 | 6.33 | 7.78 | 8.49 | 8.81 |
| reed-solomon-simd | 0.03 | 0.12 | 0.43 | 1.08 | 1.81 |
| raptorq | 0.11 | 0.13 | 0.13 | 0.15 | 0.15 |
| rlnc | 2.92 | 4.04 | 4.89 | 5.80 | 5.65 |
| rlnc, systematic | 0.98 | 1.12 | 1.15 | 1.18 | 1.18 |

## 2+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 8.62 | 10.1 | 12.9 | 12.9 | 10.9 |
| rusty_erasure | 7.73 | 7.35 | 7.78 | 8.52 | 10.4 |
| isa-l | 7.53 | 7.08 | 7.49 | 8.25 | 10.4 |
| reed-solomon-erasure | 7.61 | 7.25 | 7.61 | 8.36 | 10.3 |

## 4+2, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.3 | 17.2 | 20.2 | 21.0 | 21.2 |
| isa-l | 14.6 | 16.8 | 19.6 | 20.3 | 20.5 |
| reed-solomon-erasure | 15.6 | 12.0 | 12.7 | 12.6 | 14.3 |
| reed-solomon-simd | 7.99 | 8.81 | 9.45 | 11.8 | 11.8 |
| raptorq | 0.56 | 0.54 | 0.54 | 0.61 | 0.59 |
| rlnc | 3.88 | 5.05 | 4.76 | 4.94 | 4.89 |
| rlnc, systematic | 5.95 | 7.10 | 7.43 | 7.65 | 7.46 |

## 4+2, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 87.9 | 87.2 | 97.3 | 74.9 | 77.4 |
| isa-l | 31.2 | 34.0 | 35.1 | 34.1 | 32.9 |
| reed-solomon-erasure | 36.8 | 36.7 | 36.4 | 32.1 | 31.2 |
| reed-solomon-simd | 22.1 | 25.0 | 21.4 | 22.0 | 19.4 |
| raptorq | 0.76 | 0.56 | 0.56 | 0.62 | 0.61 |
| rlnc | 9.58 | 9.48 | 8.06 | 7.17 | 7.33 |
| rlnc, systematic | 11.7 | 12.8 | 11.0 | 9.71 | 9.99 |

## 4+2, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 4.08 | 4.89 | 5.28 | 5.90 | 5.80 |

## 4+2, decode-0, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 9.62 | 9.40 | 9.03 | 8.36 | 8.12 |

## 4+2, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 17.8 | 22.0 | 26.6 | 27.9 | 28.6 |
| isa-l | 16.8 | 21.2 | 25.9 | 27.4 | 28.0 |
| reed-solomon-erasure | 11.0 | 16.1 | 17.9 | 20.1 | 22.1 |
| reed-solomon-simd | 0.13 | 0.46 | 1.39 | 2.55 | 3.80 |
| raptorq | 0.43 | 0.49 | 0.51 | 0.57 | 0.55 |
| rlnc | 4.09 | 4.90 | 5.28 | 5.94 | 5.80 |
| rlnc, systematic | 3.15 | 3.64 | 3.74 | 3.78 | 3.82 |

## 4+2, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 83.0 | 94.5 | 112 | 91.1 | 92.6 |
| isa-l | 45.5 | 56.6 | 61.5 | 61.4 | 62.3 |
| reed-solomon-erasure | 42.7 | 60.8 | 69.2 | 60.2 | 62.3 |
| reed-solomon-simd | 0.13 | 0.48 | 1.50 | 2.78 | 4.27 |
| raptorq | 0.55 | 0.50 | 0.52 | 0.58 | 0.57 |
| rlnc | 9.74 | 9.41 | 9.02 | 8.36 | 8.12 |
| rlnc, systematic | 4.45 | 4.58 | 4.41 | 4.29 | 4.23 |

## 4+2, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 14.0 | 16.7 | 20.1 | 21.1 | 21.5 |
| isa-l | 13.5 | 15.9 | 19.1 | 20.1 | 20.5 |
| reed-solomon-erasure | 8.46 | 11.1 | 12.4 | 12.5 | 14.4 |
| reed-solomon-simd | 0.13 | 0.46 | 1.38 | 2.50 | 3.71 |
| raptorq | 0.43 | 0.50 | 0.51 | 0.57 | 0.55 |
| rlnc | 4.09 | 4.89 | 5.29 | 5.93 | 5.83 |
| rlnc, systematic | 2.70 | 3.08 | 3.20 | 3.29 | 3.24 |

## 4+2, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 67.9 | 81.1 | 94.4 | 77.0 | 79.4 |
| isa-l | 28.7 | 33.1 | 34.8 | 34.0 | 33.0 |
| reed-solomon-erasure | 29.0 | 34.4 | 35.7 | 31.4 | 31.3 |
| reed-solomon-simd | 0.13 | 0.48 | 1.50 | 2.76 | 4.23 |
| raptorq | 0.56 | 0.51 | 0.53 | 0.59 | 0.57 |
| rlnc | 9.52 | 9.40 | 8.98 | 8.35 | 8.12 |
| rlnc, systematic | 3.93 | 3.92 | 3.80 | 3.69 | 3.62 |

## 4+2, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.46 | 5.50 | 6.61 | 6.99 | 7.16 |
| isa-l | 4.20 | 5.28 | 6.46 | 6.73 | 7.00 |
| reed-solomon-erasure | 2.66 | 3.95 | 4.48 | 5.01 | 5.53 |
| reed-solomon-simd | 0.03 | 0.11 | 0.34 | 0.63 | 0.95 |
| raptorq | 0.11 | 0.12 | 0.13 | 0.14 | 0.14 |
| rlnc | 1.83 | 2.45 | 2.86 | 3.31 | 3.17 |
| rlnc, systematic | 0.79 | 0.91 | 0.93 | 0.95 | 0.96 |

## 4+2, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 20.5 | 23.6 | 28.0 | 22.7 | 23.1 |
| isa-l | 11.4 | 14.2 | 15.4 | 15.4 | 15.6 |
| reed-solomon-erasure | 9.47 | 14.5 | 17.1 | 14.9 | 15.6 |
| reed-solomon-simd | 0.03 | 0.12 | 0.37 | 0.69 | 1.07 |
| raptorq | 0.14 | 0.13 | 0.13 | 0.15 | 0.14 |
| rlnc | 4.87 | 5.47 | 5.24 | 4.73 | 4.62 |
| rlnc, systematic | 1.11 | 1.15 | 1.10 | 1.07 | 1.06 |

## 4+2, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.36 | 5.40 | 5.96 | 6.76 | 7.53 |
| isa-l | 4.29 | 5.25 | 5.89 | 6.79 | 7.24 |
| reed-solomon-erasure | 5.69 | 5.19 | 5.22 | 5.62 | 6.97 |

## 4+2, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 25.4 | 22.2 | 24.3 | 20.3 | 21.0 |
| isa-l | 22.2 | 22.2 | 23.7 | 20.5 | 20.7 |
| reed-solomon-erasure | 22.1 | 19.9 | 20.7 | 17.8 | 17.6 |

## 6+3, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 13.8 | 15.9 | 17.1 | 17.3 | 17.4 |
| isa-l | 12.9 | 15.1 | 15.9 | 16.0 | 15.9 |
| reed-solomon-erasure | 11.4 | 10.6 | 10.7 | 10.8 | 11.8 |
| reed-solomon-simd | 5.97 | 6.81 | 7.28 | 9.06 | 9.22 |
| raptorq | 0.77 | 1.00 | 0.77 | 0.72 | 0.86 |
| rlnc | 3.33 | 4.13 | 3.89 | 3.87 | 3.86 |
| rlnc, systematic | 5.53 | 6.84 | 6.66 | 7.04 | 6.49 |

## 6+3, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 3.78 | 4.22 | 4.65 | 4.79 | 4.77 |

## 6+3, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 19.8 | 24.7 | 30.3 | 31.5 | 32.2 |
| isa-l | 20.5 | 24.5 | 28.3 | 29.4 | 29.6 |
| reed-solomon-erasure | 15.1 | 18.0 | 18.9 | 21.4 | 27.2 |
| reed-solomon-simd | 0.18 | 0.59 | 1.50 | 2.08 | 2.32 |
| raptorq | 0.59 | 0.87 | 0.71 | 0.66 | 0.79 |
| rlnc | 3.78 | 4.22 | 4.65 | 4.81 | 4.77 |
| rlnc, systematic | 3.90 | 4.56 | 4.68 | 4.74 | 4.79 |

## 6+3, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.9 | 19.6 | 22.9 | 23.9 | 24.8 |
| isa-l | 16.4 | 19.5 | 20.7 | 20.8 | 21.0 |
| reed-solomon-erasure | 10.3 | 12.3 | 13.4 | 13.8 | 16.0 |
| reed-solomon-simd | 0.18 | 0.59 | 1.50 | 2.05 | 2.29 |
| raptorq | 0.60 | 0.88 | 0.72 | 0.67 | 0.80 |
| rlnc | 3.78 | 4.22 | 4.63 | 4.80 | 4.75 |
| rlnc, systematic | 3.30 | 3.70 | 3.91 | 3.95 | 3.94 |

## 6+3, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 13.3 | 15.6 | 16.9 | 17.4 | 17.5 |
| isa-l | 12.7 | 14.8 | 15.7 | 15.8 | 15.9 |
| reed-solomon-erasure | 8.36 | 10.1 | 10.5 | 10.7 | 11.8 |
| reed-solomon-simd | 0.18 | 0.59 | 1.49 | 2.04 | 2.26 |
| raptorq | 0.62 | 0.89 | 0.72 | 0.68 | 0.80 |
| rlnc | 3.79 | 4.24 | 4.66 | 4.80 | 4.79 |
| rlnc, systematic | 3.05 | 3.42 | 3.66 | 3.68 | 3.64 |

## 6+3, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.32 | 4.12 | 5.01 | 5.26 | 5.38 |
| isa-l | 3.41 | 4.08 | 4.71 | 4.88 | 4.94 |
| reed-solomon-erasure | 2.47 | 2.96 | 3.17 | 3.56 | 4.59 |
| reed-solomon-simd | 0.03 | 0.10 | 0.25 | 0.35 | 0.39 |
| raptorq | 0.10 | 0.14 | 0.12 | 0.11 | 0.13 |
| rlnc | 1.37 | 1.74 | 2.10 | 2.28 | 2.19 |
| rlnc, systematic | 0.66 | 0.76 | 0.78 | 0.79 | 0.80 |

## 6+3, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.67 | 4.40 | 5.02 | 5.68 | 5.87 |
| isa-l | 3.59 | 4.39 | 5.03 | 5.67 | 5.87 |
| reed-solomon-erasure | 4.76 | 4.00 | 3.92 | 4.22 | 5.13 |

## 8+3, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.6 | 17.9 | 19.3 | 19.4 | 19.6 |
| isa-l | 14.9 | 17.2 | 18.3 | 18.6 | 18.6 |
| reed-solomon-erasure | 12.4 | 11.3 | 11.5 | 11.6 | 12.6 |
| reed-solomon-simd | 7.78 | 9.07 | 9.17 | 11.4 | 11.5 |
| raptorq | 1.30 | 0.95 | 1.01 | 0.94 | 0.95 |
| rlnc | 3.17 | 3.84 | 3.57 | 3.45 | 3.45 |
| rlnc, systematic | 5.84 | 7.08 | 6.91 | 7.38 | 6.43 |

## 8+3, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 3.32 | 3.69 | 4.18 | 4.05 | 3.96 |

## 8+3, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 22.1 | 26.7 | 31.7 | 33.0 | 33.8 |
| isa-l | 22.1 | 26.1 | 28.1 | 28.5 | 28.5 |
| reed-solomon-erasure | 18.2 | 19.3 | 19.8 | 22.2 | 30.6 |
| reed-solomon-simd | 0.24 | 0.78 | 1.95 | 2.70 | 3.03 |
| raptorq | 0.94 | 0.84 | 0.92 | 0.86 | 0.87 |
| rlnc | 3.33 | 3.68 | 4.18 | 4.07 | 3.98 |
| rlnc, systematic | 4.36 | 5.27 | 5.39 | 5.45 | 5.30 |

## 8+3, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 18.2 | 21.3 | 23.9 | 24.4 | 24.6 |
| isa-l | 17.5 | 20.5 | 21.8 | 22.0 | 22.1 |
| reed-solomon-erasure | 12.8 | 13.1 | 14.0 | 14.8 | 17.2 |
| reed-solomon-simd | 0.24 | 0.78 | 1.95 | 2.66 | 2.99 |
| raptorq | 0.95 | 0.86 | 0.94 | 0.88 | 0.88 |
| rlnc | 3.32 | 3.70 | 4.18 | 4.06 | 3.97 |
| rlnc, systematic | 3.74 | 4.16 | 4.41 | 4.42 | 4.26 |

## 8+3, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.3 | 17.6 | 19.1 | 19.5 | 19.6 |
| isa-l | 14.2 | 16.6 | 17.8 | 18.3 | 18.3 |
| reed-solomon-erasure | 9.65 | 10.9 | 11.3 | 11.6 | 12.6 |
| reed-solomon-simd | 0.24 | 0.78 | 1.94 | 2.64 | 2.93 |
| raptorq | 0.96 | 0.87 | 0.94 | 0.88 | 0.89 |
| rlnc | 3.34 | 3.70 | 4.18 | 4.08 | 3.97 |
| rlnc, systematic | 3.43 | 3.83 | 4.07 | 4.07 | 3.90 |

## 8+3, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.77 | 3.34 | 3.95 | 4.11 | 4.22 |
| isa-l | 2.75 | 3.26 | 3.51 | 3.56 | 3.57 |
| reed-solomon-erasure | 2.23 | 2.39 | 2.47 | 2.78 | 3.87 |
| reed-solomon-simd | 0.03 | 0.10 | 0.24 | 0.34 | 0.38 |
| raptorq | 0.12 | 0.11 | 0.11 | 0.11 | 0.11 |
| rlnc | 1.08 | 1.37 | 1.71 | 1.75 | 1.60 |
| rlnc, systematic | 0.55 | 0.66 | 0.67 | 0.68 | 0.67 |

## 8+3, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.90 | 4.67 | 5.34 | 6.09 | 6.31 |
| isa-l | 3.81 | 4.62 | 5.35 | 6.09 | 6.34 |
| reed-solomon-erasure | 4.85 | 4.20 | 4.14 | 4.44 | 5.41 |

## 10+4, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 14.9 | 16.1 | 16.8 | 16.5 | 17.1 |
| isa-l | 13.3 | 14.4 | 13.8 | 14.1 | 12.5 |
| reed-solomon-erasure | 9.55 | 9.94 | 9.68 | 9.90 | 10.4 |
| reed-solomon-simd | 7.47 | 8.52 | 8.52 | 10.3 | 10.3 |
| raptorq | 1.55 | 1.15 | 1.35 | 1.14 | 1.32 |
| rlnc | 2.81 | 3.30 | 3.02 | 2.87 | 2.87 |
| rlnc, systematic | 5.24 | 6.41 | 6.27 | 6.23 | 4.60 |

## 10+4, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 31.3 | 34.7 | 34.8 | 34.7 | 34.4 |
| isa-l | 18.0 | 18.4 | 18.4 | 18.4 | 18.2 |
| reed-solomon-erasure | 21.4 | 19.8 | 17.9 | 15.0 | 15.4 |
| reed-solomon-simd | 14.2 | 13.6 | 13.1 | 14.4 | 12.3 |
| raptorq | 1.32 | 1.47 | 1.27 | 1.21 | 1.39 |
| rlnc | 4.26 | 4.85 | 3.88 | 3.39 | 3.04 |
| rlnc, systematic | 8.58 | 9.76 | 8.08 | 7.51 | 4.96 |

## 10+4, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 2.99 | 3.34 | 3.70 | 3.48 | 3.31 |

## 10+4, decode-0, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 4.73 | 4.87 | 4.64 | 4.31 | 3.31 |

## 10+4, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 21.6 | 27.0 | 32.0 | 33.6 | 34.5 |
| isa-l | 22.2 | 26.9 | 29.6 | 30.2 | 30.6 |
| reed-solomon-erasure | 20.4 | 19.9 | 20.1 | 22.4 | 32.4 |
| reed-solomon-simd | 0.30 | 0.94 | 2.30 | 3.03 | 3.31 |
| raptorq | 1.12 | 1.03 | 1.24 | 1.11 | 1.22 |
| rlnc | 2.99 | 3.34 | 3.72 | 3.48 | 3.31 |
| rlnc, systematic | 4.67 | 5.87 | 5.88 | 5.96 | 5.31 |

## 10+4, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 73.3 | 91.0 | 93.6 | 94.1 | 95.8 |
| isa-l | 49.7 | 52.6 | 56.2 | 57.6 | 57.9 |
| reed-solomon-erasure | 58.4 | 69.0 | 67.4 | 58.3 | 61.5 |
| reed-solomon-simd | 0.31 | 1.00 | 2.49 | 3.28 | 3.36 |
| raptorq | 1.02 | 1.09 | 1.13 | 1.18 | 1.28 |
| rlnc | 4.85 | 4.87 | 4.65 | 4.21 | 3.33 |
| rlnc, systematic | 7.06 | 7.94 | 7.24 | 7.19 | 5.13 |

## 10+4, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 19.4 | 22.5 | 25.0 | 25.6 | 25.9 |
| isa-l | 18.8 | 22.1 | 23.6 | 24.1 | 24.6 |
| reed-solomon-erasure | 14.0 | 13.7 | 14.3 | 15.3 | 18.0 |
| reed-solomon-simd | 0.30 | 0.94 | 2.29 | 3.00 | 3.26 |
| raptorq | 1.12 | 1.03 | 1.24 | 1.21 | 1.22 |
| rlnc | 2.99 | 3.34 | 3.71 | 3.49 | 3.32 |
| rlnc, systematic | 3.93 | 4.63 | 4.72 | 4.75 | 4.26 |

## 10+4, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 52.7 | 56.1 | 56.5 | 56.8 | 57.1 |
| isa-l | 31.1 | 34.1 | 34.9 | 34.9 | 34.9 |
| reed-solomon-erasure | 35.9 | 36.5 | 34.9 | 31.5 | 31.0 |
| reed-solomon-simd | 0.31 | 1.00 | 2.49 | 3.27 | 3.32 |
| raptorq | 1.03 | 1.31 | 1.29 | 1.18 | 1.29 |
| rlnc | 4.78 | 4.87 | 4.65 | 4.24 | 3.33 |
| rlnc, systematic | 5.88 | 6.03 | 5.64 | 5.53 | 4.11 |

## 10+4, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 16.3 | 19.0 | 20.4 | 20.9 | 21.3 |
| isa-l | 15.0 | 17.8 | 19.2 | 19.7 | 20.0 |
| reed-solomon-erasure | 10.0 | 11.3 | 11.7 | 12.0 | 13.2 |
| reed-solomon-simd | 0.30 | 0.94 | 2.28 | 2.98 | 3.26 |
| raptorq | 1.14 | 1.05 | 1.24 | 1.07 | 1.23 |
| rlnc | 2.99 | 3.34 | 3.72 | 3.49 | 3.32 |
| rlnc, systematic | 3.63 | 4.24 | 4.40 | 4.35 | 4.01 |

## 10+4, decode-3, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 33.8 | 39.8 | 40.3 | 39.9 | 39.9 |
| isa-l | 22.3 | 23.9 | 24.2 | 24.2 | 24.3 |
| reed-solomon-erasure | 24.7 | 25.4 | 23.4 | 20.2 | 20.6 |
| reed-solomon-simd | 0.31 | 1.00 | 2.48 | 3.27 | 3.30 |
| raptorq | 1.04 | 1.12 | 1.19 | 1.13 | 1.29 |
| rlnc | 4.80 | 4.77 | 4.64 | 4.20 | 3.35 |
| rlnc, systematic | 5.38 | 5.54 | 5.21 | 5.03 | 3.78 |

## 10+4, decode-4, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 14.1 | 16.1 | 16.7 | 16.7 | 16.9 |
| isa-l | 12.7 | 13.4 | 13.5 | 13.3 | 13.0 |
| reed-solomon-erasure | 8.54 | 9.67 | 9.49 | 9.92 | 10.4 |
| reed-solomon-simd | 0.30 | 0.94 | 2.27 | 2.96 | 3.23 |
| raptorq | 1.14 | 1.06 | 1.24 | 1.08 | 1.24 |
| rlnc | 2.98 | 3.34 | 3.73 | 3.49 | 3.31 |
| rlnc, systematic | 3.36 | 3.85 | 4.03 | 3.93 | 3.66 |

## 10+4, decode-4, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 28.2 | 34.0 | 34.2 | 34.7 | 34.6 |
| isa-l | 17.4 | 18.3 | 18.4 | 18.4 | 18.3 |
| reed-solomon-erasure | 19.1 | 19.5 | 17.7 | 14.9 | 15.5 |
| reed-solomon-simd | 0.31 | 0.99 | 2.48 | 3.26 | 3.29 |
| raptorq | 1.07 | 1.34 | 1.21 | 1.15 | 1.31 |
| rlnc | 4.76 | 4.85 | 4.63 | 4.21 | 3.33 |
| rlnc, systematic | 4.91 | 4.96 | 4.74 | 4.52 | 3.47 |

## 10+4, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.15 | 2.72 | 3.20 | 3.35 | 3.45 |
| isa-l | 2.22 | 2.70 | 2.96 | 3.01 | 3.05 |
| reed-solomon-erasure | 2.00 | 1.98 | 2.00 | 2.25 | 3.20 |
| reed-solomon-simd | 0.03 | 0.09 | 0.23 | 0.30 | 0.33 |
| raptorq | 0.11 | 0.10 | 0.12 | 0.11 | 0.12 |
| rlnc | 0.97 | 1.14 | 1.42 | 1.42 | 1.13 |
| rlnc, systematic | 0.47 | 0.59 | 0.59 | 0.60 | 0.53 |

## 10+4, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 7.38 | 9.11 | 9.25 | 9.25 | 9.54 |
| isa-l | 4.98 | 5.27 | 5.63 | 5.76 | 5.79 |
| reed-solomon-erasure | 5.42 | 6.76 | 6.69 | 5.83 | 6.15 |
| reed-solomon-simd | 0.03 | 0.10 | 0.25 | 0.33 | 0.33 |
| raptorq | 0.10 | 0.11 | 0.11 | 0.12 | 0.13 |
| rlnc | 2.06 | 2.31 | 1.93 | 1.89 | 1.22 |
| rlnc, systematic | 0.71 | 0.80 | 0.72 | 0.72 | 0.51 |

## 10+4, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.46 | 3.95 | 4.49 | 5.02 | 5.25 |
| isa-l | 3.34 | 3.85 | 4.43 | 4.98 | 5.28 |
| reed-solomon-erasure | 3.76 | 3.43 | 3.30 | 3.51 | 4.38 |

## 10+4, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 17.1 | 16.8 | 18.0 | 14.2 | 14.6 |
| isa-l | 15.4 | 16.1 | 17.2 | 15.0 | 15.1 |
| reed-solomon-erasure | 13.8 | 13.6 | 13.2 | 10.7 | 10.9 |

## 4+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 17.2 | 20.4 | 23.4 | 23.9 | 24.4 |
| rusty_erasure | 20.3 | 23.6 | 27.6 | 28.8 | 28.5 |

## 4+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 15.2 | 17.8 | 22.3 | 23.5 | 24.0 |
| rusty_erasure | 18.7 | 22.3 | 26.9 | 28.3 | 28.8 |

## 4+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 3.86 | 4.48 | 5.59 | 5.90 | 6.00 |
| rusty_erasure | 4.67 | 5.57 | 6.73 | 7.10 | 7.20 |

## 4+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 9.87 | 11.5 | 15.1 | 15.6 | 11.6 |
| rusty_erasure | 8.49 | 7.88 | 8.48 | 9.21 | 11.5 |

## 6+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 18.4 | 22.2 | 27.3 | 28.7 | 29.4 |
| rusty_erasure | 22.4 | 26.0 | 31.3 | 32.6 | 33.6 |

## 6+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 16.9 | 20.5 | 26.5 | 28.4 | 29.1 |
| rusty_erasure | 20.8 | 25.0 | 30.5 | 32.2 | 32.9 |

## 6+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 2.84 | 3.44 | 4.44 | 4.73 | 4.83 |
| rusty_erasure | 3.46 | 4.14 | 5.10 | 5.36 | 5.48 |

## 6+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.9 | 13.3 | 16.9 | 18.2 | 16.3 |
| rusty_erasure | 9.50 | 8.90 | 9.69 | 10.4 | 12.8 |

## 8+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 19.5 | 22.3 | 28.1 | 30.1 | 31.4 |
| rusty_erasure | 24.1 | 27.5 | 32.1 | 33.4 | 33.9 |

## 8+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 18.0 | 21.3 | 28.1 | 30.6 | 31.3 |
| rusty_erasure | 22.6 | 26.9 | 31.8 | 33.1 | 33.7 |

## 8+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 2.27 | 2.66 | 3.52 | 3.82 | 3.88 |
| rusty_erasure | 2.82 | 3.35 | 3.96 | 4.13 | 4.22 |

## 8+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 15.5 | 16.2 | 19.8 | 21.2 | 19.7 |
| rusty_erasure | 12.0 | 11.5 | 12.8 | 13.5 | 15.2 |

## 10+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 19.5 | 22.0 | 27.0 | 29.5 | 31.3 |
| rusty_erasure | 23.8 | 27.8 | 32.6 | 33.6 | 34.4 |

## 10+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 18.4 | 21.3 | 27.6 | 30.3 | 31.5 |
| rusty_erasure | 22.3 | 27.4 | 32.4 | 33.5 | 34.4 |

## 10+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 1.86 | 2.14 | 2.74 | 2.99 | 3.10 |
| rusty_erasure | 2.22 | 2.75 | 3.23 | 3.37 | 3.43 |

## 10+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 20.4 | 19.3 | 24.0 | 24.9 | 25.8 |
| rusty_erasure | 16.5 | 15.7 | 16.6 | 17.9 | 20.2 |

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
