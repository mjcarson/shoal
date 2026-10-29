# X4 results: europa x86-64-v4

Every speed cell of one run of `shoal-spike-erasure`. The figure is the median of 3 measurements, each at least 200 ms of whole calls; encode and decode are counted by the stripe data (k × unit), rebuild by the chunk rebuilt and update by the bytes changed. `n/a` is a cell the candidate cannot run, with the reason below the table.

| Run | Host | CPU | Build | rse C | Governor | Core | Detected | Runs × budget | Date |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| europa x86-64-v4 | europa | AMD Ryzen 9 7945HX with Radeon Graphics | `target-cpu=x86-64-v4` | `-march=x86-64-v4` | performance | 8 | ssse3, avx2, avx512f, avx512bw, avx512vl, gfni | 3 × 200 ms | 2026-10-03T18:39:45Z |

## 2+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 13.6 | 18.3 | 17.9 | 18.5 | 18.9 |
| rusty_erasure | 14.2 | 17.4 | 20.2 | 21.0 | 20.8 |
| isa-l | 13.5 | 16.2 | 19.4 | 20.5 | 20.8 |
| reed-solomon-erasure | 21.9 | 15.5 | 16.7 | 17.5 | 17.6 |
| reed-solomon-simd | 11.4 | 10.5 | 10.9 | 12.5 | 15.6 |
| raptorq | 0.36 | 0.27 | 0.28 | 0.32 | 0.31 |
| rlnc | 4.95 | 5.92 | 6.15 | 6.39 | 6.61 |
| rlnc, systematic | 5.41 | 7.23 | 7.69 | 8.13 | 8.19 |

## 2+1, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 3.91 | 5.49 | 6.11 | 7.11 | 7.34 |

## 2+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.9 | 16.2 | 17.3 | 18.2 | 18.8 |
| rusty_erasure | 12.7 | 15.6 | 19.5 | 20.8 | 21.1 |
| isa-l | 12.2 | 14.9 | 19.0 | 20.4 | 20.8 |
| reed-solomon-erasure | 10.3 | 12.3 | 15.6 | 16.9 | 17.5 |
| reed-solomon-simd | 0.08 | 0.29 | 0.98 | 2.40 | 3.72 |
| raptorq | 0.27 | 0.25 | 0.27 | 0.30 | 0.29 |
| rlnc | 3.90 | 5.51 | 6.12 | 7.18 | 7.41 |
| rlnc, systematic | 1.95 | 2.21 | 2.29 | 2.35 | 2.37 |

## 2+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 6.09 | 8.57 | 8.60 | 9.14 | 9.41 |
| rusty_erasure | 6.33 | 7.85 | 9.65 | 10.4 | 10.6 |
| isa-l | 6.08 | 7.49 | 9.46 | 10.2 | 10.4 |
| reed-solomon-erasure | 4.70 | 6.08 | 7.80 | 8.47 | 8.80 |
| reed-solomon-simd | 0.04 | 0.14 | 0.49 | 1.20 | 1.86 |
| raptorq | 0.13 | 0.12 | 0.13 | 0.15 | 0.15 |
| rlnc | 2.89 | 4.02 | 4.79 | 5.80 | 5.73 |
| rlnc, systematic | 0.98 | 1.11 | 1.15 | 1.18 | 1.18 |

## 2+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 8.75 | 10.8 | 13.3 | 13.1 | 10.9 |
| rusty_erasure | 7.88 | 7.49 | 7.87 | 8.73 | 10.9 |
| isa-l | 7.77 | 7.30 | 7.63 | 8.34 | 10.7 |
| reed-solomon-erasure | 8.17 | 7.36 | 7.74 | 8.59 | 10.8 |

## 4+2, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.1 | 17.6 | 20.3 | 21.1 | 21.2 |
| isa-l | 14.5 | 17.0 | 19.6 | 20.3 | 20.6 |
| reed-solomon-erasure | 15.1 | 12.0 | 12.6 | 12.6 | 14.4 |
| reed-solomon-simd | 7.88 | 9.12 | 9.83 | 11.8 | 11.6 |
| raptorq | 0.68 | 0.51 | 0.51 | 0.58 | 0.60 |
| rlnc | 3.84 | 4.94 | 4.77 | 4.87 | 4.87 |
| rlnc, systematic | 5.75 | 7.29 | 7.40 | 7.72 | 7.47 |

## 4+2, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 88.5 | 88.0 | 97.6 | 74.9 | 76.5 |
| isa-l | 31.1 | 34.0 | 35.0 | 34.4 | 33.0 |
| reed-solomon-erasure | 37.6 | 36.8 | 36.2 | 32.2 | 31.3 |
| reed-solomon-simd | 26.7 | 24.3 | 24.6 | 20.1 | 21.8 |
| raptorq | 0.72 | 0.53 | 0.61 | 0.52 | 0.55 |
| rlnc | 9.42 | 9.70 | 8.40 | 7.08 | 7.24 |
| rlnc, systematic | 11.9 | 12.3 | 10.9 | 10.2 | 9.91 |

## 4+2, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 4.08 | 4.88 | 5.29 | 5.89 | 5.77 |

## 4+2, decode-0, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 9.63 | 9.41 | 9.02 | 8.29 | 8.12 |

## 4+2, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 18.4 | 21.9 | 26.5 | 28.1 | 28.5 |
| isa-l | 17.3 | 20.9 | 25.7 | 27.4 | 27.8 |
| reed-solomon-erasure | 10.9 | 15.9 | 17.9 | 20.1 | 22.1 |
| reed-solomon-simd | 0.15 | 0.53 | 1.54 | 2.66 | 3.44 |
| raptorq | 0.51 | 0.47 | 0.48 | 0.55 | 0.56 |
| rlnc | 4.09 | 4.89 | 5.28 | 5.91 | 5.79 |
| rlnc, systematic | 3.14 | 3.55 | 3.73 | 3.76 | 3.84 |

## 4+2, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 86.5 | 95.0 | 112 | 91.7 | 92.1 |
| isa-l | 44.8 | 56.4 | 61.2 | 61.3 | 62.1 |
| reed-solomon-erasure | 40.5 | 59.0 | 68.2 | 60.2 | 62.3 |
| reed-solomon-simd | 0.15 | 0.55 | 1.62 | 3.23 | 3.72 |
| raptorq | 0.53 | 0.48 | 0.58 | 0.49 | 0.52 |
| rlnc | 9.58 | 9.42 | 9.08 | 8.28 | 8.13 |
| rlnc, systematic | 4.44 | 4.56 | 4.43 | 4.25 | 4.26 |

## 4+2, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 14.5 | 16.9 | 20.2 | 21.1 | 21.3 |
| isa-l | 13.6 | 15.9 | 19.2 | 20.2 | 20.4 |
| reed-solomon-erasure | 8.24 | 11.2 | 12.4 | 12.5 | 14.4 |
| reed-solomon-simd | 0.15 | 0.53 | 1.53 | 2.60 | 3.37 |
| raptorq | 0.52 | 0.47 | 0.48 | 0.55 | 0.57 |
| rlnc | 4.09 | 4.89 | 5.31 | 5.95 | 5.81 |
| rlnc, systematic | 2.68 | 3.03 | 3.17 | 3.28 | 3.24 |

## 4+2, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 68.7 | 80.9 | 94.2 | 76.5 | 78.7 |
| isa-l | 28.7 | 33.1 | 34.6 | 34.3 | 32.9 |
| reed-solomon-erasure | 28.4 | 33.4 | 35.3 | 31.2 | 31.3 |
| reed-solomon-simd | 0.15 | 0.55 | 1.61 | 3.21 | 3.69 |
| raptorq | 0.54 | 0.49 | 0.58 | 0.50 | 0.52 |
| rlnc | 9.56 | 9.42 | 9.12 | 8.29 | 8.15 |
| rlnc, systematic | 3.88 | 3.86 | 3.77 | 3.65 | 3.65 |

## 4+2, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.58 | 5.48 | 6.63 | 7.03 | 7.08 |
| isa-l | 4.33 | 5.23 | 6.45 | 6.85 | 6.95 |
| reed-solomon-erasure | 2.68 | 3.96 | 4.47 | 5.02 | 5.52 |
| reed-solomon-simd | 0.04 | 0.13 | 0.38 | 0.66 | 0.86 |
| raptorq | 0.13 | 0.12 | 0.12 | 0.14 | 0.14 |
| rlnc | 1.83 | 2.42 | 2.86 | 3.31 | 3.14 |
| rlnc, systematic | 0.79 | 0.89 | 0.93 | 0.94 | 0.97 |

## 4+2, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 21.5 | 23.8 | 28.1 | 22.9 | 23.1 |
| isa-l | 11.2 | 14.1 | 15.3 | 15.3 | 15.5 |
| reed-solomon-erasure | 9.19 | 14.2 | 16.9 | 15.0 | 15.5 |
| reed-solomon-simd | 0.04 | 0.14 | 0.40 | 0.81 | 0.93 |
| raptorq | 0.13 | 0.12 | 0.15 | 0.12 | 0.13 |
| rlnc | 4.88 | 5.66 | 5.26 | 4.72 | 4.69 |
| rlnc, systematic | 1.11 | 1.14 | 1.11 | 1.06 | 1.07 |

## 4+2, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 4.43 | 5.47 | 6.04 | 7.00 | 7.69 |
| isa-l | 4.43 | 5.37 | 5.94 | 6.91 | 7.51 |
| reed-solomon-erasure | 6.21 | 5.35 | 5.27 | 5.76 | 7.19 |

## 4+2, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 26.5 | 22.2 | 24.3 | 20.7 | 20.8 |
| isa-l | 22.3 | 22.1 | 23.6 | 20.4 | 21.3 |
| reed-solomon-erasure | 22.1 | 20.7 | 20.8 | 17.8 | 17.6 |

## 6+3, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 14.0 | 16.0 | 17.2 | 17.6 | 17.5 |
| isa-l | 13.0 | 15.2 | 15.9 | 16.0 | 16.0 |
| reed-solomon-erasure | 11.4 | 10.6 | 10.7 | 10.8 | 11.8 |
| reed-solomon-simd | 6.81 | 7.36 | 8.24 | 7.94 | 8.03 |
| raptorq | 0.93 | 0.87 | 0.84 | 0.76 | 0.84 |
| rlnc | 3.27 | 4.15 | 3.94 | 3.86 | 3.86 |
| rlnc, systematic | 5.35 | 6.73 | 6.80 | 6.83 | 6.47 |

## 6+3, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 3.72 | 4.20 | 4.67 | 4.80 | 4.78 |

## 6+3, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 20.7 | 25.0 | 30.4 | 31.7 | 32.4 |
| isa-l | 20.6 | 24.4 | 28.3 | 29.4 | 29.8 |
| reed-solomon-erasure | 15.2 | 17.9 | 19.0 | 21.4 | 27.6 |
| reed-solomon-simd | 0.21 | 0.66 | 1.49 | 2.44 | 2.78 |
| raptorq | 0.70 | 0.77 | 0.77 | 0.70 | 0.76 |
| rlnc | 3.73 | 4.21 | 4.69 | 4.82 | 4.78 |
| rlnc, systematic | 3.95 | 4.62 | 4.69 | 4.72 | 4.77 |

## 6+3, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 16.2 | 19.7 | 22.9 | 24.1 | 24.7 |
| isa-l | 16.4 | 19.6 | 20.8 | 20.9 | 21.0 |
| reed-solomon-erasure | 10.2 | 12.2 | 13.4 | 13.9 | 16.1 |
| reed-solomon-simd | 0.21 | 0.66 | 1.48 | 2.41 | 2.75 |
| raptorq | 0.71 | 0.77 | 0.78 | 0.71 | 0.78 |
| rlnc | 3.73 | 4.21 | 4.69 | 4.81 | 4.78 |
| rlnc, systematic | 3.31 | 3.76 | 3.89 | 3.92 | 3.93 |

## 6+3, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 13.5 | 15.7 | 17.1 | 17.4 | 17.6 |
| isa-l | 12.8 | 14.9 | 15.8 | 15.9 | 16.0 |
| reed-solomon-erasure | 8.21 | 10.1 | 10.5 | 10.8 | 11.8 |
| reed-solomon-simd | 0.21 | 0.66 | 1.47 | 2.38 | 2.71 |
| raptorq | 0.72 | 0.79 | 0.78 | 0.72 | 0.78 |
| rlnc | 3.74 | 4.22 | 4.68 | 4.83 | 4.79 |
| rlnc, systematic | 3.03 | 3.43 | 3.64 | 3.65 | 3.62 |

## 6+3, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.45 | 4.18 | 5.07 | 5.28 | 5.39 |
| isa-l | 3.44 | 4.08 | 4.73 | 4.90 | 4.94 |
| reed-solomon-erasure | 2.49 | 2.99 | 3.18 | 3.57 | 4.57 |
| reed-solomon-simd | 0.04 | 0.11 | 0.25 | 0.41 | 0.46 |
| raptorq | 0.12 | 0.13 | 0.13 | 0.12 | 0.13 |
| rlnc | 1.36 | 1.74 | 2.12 | 2.26 | 2.17 |
| rlnc, systematic | 0.66 | 0.77 | 0.78 | 0.79 | 0.80 |

## 6+3, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.76 | 4.43 | 5.09 | 5.91 | 6.11 |
| isa-l | 3.74 | 4.43 | 5.09 | 5.81 | 6.03 |
| reed-solomon-erasure | 4.99 | 4.09 | 4.01 | 4.32 | 5.33 |

## 8+3, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.5 | 18.0 | 19.4 | 19.6 | 19.8 |
| isa-l | 14.8 | 17.3 | 18.2 | 18.5 | 18.6 |
| reed-solomon-erasure | 12.7 | 11.3 | 11.5 | 11.6 | 12.7 |
| reed-solomon-simd | 7.82 | 8.86 | 9.60 | 11.4 | 11.5 |
| raptorq | 1.01 | 0.99 | 0.95 | 0.94 | 0.95 |
| rlnc | 3.00 | 3.83 | 3.55 | 3.43 | 3.44 |
| rlnc, systematic | 5.28 | 7.34 | 6.98 | 7.49 | 6.43 |

## 8+3, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 3.29 | 3.72 | 4.16 | 4.03 | 3.99 |

## 8+3, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 23.0 | 27.4 | 31.6 | 32.5 | 33.3 |
| isa-l | 21.7 | 26.0 | 27.9 | 28.3 | 28.5 |
| reed-solomon-erasure | 17.8 | 19.1 | 19.7 | 22.0 | 31.1 |
| reed-solomon-simd | 0.28 | 0.87 | 2.10 | 2.74 | 2.99 |
| raptorq | 0.91 | 0.89 | 1.00 | 0.86 | 0.87 |
| rlnc | 3.30 | 3.73 | 4.16 | 4.03 | 3.99 |
| rlnc, systematic | 4.31 | 5.31 | 5.40 | 5.46 | 5.37 |

## 8+3, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 18.1 | 21.2 | 23.9 | 24.2 | 24.4 |
| isa-l | 17.2 | 20.5 | 21.8 | 21.9 | 22.0 |
| reed-solomon-erasure | 12.5 | 13.1 | 13.9 | 14.7 | 17.2 |
| reed-solomon-simd | 0.28 | 0.87 | 2.09 | 2.71 | 2.96 |
| raptorq | 0.78 | 0.90 | 0.88 | 0.88 | 0.88 |
| rlnc | 3.30 | 3.73 | 4.16 | 4.04 | 3.99 |
| rlnc, systematic | 3.64 | 4.26 | 4.30 | 4.44 | 4.31 |

## 8+3, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.2 | 17.7 | 19.2 | 19.5 | 19.7 |
| isa-l | 14.1 | 16.6 | 17.9 | 18.2 | 18.3 |
| reed-solomon-erasure | 9.37 | 10.8 | 11.2 | 11.5 | 12.6 |
| reed-solomon-simd | 0.28 | 0.87 | 2.08 | 2.68 | 2.93 |
| raptorq | 0.79 | 0.91 | 0.89 | 0.88 | 0.89 |
| rlnc | 3.31 | 3.74 | 4.17 | 4.03 | 3.99 |
| rlnc, systematic | 3.34 | 3.89 | 4.04 | 4.05 | 3.92 |

## 8+3, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.88 | 3.41 | 3.95 | 4.03 | 4.17 |
| isa-l | 2.70 | 3.23 | 3.49 | 3.52 | 3.55 |
| reed-solomon-erasure | 2.20 | 2.39 | 2.46 | 2.76 | 3.87 |
| reed-solomon-simd | 0.03 | 0.11 | 0.26 | 0.34 | 0.37 |
| raptorq | 0.11 | 0.11 | 0.12 | 0.11 | 0.11 |
| rlnc | 1.05 | 1.36 | 1.70 | 1.74 | 1.60 |
| rlnc, systematic | 0.54 | 0.66 | 0.67 | 0.68 | 0.67 |

## 8+3, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.97 | 4.74 | 5.42 | 6.26 | 6.55 |
| isa-l | 3.99 | 4.69 | 5.38 | 6.26 | 6.55 |
| reed-solomon-erasure | 5.09 | 4.26 | 4.19 | 4.47 | 5.56 |

## 10+4, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 15.1 | 16.3 | 17.0 | 16.6 | 16.7 |
| isa-l | 13.2 | 14.2 | 14.0 | 13.8 | 12.7 |
| reed-solomon-erasure | 9.26 | 9.92 | 9.66 | 9.88 | 10.4 |
| reed-solomon-simd | 6.90 | 8.31 | 9.48 | 8.96 | 8.95 |
| raptorq | 1.21 | 1.20 | 1.21 | 1.19 | 1.33 |
| rlnc | 2.79 | 3.27 | 2.99 | 2.86 | 2.86 |
| rlnc, systematic | 5.15 | 6.73 | 6.55 | 6.45 | 4.73 |

## 10+4, encode, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 32.1 | 36.0 | 36.0 | 35.8 | 35.4 |
| isa-l | 17.9 | 18.4 | 18.4 | 18.4 | 18.2 |
| reed-solomon-erasure | 21.4 | 19.8 | 17.9 | 15.0 | 15.5 |
| reed-solomon-simd | 17.3 | 13.4 | 15.4 | 14.8 | 12.7 |
| raptorq | 1.64 | 1.67 | 1.40 | 1.41 | 1.27 |
| rlnc | 4.39 | 4.80 | 3.96 | 3.47 | 3.06 |
| rlnc, systematic | 8.57 | 10.6 | 8.00 | 7.62 | 4.99 |

## 10+4, decode-0, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 3.03 | 3.30 | 3.62 | 3.46 | 3.32 |

## 10+4, decode-0, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rlnc | 5.02 | 4.82 | 4.58 | 4.23 | 3.33 |

## 10+4, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 23.8 | 28.0 | 32.6 | 33.2 | 34.5 |
| isa-l | 22.4 | 27.1 | 29.7 | 30.0 | 30.5 |
| reed-solomon-erasure | 19.9 | 19.7 | 20.1 | 22.3 | 32.6 |
| reed-solomon-simd | 0.34 | 1.04 | 2.25 | 3.55 | 3.88 |
| raptorq | 1.09 | 1.07 | 1.12 | 1.19 | 1.24 |
| rlnc | 3.04 | 3.31 | 3.63 | 3.47 | 3.33 |
| rlnc, systematic | 4.47 | 5.84 | 5.85 | 5.98 | 5.43 |

## 10+4, decode-1, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 72.9 | 94.2 | 98.5 | 98.6 | 100 |
| isa-l | 49.9 | 52.7 | 55.7 | 57.6 | 57.9 |
| reed-solomon-erasure | 55.9 | 68.0 | 65.9 | 58.1 | 61.6 |
| reed-solomon-simd | 0.35 | 1.17 | 2.43 | 3.32 | 3.37 |
| raptorq | 1.21 | 1.46 | 1.29 | 1.31 | 1.18 |
| rlnc | 4.98 | 4.84 | 4.59 | 4.20 | 3.34 |
| rlnc, systematic | 7.06 | 7.99 | 7.22 | 7.22 | 5.11 |

## 10+4, decode-2, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 19.6 | 22.8 | 25.2 | 25.6 | 26.0 |
| isa-l | 18.9 | 22.2 | 23.7 | 24.1 | 24.5 |
| reed-solomon-erasure | 14.0 | 13.6 | 14.2 | 15.3 | 18.0 |
| reed-solomon-simd | 0.34 | 1.04 | 2.24 | 3.50 | 3.84 |
| raptorq | 1.10 | 1.08 | 1.13 | 1.20 | 1.24 |
| rlnc | 3.03 | 3.30 | 3.64 | 3.47 | 3.33 |
| rlnc, systematic | 3.70 | 4.45 | 4.61 | 4.70 | 4.23 |

## 10+4, decode-2, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 53.2 | 54.9 | 56.2 | 56.3 | 56.7 |
| isa-l | 31.2 | 34.1 | 34.8 | 34.9 | 34.9 |
| reed-solomon-erasure | 34.9 | 35.9 | 34.9 | 31.5 | 31.0 |
| reed-solomon-simd | 0.35 | 1.17 | 2.42 | 3.31 | 3.32 |
| raptorq | 1.22 | 1.47 | 1.30 | 1.30 | 1.19 |
| rlnc | 4.99 | 4.82 | 4.59 | 4.20 | 3.33 |
| rlnc, systematic | 5.52 | 6.00 | 5.58 | 5.52 | 4.06 |

## 10+4, decode-3, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 16.4 | 19.0 | 20.5 | 20.9 | 21.3 |
| isa-l | 15.1 | 18.0 | 19.1 | 19.8 | 20.0 |
| reed-solomon-erasure | 9.67 | 11.3 | 11.6 | 12.0 | 13.2 |
| reed-solomon-simd | 0.34 | 1.04 | 2.22 | 3.46 | 3.79 |
| raptorq | 1.11 | 1.09 | 1.14 | 1.21 | 1.24 |
| rlnc | 3.03 | 3.30 | 3.64 | 3.46 | 3.32 |
| rlnc, systematic | 3.45 | 4.09 | 4.31 | 4.30 | 3.92 |

## 10+4, decode-3, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 35.5 | 40.5 | 40.8 | 40.4 | 40.8 |
| isa-l | 22.4 | 23.9 | 24.2 | 24.2 | 24.3 |
| reed-solomon-erasure | 24.2 | 25.1 | 23.6 | 20.3 | 20.7 |
| reed-solomon-simd | 0.35 | 1.17 | 2.42 | 3.30 | 3.29 |
| raptorq | 1.23 | 1.47 | 1.30 | 1.31 | 1.20 |
| rlnc | 5.00 | 4.85 | 4.59 | 4.19 | 3.34 |
| rlnc, systematic | 5.36 | 5.50 | 5.18 | 5.03 | 3.79 |

## 10+4, decode-4, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 14.2 | 16.3 | 16.8 | 16.7 | 17.0 |
| isa-l | 12.7 | 13.4 | 13.3 | 13.6 | 12.4 |
| reed-solomon-erasure | 8.48 | 9.67 | 9.48 | 9.86 | 10.5 |
| reed-solomon-simd | 0.34 | 1.04 | 2.21 | 3.43 | 3.73 |
| raptorq | 0.95 | 1.11 | 1.15 | 1.13 | 1.26 |
| rlnc | 3.05 | 3.31 | 3.65 | 3.47 | 3.33 |
| rlnc, systematic | 3.22 | 3.73 | 3.95 | 3.88 | 3.55 |

## 10+4, decode-4, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 28.6 | 35.3 | 35.7 | 35.4 | 35.6 |
| isa-l | 17.4 | 18.3 | 18.3 | 18.4 | 18.1 |
| reed-solomon-erasure | 18.9 | 19.3 | 17.8 | 14.9 | 15.5 |
| reed-solomon-simd | 0.35 | 1.17 | 2.42 | 3.29 | 3.28 |
| raptorq | 1.24 | 1.51 | 1.32 | 1.33 | 1.21 |
| rlnc | 5.00 | 4.83 | 4.60 | 4.19 | 3.34 |
| rlnc, systematic | 4.87 | 4.92 | 4.69 | 4.52 | 3.50 |

## 10+4, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 2.38 | 2.80 | 3.25 | 3.34 | 3.44 |
| isa-l | 2.20 | 2.70 | 2.97 | 3.00 | 3.05 |
| reed-solomon-erasure | 1.99 | 1.99 | 2.01 | 2.23 | 3.20 |
| reed-solomon-simd | 0.03 | 0.10 | 0.23 | 0.35 | 0.39 |
| raptorq | 0.11 | 0.11 | 0.11 | 0.12 | 0.12 |
| rlnc | 0.96 | 1.14 | 1.41 | 1.42 | 1.14 |
| rlnc, systematic | 0.45 | 0.59 | 0.58 | 0.60 | 0.54 |

## 10+4, rebuild, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 7.27 | 9.38 | 9.85 | 9.79 | 10.0 |
| isa-l | 5.01 | 5.26 | 5.58 | 5.77 | 5.79 |
| reed-solomon-erasure | 5.33 | 6.71 | 6.56 | 5.81 | 6.16 |
| reed-solomon-simd | 0.04 | 0.12 | 0.24 | 0.33 | 0.33 |
| raptorq | 0.12 | 0.15 | 0.13 | 0.13 | 0.12 |
| rlnc | 2.04 | 2.37 | 1.95 | 1.91 | 1.22 |
| rlnc, systematic | 0.71 | 0.80 | 0.72 | 0.72 | 0.51 |

## 10+4, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 3.49 | 4.06 | 4.55 | 5.13 | 5.40 |
| isa-l | 3.46 | 4.02 | 4.50 | 5.06 | 5.32 |
| reed-solomon-erasure | 4.33 | 3.53 | 3.34 | 3.55 | 4.54 |

## 10+4, update, hot (one row, in cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| rusty_erasure | 17.1 | 16.9 | 17.7 | 14.3 | 14.6 |
| isa-l | 15.6 | 16.1 | 16.9 | 15.1 | 15.4 |
| reed-solomon-erasure | 13.1 | 13.8 | 13.2 | 10.7 | 11.0 |

## 4+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 17.4 | 21.6 | 25.8 | 25.7 | 26.0 |
| rusty_erasure | 20.3 | 23.5 | 27.5 | 28.7 | 28.4 |

## 4+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 15.7 | 19.4 | 24.9 | 25.1 | 25.5 |
| rusty_erasure | 18.8 | 22.2 | 26.7 | 28.3 | 28.6 |

## 4+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 4.04 | 4.92 | 6.27 | 6.29 | 6.37 |
| rusty_erasure | 4.66 | 5.54 | 6.61 | 7.07 | 7.17 |

## 4+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 9.79 | 11.8 | 15.5 | 16.0 | 11.5 |
| rusty_erasure | 8.81 | 8.14 | 8.51 | 9.38 | 11.9 |

## 6+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 18.8 | 22.7 | 28.7 | 30.3 | 30.2 |
| rusty_erasure | 22.9 | 26.5 | 31.5 | 32.7 | 33.3 |

## 6+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 17.3 | 21.4 | 27.8 | 29.5 | 29.1 |
| rusty_erasure | 21.3 | 25.4 | 30.8 | 32.3 | 32.8 |

## 6+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 2.91 | 3.64 | 4.67 | 4.90 | 4.87 |
| rusty_erasure | 3.54 | 4.22 | 5.12 | 5.37 | 5.45 |

## 6+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 11.7 | 13.6 | 18.0 | 18.9 | 17.3 |
| rusty_erasure | 9.84 | 9.07 | 9.65 | 10.6 | 13.5 |

## 8+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 19.6 | 23.1 | 29.7 | 31.9 | 32.4 |
| rusty_erasure | 25.0 | 28.7 | 32.4 | 33.1 | 33.9 |

## 8+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 17.9 | 22.2 | 29.3 | 31.4 | 31.0 |
| rusty_erasure | 23.5 | 27.8 | 31.9 | 32.9 | 33.6 |

## 8+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 2.27 | 2.81 | 3.67 | 3.96 | 3.96 |
| rusty_erasure | 2.93 | 3.47 | 3.99 | 4.10 | 4.20 |

## 8+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 15.7 | 16.8 | 21.0 | 22.5 | 21.4 |
| rusty_erasure | 13.0 | 11.9 | 12.7 | 13.7 | 16.2 |

## 10+1, encode, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 19.7 | 22.6 | 28.5 | 30.4 | 31.8 |
| rusty_erasure | 24.9 | 28.6 | 33.0 | 34.0 | 34.7 |

## 10+1, decode-1, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 18.8 | 22.0 | 28.5 | 30.2 | 31.5 |
| rusty_erasure | 24.1 | 27.9 | 32.3 | 33.9 | 34.5 |

## 10+1, rebuild, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 1.89 | 2.21 | 2.89 | 3.10 | 3.20 |
| rusty_erasure | 2.42 | 2.80 | 3.24 | 3.35 | 3.43 |

## 10+1, update, cold (rows in turn, out of cache), GiB/s

| Candidate | 4 KiB | 16 KiB | 64 KiB | 256 KiB | 1 MiB |
| --- | --- | --- | --- | --- | --- |
| xor | 20.0 | 19.7 | 25.1 | 26.7 | 26.6 |
| rusty_erasure | 17.3 | 15.8 | 16.9 | 18.1 | 21.0 |

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
