
## Big

| Figure | before (F115) | after (F119) | Disjoint |
| --- | --- | --- | --- |
| MiB a second, alone | 83.2 [81.1, 89.1] (n 4) | 85 [75.9, 97.8] (n 4) | no |
| Resident evictions, alone | 0 [0, 0] (n 4) | 0 [0, 0] (n 4) | no |
| Resident evictions, beside the cell | 0 [0, 0] (n 4) | 0 [0, 0] (n 4) | no |
| cell insert p99 during / before | 1.6 [1.58, 1.74] (n 4) | 1.63 [1.56, 1.7] (n 4) | no |
| cell insert p99 ms after | 45.2 [43.3, 45.8] (n 4) | 44.3 [40.6, 46.7] (n 4) | no |
| cell insert p99 ms before | 24.3 [23.8, 25.7] (n 4) | 24.6 [24, 24.7] (n 4) | no |
| cell insert p99 ms during | 39.8 [38.8, 41.6] (n 4) | 40.1 [37.6, 42.1] (n 4) | no |
| cell read p99 during / before | 1.13 [1.08, 1.16] (n 4) | 1.12 [1.11, 1.14] (n 4) | no |
| cell read p99 ms after | 7.58 [7.14, 8.11] (n 4) | 6.98 [6.14, 7.08] (n 4) | yes |
| cell read p99 ms before | 6.58 [6.16, 6.7] (n 4) | 6.54 [6.51, 6.57] (n 4) | no |
| cell read p99 ms during | 7.24 [7.13, 7.59] (n 4) | 7.33 [7.25, 7.43] (n 4) | no |
| device bytes a partition, alone | 1.59e+03 [1.59e+03, 1.6e+03] (n 4) | 1.59e+03 [1.59e+03, 1.6e+03] (n 4) | no |
| europa: page held p99 ms, beside the cell | 0.0165 [0.016, 0.017] (n 4) | 0.018 [0.016, 0.021] (n 4) | no |
| europa: page read p99 ms, alone | 25 [24.7, 25.2] (n 4) | 25 [24.4, 25.2] (n 4) | no |
| europa: page read p99 ms, beside the cell | 32.5 [31, 34] (n 4) | 32 [30.7, 32.4] (n 4) | no |
| europa: partitions a second, alone | 1.97e+04 [1.81e+04, 2.19e+04] (n 4) | 2.28e+04 [1.6e+04, 2.64e+04] (n 4) | no |
| europa: partitions a second, beside the cell | 6.38e+03 [5.56e+03, 9.14e+03] (n 4) | 8.86e+03 [6.74e+03, 9.99e+03] (n 4) | no |
| hyperion: page held p99 ms, beside the cell | 0.122 [0.097, 0.132] (n 4) | 0.136 [0.126, 0.14] (n 4) | no |
| hyperion: page read p99 ms, alone | 185 [180, 187] (n 4) | 182 [178, 184] (n 4) | no |
| hyperion: page read p99 ms, beside the cell | 772 [705, 831] (n 4) | 725 [713, 737] (n 4) | no |
| hyperion: partitions a second, alone | 3.27e+04 [2.76e+04, 3.33e+04] (n 4) | 3.18e+04 [2.62e+04, 3.4e+04] (n 4) | no |
| hyperion: partitions a second, beside the cell | 1.06e+04 [8.92e+03, 1.33e+04] (n 4) | 1.28e+04 [1.04e+04, 1.3e+04] (n 4) | no |
| page held max ms, alone | 0.338 [0.244, 0.683] (n 4) | 0.479 [0.315, 0.651] (n 4) | no |
| page held max ms, beside the cell | 0.409 [0.297, 0.567] (n 4) | 0.405 [0.309, 0.507] (n 4) | no |
| page held p50 ms, alone | 0.015 [0.01, 0.017] (n 4) | 0.0145 [0.012, 0.016] (n 4) | no |
| page held p50 ms, beside the cell | 0.0165 [0.011, 0.019] (n 4) | 0.0155 [0.013, 0.018] (n 4) | no |
| page held p99 ms, alone | 0.108 [0.103, 0.116] (n 4) | 0.115 [0.112, 0.116] (n 4) | no |
| page held p99 ms, beside the cell | 0.126 [0.07, 0.133] (n 4) | 0.127 [0.111, 0.137] (n 4) | no |
| page paced p99 ms, beside the cell | - | 5.87 [5.73, 9.86] (n 4) |  |
| page queued p99 ms, beside the cell | - | 0.0225 [0.021, 0.026] (n 4) |  |
| page read max ms, alone | 289 [202, 436] (n 4) | 266 [208, 338] (n 4) | no |
| page read max ms, beside the cell | 1.3e+03 [1.2e+03, 1.92e+03] (n 4) | 1.17e+03 [1.14e+03, 1.37e+03] (n 4) | no |
| page read p50 ms, alone | 154 [153, 154] (n 4) | 154 [152, 155] (n 4) | no |
| page read p50 ms, beside the cell | 346 [328, 350] (n 4) | 326 [315, 334] (n 4) | no |
| page read p99 ms, alone | 185 [183, 185] (n 4) | 185 [183, 185] (n 4) | no |
| page read p99 ms, beside the cell | 711 [655, 782] (n 4) | 678 [631, 696] (n 4) | no |
| pages shed a page, alone | 35.1 [31.2, 37.5] (n 4) | 34 [26.6, 42.9] (n 4) | no |
| partitions a second, alone | 8.39e+04 [8.18e+04, 8.98e+04] (n 4) | 8.57e+04 [7.65e+04, 9.87e+04] (n 4) | no |
| partitions a second, beside the cell | 2.67e+04 [2.63e+04, 3.75e+04] (n 4) | 3.45e+04 [3e+04, 3.75e+04] (n 4) | no |
| titan: page held p99 ms, beside the cell | 0.139 [0.111, 0.15] (n 4) | 0.13 [0.083, 0.162] (n 4) | no |
| titan: page read p99 ms, alone | 186 [183, 193] (n 4) | 190 [187, 196] (n 4) | no |
| titan: page read p99 ms, beside the cell | 780 [664, 795] (n 4) | 709 [618, 774] (n 4) | no |
| titan: partitions a second, alone | 3.44e+04 [2.91e+04, 3.59e+04] (n 4) | 3.37e+04 [2.91e+04, 3.82e+04] (n 4) | no |
| titan: partitions a second, beside the cell | 1.09e+04 [9.45e+03, 1.5e+04] (n 4) | 1.36e+04 [1.13e+04, 1.46e+04] (n 4) | no |
| whole scan s, beside the cell | 112 [79.5, 113] (n 4) | 86.3 [79.8, 99.5] (n 4) | no |

## Keyword

| Figure | before (F115) | after (F119) | Disjoint |
| --- | --- | --- | --- |
| MiB a second, alone | 141 [133, 157] (n 4) | 118 [112, 133] (n 4) | yes |
| Resident evictions, alone | 0 [0, 0] (n 4) | 0 [0, 0] (n 4) | no |
| Resident evictions, beside the cell | 0 [0, 0] (n 4) | 0 [0, 0] (n 4) | no |
| cell insert p99 during / before | 1.07 [0.965, 1.15] (n 4) | 0.938 [0.881, 1.14] (n 4) | no |
| cell insert p99 ms after | 26.9 [25.8, 27.9] (n 4) | 26.3 [25.9, 27.1] (n 4) | no |
| cell insert p99 ms before | 28.8 [27.3, 33.2] (n 4) | 28.7 [28.2, 32.1] (n 4) | no |
| cell insert p99 ms during | 30.4 [27.7, 37.5] (n 4) | 28.2 [25.5, 32.5] (n 4) | no |
| cell read p99 during / before | 1.18 [1.02, 1.3] (n 4) | 1.11 [1.04, 1.4] (n 4) | no |
| cell read p99 ms after | 8.51 [8.1, 9.51] (n 4) | 8.63 [8.33, 9.41] (n 4) | no |
| cell read p99 ms before | 9.96 [9.36, 10.5] (n 4) | 10.3 [9.43, 10.6] (n 4) | no |
| cell read p99 ms during | 11.7 [10, 13.1] (n 4) | 11.3 [10.5, 14.1] (n 4) | no |
| device bytes a partition, alone | 2.84e+03 [2.73e+03, 3.12e+03] (n 4) | 2.99e+03 [2.87e+03, 3.17e+03] (n 4) | no |
| europa: page held p99 ms, beside the cell | 0.112 [0.083, 0.169] (n 4) | 0.0995 [0.049, 0.132] (n 4) | no |
| europa: page read p99 ms, alone | 0.863 [0.847, 0.867] (n 4) | 0.887 [0.851, 0.979] (n 4) | no |
| europa: page read p99 ms, beside the cell | 2.42 [2.29, 2.83] (n 4) | 2.34 [2.04, 2.86] (n 4) | no |
| europa: partitions a second, alone | 1.4e+04 [1.14e+04, 1.78e+04] (n 4) | 1.11e+04 [1.05e+04, 1.29e+04] (n 4) | no |
| europa: partitions a second, beside the cell | 4.08e+03 [3.13e+03, 5e+03] (n 4) | 3.67e+03 [3.27e+03, 4.86e+03] (n 4) | no |
| hyperion: page held p99 ms, beside the cell | 0.052 [0.037, 0.121] (n 4) | 0.0725 [0.031, 0.184] (n 4) | no |
| hyperion: page read p99 ms, alone | 9.86 [9.21, 11.7] (n 4) | 16.4 [13.6, 18.9] (n 4) | yes |
| hyperion: page read p99 ms, beside the cell | 38.7 [33.8, 48.1] (n 4) | 36.9 [34.6, 47.1] (n 4) | no |
| hyperion: partitions a second, alone | 1.83e+04 [1.76e+04, 2.08e+04] (n 4) | 1.81e+04 [1.38e+04, 1.9e+04] (n 4) | no |
| hyperion: partitions a second, beside the cell | 5.06e+03 [4.67e+03, 6.65e+03] (n 4) | 5.91e+03 [4.5e+03, 7e+03] (n 4) | no |
| page held max ms, alone | 6.46 [5.34, 8.32] (n 4) | 4.66 [1.9, 6.5] (n 4) | no |
| page held max ms, beside the cell | 1.84 [1.29, 4.16] (n 4) | 2.75 [1.89, 4.89] (n 4) | no |
| page held p50 ms, alone | 0.007 [0.007, 0.007] (n 4) | 0.007 [0.007, 0.008] (n 4) | no |
| page held p50 ms, beside the cell | 0.005 [0.005, 0.006] (n 4) | 0.005 [0.005, 0.006] (n 4) | no |
| page held p99 ms, alone | 0.194 [0.11, 0.213] (n 4) | 0.144 [0.123, 0.196] (n 4) | no |
| page held p99 ms, beside the cell | 0.102 [0.101, 0.158] (n 4) | 0.108 [0.1, 0.16] (n 4) | no |
| page paced p99 ms, beside the cell | - | 6.53 [6.37, 7.1] (n 4) |  |
| page queued p99 ms, beside the cell | - | 0.107 [0.085, 0.263] (n 4) |  |
| page read max ms, alone | 53.8 [29.2, 108] (n 4) | 86.5 [74.2, 142] (n 4) | no |
| page read max ms, beside the cell | 124 [75.3, 170] (n 4) | 112 [77.3, 117] (n 4) | no |
| page read p50 ms, alone | 3.13 [2.83, 3.26] (n 4) | 3.28 [3.18, 3.41] (n 4) | no |
| page read p50 ms, beside the cell | 6.8 [5.66, 7.04] (n 4) | 7.2 [6.88, 7.42] (n 4) | no |
| page read p99 ms, alone | 9.15 [8.89, 11.5] (n 4) | 12.7 [11.7, 14] (n 4) | yes |
| page read p99 ms, beside the cell | 34.2 [30.2, 40.2] (n 4) | 36.1 [31.5, 38.7] (n 4) | no |
| pages shed a page, alone | 1.16 [0.977, 1.18] (n 4) | 1.36 [1.21, 1.4] (n 4) | yes |
| partitions a second, alone | 5.26e+04 [4.99e+04, 5.86e+04] (n 4) | 4.42e+04 [4.18e+04, 4.97e+04] (n 4) | yes |
| partitions a second, beside the cell | 1.41e+04 [1.41e+04, 1.87e+04] (n 4) | 1.4e+04 [1.4e+04, 1.87e+04] (n 4) | no |
| titan: page held p99 ms, beside the cell | 0.15 [0.067, 0.175] (n 4) | 0.187 [0.143, 0.204] (n 4) | no |
| titan: page read p99 ms, alone | 9.7 [9.41, 13] (n 4) | 9.82 [9.09, 11.7] (n 4) | no |
| titan: page read p99 ms, beside the cell | 35.5 [30.1, 42] (n 4) | 33.9 [28.2, 44.5] (n 4) | no |
| titan: partitions a second, alone | 2.05e+04 [1.81e+04, 2.21e+04] (n 4) | 1.67e+04 [1.35e+04, 1.82e+04] (n 4) | no |
| titan: partitions a second, beside the cell | 5.7e+03 [4.73e+03, 7.04e+03] (n 4) | 5.31e+03 [4.51e+03, 6.85e+03] (n 4) | no |
| whole scan s, beside the cell | 2.99 [2.73, 3.39] (n 4) | 3.14 [2.5, 3.35] (n 4) | no |

## cell

| Figure | before (F115) | after (F119) | Disjoint |
| --- | --- | --- | --- |
| Resident evictions, alone | 0 [0, 0] (n 4) | 0 [0, 0] (n 4) | no |
| insert p99 ms, alone | 24.7 [24, 25.2] (n 4) | 24.5 [23.9, 24.6] (n 4) | no |
| read p99 ms, alone | 7.68 [7.43, 7.92] (n 4) | 7.64 [7.43, 7.92] (n 4) | no |
| reads a second, alone | 2.39e+03 [2.33e+03, 2.44e+03] (n 4) | 2.38e+03 [2.34e+03, 2.41e+03] (n 4) | no |
