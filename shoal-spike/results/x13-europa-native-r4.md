### Checks, round 4

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | ok | objects/s |
| --- | --- | --- | --- |
| stamped | seek | 1.00 | — |
| stamped | digest | — | — |
| splitmix | reference | 1.00 | — |
| splitmix | seek | 1.00 | — |
| splitmix | digest | — | — |
| xoshiro | reference | 1.00 | — |
| xoshiro | seek | 1.00 | — |
| xoshiro | digest | — | — |
| chacha8 | reference | 1.00 | — |
| chacha8 | seek | 1.00 | — |
| chacha8 | digest | — | — |
| aes-ctr | reference | 1.00 | — |
| aes-ctr | seek | 1.00 | — |
| aes-ctr | digest | — | — |
| crc64nvme | reference | 1.00 | — |
| describe fixed | expand | 1.00 | 7491505 |
| describe uniform | expand | 1.00 | 8796850 |
| describe doublings | expand | 1.00 | 10620410 |
| describe table | expand | 1.00 | 8109447 |

### 1. Making bytes on one core, GiB/s, round 4

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 10.98 | 10.92 | 11.06 | 0.347 |
| fill 4K cold | splitmix | 23.37 | 22.82 | 23.40 | 0.163 |
| fill 4K cold | xoshiro | 15.34 | 15.11 | 15.37 | 0.249 |
| fill 4K cold | chacha8 | 6.75 | 6.65 | 6.76 | 0.565 |
| fill 4K cold | aes-ctr | 10.26 | 10.13 | 10.27 | 0.372 |
| fill+crc 4K cold | stamped | 13.61 | 13.59 | 13.61 | 0.280 |
| fill+crc 4K cold | splitmix | 22.82 | 22.80 | 22.82 | 0.167 |
| fill+crc 4K cold | xoshiro | 12.96 | 12.95 | 13.00 | 0.294 |
| fill+crc 4K cold | chacha8 | 6.24 | 6.23 | 6.24 | 0.612 |
| fill+crc 4K cold | aes-ctr | 7.81 | 7.79 | 7.88 | 0.488 |
| verify 4K cold | stamped | 16.71 | 16.69 | 16.99 | 0.228 |
| verify 4K cold | splitmix | 32.25 | 32.23 | 32.35 | 0.118 |
| verify 4K cold | xoshiro | 15.00 | 15.00 | 15.03 | 0.254 |
| verify 4K cold | chacha8 | 6.46 | 6.45 | 6.46 | 0.591 |
| verify 4K cold | aes-ctr | 9.65 | 9.62 | 9.68 | 0.395 |
| crc 4K cold | crc64nvme | 38.81 | 38.76 | 38.85 | 0.098 |
| copy 4K cold | memcpy | 6.34 | 6.34 | 6.35 | 0.601 |
| fill 4K hot | stamped | 86.52 | 86.04 | 86.75 | 0.044 |
| fill 4K hot | splitmix | 59.37 | 59.35 | 59.38 | 0.064 |
| fill 4K hot | xoshiro | 20.66 | 20.66 | 20.67 | 0.185 |
| fill 4K hot | chacha8 | 6.76 | 6.76 | 6.76 | 0.564 |
| fill 4K hot | aes-ctr | 10.83 | 10.79 | 10.85 | 0.352 |
| fill+crc 4K hot | stamped | 39.08 | 39.04 | 39.46 | 0.098 |
| fill+crc 4K hot | splitmix | 32.65 | 32.63 | 32.66 | 0.117 |
| fill+crc 4K hot | xoshiro | 15.77 | 15.76 | 15.78 | 0.242 |
| fill+crc 4K hot | chacha8 | 6.31 | 6.31 | 6.32 | 0.604 |
| fill+crc 4K hot | aes-ctr | 9.37 | 9.36 | 9.38 | 0.407 |
| verify 4K hot | stamped | 53.71 | 53.63 | 53.76 | 0.071 |
| verify 4K hot | splitmix | 43.06 | 43.04 | 43.06 | 0.089 |
| verify 4K hot | xoshiro | 18.10 | 18.09 | 18.10 | 0.211 |
| verify 4K hot | chacha8 | 6.61 | 6.61 | 6.61 | 0.577 |
| verify 4K hot | aes-ctr | 10.15 | 10.15 | 10.15 | 0.376 |
| crc 4K hot | crc64nvme | 67.99 | 67.99 | 67.99 | 0.056 |
| copy 4K hot | memcpy | 148.5 | 148.5 | 148.6 | 0.026 |
| fill 64K cold | stamped | 12.24 | 12.23 | 12.25 | 4.98 |
| fill 64K cold | splitmix | 23.38 | 23.35 | 23.39 | 2.61 |
| fill 64K cold | xoshiro | 15.26 | 15.18 | 15.40 | 4.00 |
| fill 64K cold | chacha8 | 6.88 | 6.88 | 6.90 | 8.87 |
| fill 64K cold | aes-ctr | 8.46 | 8.45 | 8.51 | 7.21 |
| fill+crc 64K cold | stamped | 12.21 | 11.90 | 12.21 | 5.00 |
| fill+crc 64K cold | splitmix | 19.03 | 18.92 | 19.05 | 3.21 |
| fill+crc 64K cold | xoshiro | 11.74 | 11.73 | 11.76 | 5.20 |
| fill+crc 64K cold | chacha8 | 6.38 | 6.38 | 6.38 | 9.57 |
| fill+crc 64K cold | aes-ctr | 7.68 | 7.65 | 7.68 | 7.95 |
| verify 64K cold | stamped | 10.86 | 10.78 | 10.86 | 5.62 |
| verify 64K cold | splitmix | 15.77 | 15.75 | 15.78 | 3.87 |
| verify 64K cold | xoshiro | 11.57 | 11.51 | 11.57 | 5.28 |
| verify 64K cold | chacha8 | 5.34 | 5.31 | 5.34 | 11.44 |
| verify 64K cold | aes-ctr | 7.74 | 7.70 | 7.75 | 7.88 |
| crc 64K cold | crc64nvme | 37.59 | 37.50 | 37.61 | 1.62 |
| copy 64K cold | memcpy | 9.09 | 9.03 | 9.13 | 6.72 |
| fill 64K hot | stamped | 65.05 | 65.03 | 65.06 | 0.938 |
| fill 64K hot | splitmix | 59.44 | 59.43 | 59.46 | 1.03 |
| fill 64K hot | xoshiro | 23.64 | 23.63 | 23.65 | 2.58 |
| fill 64K hot | chacha8 | 7.05 | 7.05 | 7.05 | 8.66 |
| fill 64K hot | aes-ctr | 11.75 | 11.75 | 11.75 | 5.20 |
| fill+crc 64K hot | stamped | 35.25 | 35.21 | 35.26 | 1.73 |
| fill+crc 64K hot | splitmix | 33.51 | 33.49 | 33.52 | 1.82 |
| fill+crc 64K hot | xoshiro | 17.92 | 17.92 | 17.92 | 3.41 |
| fill+crc 64K hot | chacha8 | 6.49 | 6.49 | 6.49 | 9.41 |
| fill+crc 64K hot | aes-ctr | 10.19 | 10.19 | 10.19 | 5.99 |
| verify 64K hot | stamped | 35.05 | 35.04 | 35.05 | 1.74 |
| verify 64K hot | splitmix | 33.42 | 33.41 | 33.43 | 1.83 |
| verify 64K hot | xoshiro | 17.92 | 17.91 | 17.92 | 3.41 |
| verify 64K hot | chacha8 | 6.49 | 6.49 | 6.49 | 9.41 |
| verify 64K hot | aes-ctr | 10.18 | 10.18 | 10.18 | 5.99 |
| crc 64K hot | crc64nvme | 76.45 | 76.45 | 76.45 | 0.798 |
| copy 64K hot | memcpy | 75.02 | 75.01 | 75.02 | 0.814 |
| fill 1M cold | stamped | 12.47 | 12.46 | 12.47 | 78.32 |
| fill 1M cold | splitmix | 23.44 | 23.42 | 23.45 | 41.67 |
| fill 1M cold | xoshiro | 14.10 | 14.02 | 14.17 | 69.28 |
| fill 1M cold | chacha8 | 6.89 | 6.88 | 6.89 | 141.8 |
| fill 1M cold | aes-ctr | 8.52 | 8.50 | 8.53 | 114.6 |
| fill+crc 1M cold | stamped | 10.92 | 10.91 | 10.92 | 89.46 |
| fill+crc 1M cold | splitmix | 17.93 | 17.93 | 17.93 | 54.47 |
| fill+crc 1M cold | xoshiro | 12.88 | 12.85 | 12.92 | 75.84 |
| fill+crc 1M cold | chacha8 | 6.37 | 6.37 | 6.37 | 153.3 |
| fill+crc 1M cold | aes-ctr | 7.71 | 7.69 | 7.71 | 126.7 |
| verify 1M cold | stamped | 17.52 | 17.50 | 17.71 | 55.73 |
| verify 1M cold | splitmix | 23.24 | 23.16 | 23.39 | 42.01 |
| verify 1M cold | xoshiro | 14.32 | 14.32 | 14.38 | 68.18 |
| verify 1M cold | chacha8 | 5.94 | 5.93 | 5.95 | 164.4 |
| verify 1M cold | aes-ctr | 8.76 | 8.75 | 8.79 | 111.5 |
| crc 1M cold | crc64nvme | 40.46 | 40.45 | 40.48 | 24.13 |
| copy 1M cold | memcpy | 10.01 | 9.97 | 10.06 | 97.56 |
| fill 1M hot | stamped | 55.11 | 55.04 | 55.13 | 17.72 |
| fill 1M hot | splitmix | 59.87 | 59.85 | 59.88 | 16.31 |
| fill 1M hot | xoshiro | 23.44 | 23.42 | 23.46 | 41.67 |
| fill 1M hot | chacha8 | 7.07 | 7.07 | 7.07 | 138.2 |
| fill 1M hot | aes-ctr | 11.61 | 11.61 | 11.61 | 84.11 |
| fill+crc 1M hot | stamped | 31.94 | 31.93 | 31.94 | 30.58 |
| fill+crc 1M hot | splitmix | 33.58 | 33.58 | 33.58 | 29.08 |
| fill+crc 1M hot | xoshiro | 17.82 | 17.81 | 17.82 | 54.80 |
| fill+crc 1M hot | chacha8 | 6.49 | 6.49 | 6.49 | 150.4 |
| fill+crc 1M hot | aes-ctr | 10.07 | 10.07 | 10.08 | 96.94 |
| verify 1M hot | stamped | 29.90 | 29.88 | 29.91 | 32.66 |
| verify 1M hot | splitmix | 30.72 | 30.72 | 30.73 | 31.79 |
| verify 1M hot | xoshiro | 17.03 | 17.02 | 17.03 | 57.36 |
| verify 1M hot | chacha8 | 6.37 | 6.37 | 6.37 | 153.3 |
| verify 1M hot | aes-ctr | 9.73 | 9.73 | 9.73 | 100.4 |
| crc 1M hot | crc64nvme | 76.58 | 76.58 | 76.58 | 12.75 |
| copy 1M hot | memcpy | 62.37 | 62.34 | 62.43 | 15.66 |

### 3a. Making bytes on several cores, no wire, round 4

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | GiB/s, all | GiB/s, least core |
| --- | --- | --- | --- |
| fill+crc 1M cold x1 | aes-ctr | 7.69 | 7.69 |
| fill+crc 1M cold x1 | chacha8 | 6.37 | 6.37 |
| fill+crc 1M cold x1 | xoshiro | 13.09 | 13.09 |
| fill+crc 1M cold x1 | splitmix | 17.92 | 17.92 |
| fill+crc 1M cold x1 | stamped | 10.90 | 10.90 |
| fill+crc 1M cold x2 | aes-ctr | 14.96 | 7.48 |
| fill+crc 1M cold x2 | chacha8 | 11.55 | 5.77 |
| fill+crc 1M cold x2 | xoshiro | 15.02 | 7.50 |
| fill+crc 1M cold x2 | splitmix | 18.94 | 9.46 |
| fill+crc 1M cold x2 | stamped | 15.47 | 7.73 |
| fill+crc 1M cold x4 | aes-ctr | 20.26 | 5.06 |
| fill+crc 1M cold x4 | chacha8 | 18.36 | 4.59 |
| fill+crc 1M cold x4 | xoshiro | 18.79 | 4.68 |
| fill+crc 1M cold x4 | splitmix | 18.73 | 4.68 |
| fill+crc 1M cold x4 | stamped | 17.68 | 4.41 |

### 2. Against a server that discards, round 4

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | fill+crc aes-ctr | 4922 | 208.0 | 1.000 | 1.00 | 4923 | 120.9 | 0.554 | 334.0 | 0 | 0 |
| put 1M plain | fill aes-ctr | 5273 | 194.2 | 1.000 | 1.00 | 5273 | 120.0 | 0.591 | 318.0 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 3926 | 260.8 | 1.000 | 1.00 | 3926 | 121.4 | 0.438 | 388.6 | 0 | 0 |
| put 1M plain | fill chacha8 | 4152 | 246.5 | 1.000 | 1.00 | 4153 | 121.6 | 0.466 | 370.3 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 6309 | 162.3 | 1.000 | 1.00 | 6310 | 119.7 | 0.711 | 281.1 | 0 | 0 |
| put 1M plain | fill xoshiro | 6870 | 149.0 | 1.000 | 1.00 | 6870 | 119.7 | 0.776 | 272.4 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 7487 | 136.7 | 1.000 | 1.00 | 7488 | 119.4 | 0.846 | 252.7 | 0 | 0 |
| put 1M plain | fill splitmix | 8285 | 123.6 | 1.000 | 1.00 | 8286 | 119.4 | 0.938 | 241.5 | 0 | 0 |
| put 1M plain | fill+crc stamped | 6741 | 151.9 | 1.000 | 1.00 | 6742 | 121.9 | 0.775 | 276.1 | 0 | 0 |
| put 1M plain | fill stamped | 7217 | 141.9 | 1.000 | 1.00 | 7218 | 124.0 | 0.848 | 264.4 | 0 | 0 |
| put 1M plain | pattern | 7113 | 143.9 | 1.000 | 1.00 | 7114 | 147.9 | 1.000 | 296.2 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 6235 | 154.3 | 0.940 | 0.956 | 6635 | 147.1 | 0.868 | 299.2 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 4523 | 211.0 | 0.932 | 0.951 | 4853 | 148.5 | 0.629 | 359.8 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 6878 | 121.3 | 0.815 | 0.851 | 8441 | 153.0 | 1.000 | 266.4 | 0 | 0 |
| get 1M plain | regenerate splitmix | 5432 | 113.9 | 0.604 | 0.605 | 8994 | 193.7 | 1.000 | 292.9 | 0 | 0 |
| get 1M plain | regenerate stamped | 5371 | 132.5 | 0.695 | 0.710 | 7726 | 195.9 | 1.000 | 314.5 | 0 | 0 |
| get 1M plain | crc | 5560 | 115.1 | 0.625 | 0.637 | 8899 | 189.3 | 1.000 | 282.4 | 0 | 0 |
| get 1M plain | none | 5706 | 106.1 | 0.591 | 0.583 | 9648 | 184.4 | 1.000 | 260.9 | 0 | 0 |
| put 1M ktls | fill+crc aes-ctr | 2717 | 376.6 | 0.999 | 0.998 | 2719 | 343.8 | 0.885 | 733.2 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 1625 | 333.4 | 0.529 | 0.529 | 3071 | 478.5 | 0.732 | 838.1 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 1765 | 398.5 | 0.687 | 0.687 | 2570 | 441.6 | 0.734 | 869.1 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 2518 | 406.3 | 0.999 | 0.998 | 2520 | 338.7 | 0.805 | 757.2 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 2924 | 340.9 | 0.974 | 0.974 | 3004 | 338.8 | 0.940 | 693.2 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 2953 | 329.0 | 0.949 | 0.951 | 3113 | 334.2 | 0.936 | 676.0 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 1828 | 278.7 | 0.498 | 0.500 | 3674 | 444.1 | 0.766 | 752.7 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 1695 | 266.0 | 0.440 | 0.443 | 3850 | 478.1 | 0.765 | 776.7 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 2934 | 330.2 | 0.946 | 0.946 | 3101 | 335.3 | 0.933 | 679.0 | 0 | 1.00 |
| put 1M ktls | fill stamped | 2798 | 310.3 | 0.848 | 0.851 | 3300 | 330.9 | 0.877 | 654.9 | 0 | 1.00 |
| put 1M ktls | pattern | 2792 | 366.6 | 0.999 | 1.00 | 2793 | 354.0 | 0.938 | 731.9 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 1487 | 447.3 | 0.649 | 0.664 | 2289 | 347.0 | 0.477 | 808.5 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 1205 | 558.2 | 0.657 | 0.681 | 1834 | 353.3 | 0.389 | 939.6 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 1379 | 454.4 | 0.612 | 0.637 | 2253 | 348.4 | 0.442 | 837.5 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 1415 | 432.8 | 0.598 | 0.626 | 2366 | 351.7 | 0.459 | 822.2 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 1375 | 452.1 | 0.607 | 0.634 | 2265 | 351.0 | 0.444 | 837.0 | 0 | 1.00 |
| get 1M ktls | crc | 1459 | 413.3 | 0.589 | 0.618 | 2478 | 345.7 | 0.466 | 791.6 | 0 | 1.00 |
| get 1M ktls | none | 1627 | 360.3 | 0.573 | 0.593 | 2842 | 354.5 | 0.537 | 746.2 | 0 | 1.00 |

### 3b. The put from several client cores, round 4

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain x1 | fill+crc aes-ctr | 4962 | 206.4 | 1.000 | 1.00 | 4962 | 120.9 | 0.559 | 331.4 | 0 | 0 |
| put 1M plain x1 | fill+crc chacha8 | 3942 | 259.7 | 1.000 | 1.00 | 3942 | 121.8 | 0.442 | 389.1 | 0 | 0 |
| put 1M plain x1 | fill+crc xoshiro | 6297 | 162.6 | 1.000 | 1.00 | 6297 | 120.2 | 0.712 | 282.6 | 0 | 0 |
| put 1M plain x1 | fill+crc splitmix | 7470 | 137.1 | 1.000 | 1.00 | 7470 | 119.9 | 0.847 | 255.5 | 0 | 0 |
| put 1M plain x2 | fill+crc aes-ctr | 9826 | 208.4 | 1.000 | 1.00 | 4913 | 118.7 | 0.561 | 329.3 | 0 | 0 |
| put 1M plain x2 | fill+crc chacha8 | 7800 | 262.6 | 1.000 | 1.00 | 3900 | 118.8 | 0.445 | 383.8 | 0 | 0 |
| put 1M plain x2 | fill+crc xoshiro | 12427 | 164.8 | 1.000 | 1.00 | 6214 | 119.2 | 0.715 | 280.4 | 0 | 0 |
| put 1M plain x2 | fill+crc splitmix | 14720 | 139.1 | 1.000 | 1.00 | 7360 | 119.3 | 0.849 | 255.5 | 0 | 0 |
| put 1M plain x4 | fill+crc aes-ctr | 19124 | 214.1 | 1.000 | 1.00 | 4782 | 120.6 | 0.565 | 333.0 | 0 | 0 |
| put 1M plain x4 | fill+crc chacha8 | 15210 | 269.3 | 1.000 | 1.00 | 3803 | 120.4 | 0.449 | 388.0 | 0 | 0 |
| put 1M plain x4 | fill+crc xoshiro | 23972 | 170.8 | 1.000 | 1.00 | 5994 | 122.6 | 0.723 | 279.2 | 0 | 0 |
| put 1M plain x4 | fill+crc splitmix | 28077 | 145.8 | 1.000 | 1.00 | 7021 | 124.2 | 0.852 | 259.8 | 0 | 0 |

### 4. A folder of real files, round 4

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | MiB/s | cpu ms/GiB | device MiB read | from device |
| --- | --- | --- | --- | --- | --- |
| 64K cold | read+sha256 | 738.4 | 882.1 | 2048 | 1.00 |
| 64K cold | read+crc | 1047 | 474.4 | 2048 | 1.00 |
| 64K cold | read | 1064 | 458.9 | 2048 | 1.00 |
| 64K hot | read+sha256 | 1764 | 580.5 | 0 | 0 |
| 64K hot | read+crc | 5801 | 176.5 | 0 | 0 |
| 64K hot | read | 6274 | 163.2 | 0 | 0 |
| 1M cold | read+sha256 | 1163 | 577.1 | 2048 | 1.00 |
| 1M cold | read+crc | 2127 | 180.8 | 2048 | 1.00 |
| 1M cold | read | 2194 | 165.8 | 2048 | 1.00 |
| 1M hot | read+sha256 | 2210 | 463.3 | 0 | 0 |
| 1M hot | read+crc | 15180 | 67.45 | 0 | 0 |
| 1M hot | read | 19021 | 53.83 | 0 | 0 |
| 64M cold | read+sha256 | 1314 | 529.9 | 2048 | 1.00 |
| 64M cold | read+crc | 2604 | 134.8 | 2048 | 1.00 |
| 64M cold | read | 2606 | 120.6 | 2048 | 1.00 |
| 64M hot | read+sha256 | 2274 | 450.3 | 0 | 0 |
| 64M hot | read+crc | 18363 | 55.76 | 0 | 0 |
| 64M hot | read | 25027 | 40.91 | 0 | 0 |

