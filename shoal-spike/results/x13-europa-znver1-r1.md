### Checks, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build znver1 (avx2 aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa znver1

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
| describe fixed | expand | 1.00 | 7738817 |
| describe uniform | expand | 1.00 | 8618919 |
| describe doublings | expand | 1.00 | 10091200 |
| describe table | expand | 1.00 | 5251221 |

### 1. Making bytes on one core, GiB/s, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build znver1 (avx2 aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa znver1

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 10.95 | 10.44 | 10.97 | 0.348 |
| fill 4K cold | splitmix | 19.56 | 19.41 | 19.57 | 0.195 |
| fill 4K cold | xoshiro | 8.02 | 7.43 | 8.02 | 0.476 |
| fill 4K cold | chacha8 | 6.12 | 6.05 | 6.13 | 0.623 |
| fill 4K cold | aes-ctr | 10.29 | 10.28 | 10.29 | 0.371 |
| fill+crc 4K cold | stamped | 13.29 | 13.25 | 13.30 | 0.287 |
| fill+crc 4K cold | splitmix | 14.58 | 14.57 | 14.59 | 0.262 |
| fill+crc 4K cold | xoshiro | 7.38 | 7.34 | 7.38 | 0.517 |
| fill+crc 4K cold | chacha8 | 5.50 | 5.50 | 5.50 | 0.693 |
| fill+crc 4K cold | aes-ctr | 7.72 | 7.57 | 7.74 | 0.494 |
| verify 4K cold | stamped | 17.71 | 17.27 | 17.74 | 0.215 |
| verify 4K cold | splitmix | 16.34 | 16.31 | 16.38 | 0.233 |
| verify 4K cold | xoshiro | 8.29 | 8.27 | 8.37 | 0.460 |
| verify 4K cold | chacha8 | 5.70 | 5.69 | 5.72 | 0.670 |
| verify 4K cold | aes-ctr | 9.57 | 9.54 | 9.60 | 0.399 |
| crc 4K cold | crc64nvme | 39.86 | 39.82 | 39.87 | 0.096 |
| copy 4K cold | memcpy | 6.39 | 6.38 | 6.39 | 0.597 |
| fill 4K hot | stamped | 87.08 | 87.02 | 87.57 | 0.044 |
| fill 4K hot | splitmix | 22.04 | 22.02 | 22.06 | 0.173 |
| fill 4K hot | xoshiro | 9.87 | 9.87 | 9.88 | 0.386 |
| fill 4K hot | chacha8 | 6.20 | 6.20 | 6.20 | 0.616 |
| fill 4K hot | aes-ctr | 10.84 | 10.83 | 10.85 | 0.352 |
| fill+crc 4K hot | stamped | 37.78 | 37.72 | 37.85 | 0.101 |
| fill+crc 4K hot | splitmix | 16.28 | 16.28 | 16.30 | 0.234 |
| fill+crc 4K hot | xoshiro | 8.33 | 8.33 | 8.34 | 0.458 |
| fill+crc 4K hot | chacha8 | 5.50 | 5.49 | 5.50 | 0.694 |
| fill+crc 4K hot | aes-ctr | 9.02 | 9.00 | 9.04 | 0.423 |
| verify 4K hot | stamped | 53.88 | 53.68 | 53.93 | 0.071 |
| verify 4K hot | splitmix | 19.46 | 19.44 | 19.46 | 0.196 |
| verify 4K hot | xoshiro | 9.20 | 9.20 | 9.21 | 0.414 |
| verify 4K hot | chacha8 | 5.94 | 5.93 | 5.94 | 0.642 |
| verify 4K hot | aes-ctr | 10.07 | 9.98 | 10.08 | 0.379 |
| crc 4K hot | crc64nvme | 55.87 | 55.73 | 55.94 | 0.068 |
| copy 4K hot | memcpy | 148.7 | 148.7 | 148.7 | 0.026 |
| fill 64K cold | stamped | 11.62 | 11.58 | 11.62 | 5.25 |
| fill 64K cold | splitmix | 19.83 | 19.82 | 19.83 | 3.08 |
| fill 64K cold | xoshiro | 8.39 | 8.37 | 8.40 | 7.28 |
| fill 64K cold | chacha8 | 6.45 | 6.45 | 6.45 | 9.47 |
| fill 64K cold | aes-ctr | 8.62 | 8.62 | 8.63 | 7.08 |
| fill+crc 64K cold | stamped | 12.06 | 12.01 | 12.09 | 5.06 |
| fill+crc 64K cold | splitmix | 15.65 | 15.64 | 15.65 | 3.90 |
| fill+crc 64K cold | xoshiro | 7.26 | 7.24 | 7.26 | 8.41 |
| fill+crc 64K cold | chacha8 | 5.88 | 5.87 | 5.89 | 10.39 |
| fill+crc 64K cold | aes-ctr | 7.69 | 7.63 | 7.71 | 7.93 |
| verify 64K cold | stamped | 10.55 | 10.50 | 10.56 | 5.78 |
| verify 64K cold | splitmix | 11.62 | 11.61 | 11.63 | 5.25 |
| verify 64K cold | xoshiro | 7.71 | 7.70 | 7.71 | 7.92 |
| verify 64K cold | chacha8 | 5.15 | 5.15 | 5.16 | 11.84 |
| verify 64K cold | aes-ctr | 7.76 | 7.76 | 7.76 | 7.87 |
| crc 64K cold | crc64nvme | 40.10 | 40.03 | 40.10 | 1.52 |
| copy 64K cold | memcpy | 9.23 | 9.22 | 9.24 | 6.62 |
| fill 64K hot | stamped | 64.76 | 64.66 | 64.77 | 0.942 |
| fill 64K hot | splitmix | 22.78 | 22.75 | 22.79 | 2.68 |
| fill 64K hot | xoshiro | 10.44 | 10.43 | 10.44 | 5.85 |
| fill 64K hot | chacha8 | 6.51 | 6.51 | 6.51 | 9.37 |
| fill 64K hot | aes-ctr | 11.76 | 11.76 | 11.76 | 5.19 |
| fill+crc 64K hot | stamped | 35.04 | 35.03 | 35.07 | 1.74 |
| fill+crc 64K hot | splitmix | 17.52 | 17.52 | 17.54 | 3.48 |
| fill+crc 64K hot | xoshiro | 9.14 | 9.14 | 9.15 | 6.68 |
| fill+crc 64K hot | chacha8 | 5.97 | 5.97 | 5.97 | 10.22 |
| fill+crc 64K hot | aes-ctr | 10.10 | 10.10 | 10.10 | 6.05 |
| verify 64K hot | stamped | 34.99 | 34.96 | 35.00 | 1.74 |
| verify 64K hot | splitmix | 17.55 | 17.54 | 17.56 | 3.48 |
| verify 64K hot | xoshiro | 9.14 | 9.14 | 9.14 | 6.68 |
| verify 64K hot | chacha8 | 5.98 | 5.98 | 5.98 | 10.21 |
| verify 64K hot | aes-ctr | 10.11 | 10.11 | 10.11 | 6.04 |
| crc 64K hot | crc64nvme | 75.67 | 75.63 | 75.68 | 0.807 |
| copy 64K hot | memcpy | 76.44 | 76.44 | 76.45 | 0.798 |
| fill 1M cold | stamped | 12.10 | 12.01 | 12.11 | 80.72 |
| fill 1M cold | splitmix | 19.95 | 19.94 | 19.95 | 48.95 |
| fill 1M cold | xoshiro | 8.69 | 8.67 | 8.72 | 112.4 |
| fill 1M cold | chacha8 | 6.46 | 6.46 | 6.46 | 151.1 |
| fill 1M cold | aes-ctr | 8.51 | 8.51 | 8.51 | 114.7 |
| fill+crc 1M cold | stamped | 10.77 | 10.77 | 10.77 | 90.67 |
| fill+crc 1M cold | splitmix | 14.99 | 14.98 | 14.99 | 65.14 |
| fill+crc 1M cold | xoshiro | 7.89 | 7.88 | 7.91 | 123.7 |
| fill+crc 1M cold | chacha8 | 5.87 | 5.87 | 5.87 | 166.4 |
| fill+crc 1M cold | aes-ctr | 7.68 | 7.67 | 7.68 | 127.2 |
| verify 1M cold | stamped | 17.23 | 16.94 | 17.52 | 56.67 |
| verify 1M cold | splitmix | 14.09 | 14.00 | 14.14 | 69.32 |
| verify 1M cold | xoshiro | 8.10 | 8.10 | 8.11 | 120.5 |
| verify 1M cold | chacha8 | 5.54 | 5.53 | 5.54 | 176.4 |
| verify 1M cold | aes-ctr | 8.78 | 8.76 | 8.79 | 111.3 |
| crc 1M cold | crc64nvme | 40.29 | 40.26 | 40.32 | 24.24 |
| copy 1M cold | memcpy | 10.14 | 10.02 | 10.18 | 96.34 |
| fill 1M hot | stamped | 55.23 | 55.22 | 55.24 | 17.68 |
| fill 1M hot | splitmix | 22.84 | 22.82 | 22.84 | 42.75 |
| fill 1M hot | xoshiro | 10.40 | 10.39 | 10.40 | 93.92 |
| fill 1M hot | chacha8 | 6.52 | 6.52 | 6.52 | 149.8 |
| fill 1M hot | aes-ctr | 11.48 | 11.47 | 11.48 | 85.10 |
| fill+crc 1M hot | stamped | 31.83 | 31.83 | 31.84 | 30.68 |
| fill+crc 1M hot | splitmix | 17.60 | 17.58 | 17.60 | 55.50 |
| fill+crc 1M hot | xoshiro | 9.12 | 9.11 | 9.12 | 107.1 |
| fill+crc 1M hot | chacha8 | 6.01 | 6.01 | 6.02 | 162.4 |
| fill+crc 1M hot | aes-ctr | 9.97 | 9.96 | 9.97 | 97.94 |
| verify 1M hot | stamped | 29.90 | 29.89 | 29.93 | 32.66 |
| verify 1M hot | splitmix | 16.71 | 16.71 | 16.71 | 58.43 |
| verify 1M hot | xoshiro | 8.88 | 8.88 | 8.88 | 110.0 |
| verify 1M hot | chacha8 | 5.91 | 5.91 | 5.91 | 165.3 |
| verify 1M hot | aes-ctr | 9.70 | 9.70 | 9.70 | 100.7 |
| crc 1M hot | crc64nvme | 76.51 | 76.50 | 76.51 | 12.76 |
| copy 1M hot | memcpy | 63.10 | 62.91 | 63.13 | 15.48 |

### 2. Against a server that discards, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build znver1 (avx2 aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | pattern | 6432 | 159.2 | 1.000 | 1.00 | 6432 | 163.6 | 1.000 | 329.5 | 0 | 0 |
| put 1M plain | fill stamped | 7423 | 137.9 | 1.000 | 1.00 | 7424 | 120.9 | 0.849 | 258.5 | 0 | 0 |
| put 1M plain | fill+crc stamped | 6805 | 150.5 | 1.000 | 1.00 | 6806 | 120.7 | 0.774 | 272.3 | 0 | 0 |
| put 1M plain | fill splitmix | 6764 | 151.4 | 1.000 | 1.00 | 6765 | 120.6 | 0.770 | 273.6 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 6213 | 164.8 | 1.000 | 1.00 | 6213 | 121.3 | 0.709 | 285.1 | 0 | 0 |
| put 1M plain | fill xoshiro | 5082 | 201.4 | 1.000 | 1.00 | 5084 | 120.9 | 0.573 | 324.3 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 4751 | 215.5 | 1.000 | 1.00 | 4752 | 121.8 | 0.538 | 340.5 | 0 | 0 |
| put 1M plain | fill chacha8 | 3964 | 258.3 | 1.000 | 1.00 | 3965 | 121.7 | 0.444 | 390.0 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 3473 | 294.8 | 1.000 | 1.00 | 3473 | 143.6 | 0.460 | 437.5 | 0 | 0 |
| put 1M plain | fill aes-ctr | 5306 | 193.0 | 1.000 | 1.00 | 5306 | 121.4 | 0.602 | 317.2 | 0 | 0 |
| put 1M plain | fill+crc aes-ctr | 4974 | 205.8 | 1.000 | 1.00 | 4975 | 120.2 | 0.557 | 336.3 | 0 | 0 |
| get 1M plain | none | 5703 | 107.0 | 0.596 | 0.616 | 9572 | 184.5 | 1.000 | 279.0 | 0 | 0 |
| get 1M plain | crc | 6389 | 107.9 | 0.673 | 0.705 | 9493 | 164.7 | 1.000 | 255.7 | 0 | 0 |
| get 1M plain | regenerate stamped | 5391 | 132.4 | 0.697 | 0.707 | 7733 | 195.1 | 1.000 | 315.2 | 0 | 0 |
| get 1M plain | regenerate splitmix | 6937 | 121.6 | 0.824 | 0.856 | 8419 | 151.7 | 1.000 | 268.6 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 5823 | 163.8 | 0.931 | 0.949 | 6253 | 148.9 | 0.819 | 312.2 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 4211 | 225.8 | 0.929 | 0.945 | 4534 | 145.3 | 0.570 | 371.0 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 6267 | 153.5 | 0.940 | 0.954 | 6670 | 146.3 | 0.868 | 303.5 | 0 | 0 |
| put 1M ktls | pattern | 2861 | 357.8 | 1.000 | 1.00 | 2862 | 325.1 | 0.881 | 690.5 | 0 | 1.00 |
| put 1M ktls | fill stamped | 1822 | 279.7 | 0.498 | 0.500 | 3661 | 443.4 | 0.765 | 737.2 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 1723 | 292.4 | 0.492 | 0.496 | 3502 | 458.2 | 0.745 | 773.4 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 1776 | 290.9 | 0.504 | 0.505 | 3521 | 444.3 | 0.744 | 751.9 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 1720 | 302.8 | 0.509 | 0.511 | 3382 | 458.9 | 0.744 | 789.2 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 2027 | 345.4 | 0.684 | 0.685 | 2965 | 397.0 | 0.759 | 764.8 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 1805 | 353.1 | 0.623 | 0.622 | 2900 | 442.7 | 0.753 | 818.9 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 2436 | 411.4 | 0.979 | 0.978 | 2489 | 333.3 | 0.766 | 766.7 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 1494 | 405.8 | 0.592 | 0.591 | 2524 | 514.6 | 0.724 | 951.0 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 2047 | 338.7 | 0.677 | 0.675 | 3023 | 397.0 | 0.766 | 756.4 | 0 | 1.00 |
| put 1M ktls | fill+crc aes-ctr | 1806 | 344.5 | 0.608 | 0.609 | 2972 | 441.1 | 0.751 | 820.7 | 0 | 1.00 |
| get 1M ktls | none | 1467 | 404.2 | 0.579 | 0.607 | 2533 | 360.5 | 0.490 | 806.6 | 0 | 1.00 |
| get 1M ktls | crc | 1480 | 407.5 | 0.589 | 0.618 | 2513 | 343.7 | 0.471 | 793.0 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 1369 | 453.5 | 0.606 | 0.634 | 2258 | 352.1 | 0.444 | 849.7 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 1480 | 422.2 | 0.610 | 0.633 | 2425 | 356.0 | 0.488 | 813.2 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 1271 | 515.1 | 0.639 | 0.664 | 1988 | 351.3 | 0.409 | 913.3 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 1190 | 569.5 | 0.662 | 0.685 | 1798 | 353.5 | 0.384 | 982.9 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 1279 | 507.6 | 0.634 | 0.660 | 2018 | 352.3 | 0.413 | 909.7 | 0 | 1.00 |

