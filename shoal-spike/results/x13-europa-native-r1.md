### Checks, round 1

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
| describe fixed | expand | 1.00 | 7668504 |
| describe uniform | expand | 1.00 | 8811379 |
| describe doublings | expand | 1.00 | 10514732 |
| describe table | expand | 1.00 | 6860075 |

### 1. Making bytes on one core, GiB/s, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 11.27 | 10.91 | 11.34 | 0.339 |
| fill 4K cold | splitmix | 22.63 | 22.56 | 22.91 | 0.169 |
| fill 4K cold | xoshiro | 14.42 | 14.39 | 14.53 | 0.264 |
| fill 4K cold | chacha8 | 6.82 | 6.82 | 6.82 | 0.559 |
| fill 4K cold | aes-ctr | 10.33 | 10.32 | 10.35 | 0.369 |
| fill+crc 4K cold | stamped | 13.86 | 13.79 | 13.87 | 0.275 |
| fill+crc 4K cold | splitmix | 22.73 | 22.70 | 23.03 | 0.168 |
| fill+crc 4K cold | xoshiro | 12.13 | 12.12 | 12.14 | 0.314 |
| fill+crc 4K cold | chacha8 | 6.30 | 6.30 | 6.31 | 0.605 |
| fill+crc 4K cold | aes-ctr | 7.44 | 7.43 | 7.44 | 0.513 |
| verify 4K cold | stamped | 16.91 | 16.57 | 17.00 | 0.226 |
| verify 4K cold | splitmix | 31.94 | 31.85 | 32.08 | 0.119 |
| verify 4K cold | xoshiro | 14.01 | 14.01 | 14.10 | 0.272 |
| verify 4K cold | chacha8 | 6.52 | 6.52 | 6.52 | 0.585 |
| verify 4K cold | aes-ctr | 9.79 | 9.77 | 9.80 | 0.390 |
| crc 4K cold | crc64nvme | 39.59 | 39.58 | 39.63 | 0.096 |
| copy 4K cold | memcpy | 6.47 | 6.45 | 6.47 | 0.590 |
| fill 4K hot | stamped | 87.00 | 86.65 | 87.52 | 0.044 |
| fill 4K hot | splitmix | 59.70 | 59.67 | 59.72 | 0.064 |
| fill 4K hot | xoshiro | 20.46 | 20.45 | 20.48 | 0.186 |
| fill 4K hot | chacha8 | 6.82 | 6.82 | 6.83 | 0.559 |
| fill 4K hot | aes-ctr | 10.95 | 10.93 | 10.95 | 0.349 |
| fill+crc 4K hot | stamped | 39.46 | 39.29 | 39.48 | 0.097 |
| fill+crc 4K hot | splitmix | 32.81 | 32.80 | 32.82 | 0.116 |
| fill+crc 4K hot | xoshiro | 15.24 | 15.24 | 15.24 | 0.250 |
| fill+crc 4K hot | chacha8 | 6.38 | 6.37 | 6.38 | 0.598 |
| fill+crc 4K hot | aes-ctr | 9.42 | 9.42 | 9.43 | 0.405 |
| verify 4K hot | stamped | 54.17 | 54.03 | 54.19 | 0.070 |
| verify 4K hot | splitmix | 43.25 | 43.24 | 43.27 | 0.088 |
| verify 4K hot | xoshiro | 17.41 | 17.41 | 17.43 | 0.219 |
| verify 4K hot | chacha8 | 6.67 | 6.67 | 6.67 | 0.572 |
| verify 4K hot | aes-ctr | 10.21 | 10.21 | 10.22 | 0.374 |
| crc 4K hot | crc64nvme | 68.28 | 68.28 | 68.44 | 0.056 |
| copy 4K hot | memcpy | 149.5 | 149.5 | 149.5 | 0.026 |
| fill 64K cold | stamped | 12.59 | 12.28 | 12.60 | 4.85 |
| fill 64K cold | splitmix | 22.59 | 22.13 | 22.80 | 2.70 |
| fill 64K cold | xoshiro | 15.83 | 15.67 | 15.86 | 3.86 |
| fill 64K cold | chacha8 | 6.95 | 6.95 | 6.96 | 8.78 |
| fill 64K cold | aes-ctr | 8.53 | 8.53 | 8.54 | 7.16 |
| fill+crc 64K cold | stamped | 12.42 | 12.29 | 12.45 | 4.92 |
| fill+crc 64K cold | splitmix | 18.63 | 18.50 | 18.70 | 3.28 |
| fill+crc 64K cold | xoshiro | 11.94 | 11.89 | 11.95 | 5.11 |
| fill+crc 64K cold | chacha8 | 6.45 | 6.45 | 6.45 | 9.47 |
| fill+crc 64K cold | aes-ctr | 7.69 | 7.69 | 7.70 | 7.93 |
| verify 64K cold | stamped | 10.33 | 10.13 | 10.50 | 5.91 |
| verify 64K cold | splitmix | 15.80 | 15.79 | 15.82 | 3.86 |
| verify 64K cold | xoshiro | 11.40 | 11.32 | 11.42 | 5.36 |
| verify 64K cold | chacha8 | 5.35 | 5.34 | 5.36 | 11.42 |
| verify 64K cold | aes-ctr | 7.76 | 7.76 | 7.76 | 7.87 |
| crc 64K cold | crc64nvme | 38.67 | 38.66 | 38.72 | 1.58 |
| copy 64K cold | memcpy | 9.87 | 9.84 | 9.87 | 6.19 |
| fill 64K hot | stamped | 64.92 | 64.87 | 64.93 | 0.940 |
| fill 64K hot | splitmix | 59.66 | 59.61 | 59.66 | 1.02 |
| fill 64K hot | xoshiro | 23.12 | 23.11 | 23.14 | 2.64 |
| fill 64K hot | chacha8 | 7.10 | 7.10 | 7.10 | 8.60 |
| fill 64K hot | aes-ctr | 11.81 | 11.81 | 11.81 | 5.17 |
| fill+crc 64K hot | stamped | 35.23 | 35.20 | 35.26 | 1.73 |
| fill+crc 64K hot | splitmix | 33.62 | 33.62 | 33.62 | 1.82 |
| fill+crc 64K hot | xoshiro | 17.15 | 17.14 | 17.15 | 3.56 |
| fill+crc 64K hot | chacha8 | 6.52 | 6.52 | 6.52 | 9.36 |
| fill+crc 64K hot | aes-ctr | 10.23 | 10.23 | 10.23 | 5.97 |
| verify 64K hot | stamped | 35.11 | 35.10 | 35.11 | 1.74 |
| verify 64K hot | splitmix | 33.59 | 33.58 | 33.60 | 1.82 |
| verify 64K hot | xoshiro | 17.14 | 17.14 | 17.14 | 3.56 |
| verify 64K hot | chacha8 | 6.52 | 6.52 | 6.52 | 9.36 |
| verify 64K hot | aes-ctr | 10.24 | 10.23 | 10.24 | 5.96 |
| crc 64K hot | crc64nvme | 76.53 | 76.51 | 76.57 | 0.797 |
| copy 64K hot | memcpy | 76.83 | 76.81 | 76.87 | 0.794 |
| fill 1M cold | stamped | 12.85 | 12.71 | 12.88 | 75.98 |
| fill 1M cold | splitmix | 23.20 | 23.12 | 23.27 | 42.10 |
| fill 1M cold | xoshiro | 16.01 | 16.00 | 16.13 | 61.00 |
| fill 1M cold | chacha8 | 6.93 | 6.92 | 6.93 | 141.0 |
| fill 1M cold | aes-ctr | 8.57 | 8.57 | 8.57 | 114.0 |
| fill+crc 1M cold | stamped | 11.02 | 11.01 | 11.03 | 88.65 |
| fill+crc 1M cold | splitmix | 17.36 | 17.36 | 17.37 | 56.25 |
| fill+crc 1M cold | xoshiro | 12.08 | 12.07 | 12.09 | 80.83 |
| fill+crc 1M cold | chacha8 | 6.29 | 6.29 | 6.29 | 155.3 |
| fill+crc 1M cold | aes-ctr | 7.71 | 7.70 | 7.71 | 126.7 |
| verify 1M cold | stamped | 17.29 | 17.22 | 17.46 | 56.48 |
| verify 1M cold | splitmix | 23.26 | 23.23 | 23.35 | 41.99 |
| verify 1M cold | xoshiro | 13.77 | 13.76 | 13.86 | 70.90 |
| verify 1M cold | chacha8 | 5.98 | 5.98 | 5.98 | 163.4 |
| verify 1M cold | aes-ctr | 8.85 | 8.85 | 8.85 | 110.4 |
| crc 1M cold | crc64nvme | 40.62 | 40.60 | 40.65 | 24.04 |
| copy 1M cold | memcpy | 10.67 | 10.67 | 10.69 | 91.53 |
| fill 1M hot | stamped | 55.33 | 55.31 | 55.38 | 17.65 |
| fill 1M hot | splitmix | 59.59 | 59.58 | 59.59 | 16.39 |
| fill 1M hot | xoshiro | 22.85 | 22.85 | 22.87 | 42.73 |
| fill 1M hot | chacha8 | 7.10 | 7.10 | 7.11 | 137.5 |
| fill 1M hot | aes-ctr | 11.65 | 11.63 | 11.66 | 83.80 |
| fill+crc 1M hot | stamped | 31.91 | 31.90 | 31.92 | 30.61 |
| fill+crc 1M hot | splitmix | 33.52 | 33.51 | 33.55 | 29.13 |
| fill+crc 1M hot | xoshiro | 17.01 | 17.01 | 17.01 | 57.41 |
| fill+crc 1M hot | chacha8 | 6.51 | 6.51 | 6.51 | 150.1 |
| fill+crc 1M hot | aes-ctr | 10.11 | 10.11 | 10.11 | 96.61 |
| verify 1M hot | stamped | 30.00 | 30.00 | 30.07 | 32.55 |
| verify 1M hot | splitmix | 30.64 | 30.64 | 30.67 | 31.87 |
| verify 1M hot | xoshiro | 16.23 | 16.23 | 16.24 | 60.15 |
| verify 1M hot | chacha8 | 6.39 | 6.39 | 6.39 | 152.9 |
| verify 1M hot | aes-ctr | 9.80 | 9.79 | 9.80 | 99.69 |
| crc 1M hot | crc64nvme | 76.23 | 76.21 | 76.31 | 12.81 |
| copy 1M hot | memcpy | 63.25 | 63.23 | 63.30 | 15.44 |

### 3a. Making bytes on several cores, no wire, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | GiB/s, all | GiB/s, least core |
| --- | --- | --- | --- |
| fill+crc 1M cold x1 | stamped | 10.95 | 10.95 |
| fill+crc 1M cold x1 | splitmix | 17.73 | 17.73 |
| fill+crc 1M cold x1 | xoshiro | 12.86 | 12.86 |
| fill+crc 1M cold x1 | chacha8 | 6.34 | 6.34 |
| fill+crc 1M cold x1 | aes-ctr | 7.62 | 7.62 |
| fill+crc 1M cold x2 | stamped | 15.50 | 7.75 |
| fill+crc 1M cold x2 | splitmix | 18.87 | 9.43 |
| fill+crc 1M cold x2 | xoshiro | 14.92 | 7.43 |
| fill+crc 1M cold x2 | chacha8 | 11.57 | 5.78 |
| fill+crc 1M cold x2 | aes-ctr | 14.84 | 7.42 |
| fill+crc 1M cold x4 | stamped | 17.41 | 4.35 |
| fill+crc 1M cold x4 | splitmix | 18.82 | 4.69 |
| fill+crc 1M cold x4 | xoshiro | 18.88 | 4.68 |
| fill+crc 1M cold x4 | chacha8 | 18.41 | 4.59 |
| fill+crc 1M cold x4 | aes-ctr | 20.39 | 5.08 |

### 2. Against a server that discards, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | pattern | 6353 | 161.1 | 1.000 | 1.00 | 6354 | 165.7 | 1.000 | 333.9 | 0 | 0 |
| put 1M plain | fill stamped | 6136 | 166.9 | 1.000 | 1.00 | 6137 | 149.1 | 0.866 | 321.4 | 0 | 0 |
| put 1M plain | fill+crc stamped | 5706 | 179.4 | 1.000 | 1.00 | 5706 | 149.1 | 0.803 | 336.6 | 0 | 0 |
| put 1M plain | fill splitmix | 6793 | 150.7 | 1.000 | 1.00 | 6794 | 148.8 | 0.960 | 349.9 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 6272 | 163.2 | 1.000 | 1.00 | 6273 | 148.5 | 0.882 | 313.7 | 0 | 0 |
| put 1M plain | fill xoshiro | 5821 | 175.9 | 1.000 | 1.00 | 5822 | 148.9 | 0.819 | 328.2 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 5427 | 188.7 | 1.000 | 1.00 | 5428 | 148.8 | 0.761 | 341.4 | 0 | 0 |
| put 1M plain | fill chacha8 | 3754 | 272.8 | 1.000 | 1.00 | 3754 | 150.1 | 0.523 | 430.9 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 3741 | 273.7 | 1.000 | 1.00 | 3741 | 135.3 | 0.467 | 417.1 | 0 | 0 |
| put 1M plain | fill aes-ctr | 4655 | 219.9 | 1.000 | 1.00 | 4656 | 149.5 | 0.652 | 374.8 | 0 | 0 |
| put 1M plain | fill+crc aes-ctr | 4409 | 232.2 | 1.000 | 1.00 | 4409 | 149.4 | 0.616 | 384.5 | 0 | 0 |
| get 1M plain | none | 5772 | 106.0 | 0.597 | 0.615 | 9663 | 182.3 | 1.000 | 269.9 | 0 | 0 |
| get 1M plain | crc | 5569 | 115.5 | 0.628 | 0.645 | 8863 | 189.0 | 1.000 | 286.4 | 0 | 0 |
| get 1M plain | regenerate stamped | 5380 | 132.9 | 0.698 | 0.716 | 7706 | 195.6 | 1.000 | 321.6 | 0 | 0 |
| get 1M plain | regenerate splitmix | 6826 | 108.0 | 0.720 | 0.761 | 9483 | 154.2 | 1.000 | 256.5 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 6897 | 121.4 | 0.817 | 0.850 | 8438 | 152.6 | 1.000 | 268.1 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 4489 | 211.5 | 0.927 | 0.943 | 4842 | 148.1 | 0.622 | 358.9 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 6228 | 154.4 | 0.939 | 0.954 | 6632 | 145.9 | 0.860 | 299.5 | 0 | 0 |
| put 1M ktls | pattern | 1748 | 275.0 | 0.470 | 0.488 | 3723 | 441.2 | 0.726 | 741.4 | 0 | 1.00 |
| put 1M ktls | fill stamped | 1825 | 280.4 | 0.500 | 0.503 | 3652 | 444.6 | 0.765 | 747.4 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 1772 | 292.2 | 0.506 | 0.505 | 3505 | 445.1 | 0.743 | 754.6 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 1873 | 266.7 | 0.488 | 0.489 | 3840 | 443.6 | 0.784 | 723.7 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 2807 | 309.0 | 0.847 | 0.851 | 3314 | 330.3 | 0.879 | 655.0 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 2152 | 297.8 | 0.626 | 0.627 | 3439 | 380.8 | 0.773 | 697.5 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 1689 | 302.2 | 0.499 | 0.499 | 3389 | 468.2 | 0.746 | 787.8 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 2444 | 400.9 | 0.957 | 0.958 | 2555 | 333.5 | 0.769 | 739.0 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 2440 | 411.8 | 0.981 | 0.980 | 2487 | 332.4 | 0.765 | 755.3 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 2101 | 337.6 | 0.693 | 0.693 | 3033 | 387.4 | 0.768 | 737.8 | 0 | 1.00 |
| put 1M ktls | fill+crc aes-ctr | 1807 | 344.4 | 0.608 | 0.608 | 2973 | 440.3 | 0.750 | 800.2 | 0 | 1.00 |
| get 1M ktls | none | 1745 | 358.0 | 0.610 | 0.629 | 2860 | 311.1 | 0.503 | 685.2 | 0 | 1.00 |
| get 1M ktls | crc | 1565 | 407.9 | 0.624 | 0.654 | 2510 | 301.9 | 0.435 | 743.0 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 1411 | 450.2 | 0.621 | 0.647 | 2274 | 333.5 | 0.433 | 812.4 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 1629 | 391.7 | 0.623 | 0.642 | 2614 | 321.3 | 0.484 | 741.7 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 1423 | 459.8 | 0.639 | 0.669 | 2227 | 318.1 | 0.415 | 814.6 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 1205 | 582.6 | 0.686 | 0.713 | 1758 | 323.7 | 0.354 | 934.3 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 1347 | 502.9 | 0.662 | 0.687 | 2036 | 316.5 | 0.389 | 842.2 | 0 | 1.00 |

### 3b. The put from several client cores, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain x1 | fill+crc splitmix | 7258 | 141.1 | 1.000 | 1.00 | 7258 | 124.2 | 0.853 | 264.4 | 0 | 0 |
| put 1M plain x1 | fill+crc xoshiro | 5446 | 188.0 | 1.000 | 1.00 | 5447 | 148.7 | 0.764 | 339.9 | 0 | 0 |
| put 1M plain x1 | fill+crc chacha8 | 3582 | 285.9 | 1.000 | 1.00 | 3582 | 150.2 | 0.498 | 439.6 | 0 | 0 |
| put 1M plain x1 | fill+crc aes-ctr | 4431 | 231.1 | 1.000 | 1.00 | 4431 | 148.9 | 0.617 | 379.9 | 0 | 0 |
| put 1M plain x2 | fill+crc splitmix | 13354 | 153.3 | 1.000 | 1.00 | 6679 | 133.9 | 0.882 | 282.4 | 0 | 0 |
| put 1M plain x2 | fill+crc xoshiro | 11630 | 176.1 | 1.000 | 1.00 | 5816 | 130.4 | 0.749 | 299.5 | 0 | 0 |
| put 1M plain x2 | fill+crc chacha8 | 7402 | 276.7 | 1.000 | 1.00 | 3701 | 133.4 | 0.500 | 409.7 | 0 | 0 |
| put 1M plain x2 | fill+crc aes-ctr | 9148 | 223.9 | 1.000 | 1.00 | 4574 | 133.0 | 0.605 | 352.8 | 0 | 0 |
| put 1M plain x4 | fill+crc splitmix | 24344 | 168.2 | 1.000 | 1.00 | 6087 | 145.3 | 0.880 | 309.0 | 0 | 0 |
| put 1M plain x4 | fill+crc xoshiro | 21109 | 194.0 | 1.000 | 1.00 | 5278 | 143.9 | 0.761 | 333.6 | 0 | 0 |
| put 1M plain x4 | fill+crc chacha8 | 14671 | 279.2 | 1.000 | 1.00 | 3668 | 129.5 | 0.502 | 404.9 | 0 | 0 |
| put 1M plain x4 | fill+crc aes-ctr | 17053 | 240.2 | 1.000 | 1.00 | 4264 | 143.3 | 0.616 | 379.8 | 0 | 0 |

### 4. A folder of real files, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | MiB/s | cpu ms/GiB | device MiB read | from device |
| --- | --- | --- | --- | --- | --- |
| 64K cold | read | 1094 | 436.4 | 2048 | 1.00 |
| 64K cold | read+crc | 1103 | 425.9 | 2048 | 1.00 |
| 64K cold | read+sha256 | 767.7 | 830.3 | 2048 | 1.00 |
| 64K hot | read | 7029 | 145.7 | 0 | 0 |
| 64K hot | read+crc | 6434 | 159.1 | 0 | 0 |
| 64K hot | read+sha256 | 1831 | 559.2 | 0 | 0 |
| 1M cold | read | 2154 | 199.4 | 2048 | 1.00 |
| 1M cold | read+crc | 2126 | 182.8 | 2048 | 1.000 |
| 1M cold | read+sha256 | 1165 | 577.5 | 2048 | 1.00 |
| 1M hot | read | 16961 | 60.37 | 0 | 0 |
| 1M hot | read+crc | 14434 | 70.94 | 0 | 0 |
| 1M hot | read+sha256 | 2207 | 463.9 | 0 | 0 |
| 64M cold | read | 2610 | 120.8 | 2048 | 1.00 |
| 64M cold | read+crc | 2606 | 131.0 | 2048 | 1.00 |
| 64M cold | read+sha256 | 1315 | 526.6 | 2048 | 1.00 |
| 64M hot | read | 20972 | 48.82 | 0 | 0 |
| 64M hot | read+crc | 17191 | 59.55 | 0 | 0 |
| 64M hot | read+sha256 | 2272 | 450.6 | 0 | 0 |

