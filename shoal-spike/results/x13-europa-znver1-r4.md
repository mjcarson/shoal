### Checks, round 4

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
| describe fixed | expand | 1.00 | 7655497 |
| describe uniform | expand | 1.00 | 8840342 |
| describe doublings | expand | 1.00 | 10308859 |
| describe table | expand | 1.00 | 7676124 |

### 1. Making bytes on one core, GiB/s, round 4

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build znver1 (avx2 aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa znver1

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 10.97 | 10.93 | 10.97 | 0.348 |
| fill 4K cold | splitmix | 19.63 | 19.61 | 19.64 | 0.194 |
| fill 4K cold | xoshiro | 8.13 | 8.13 | 8.14 | 0.469 |
| fill 4K cold | chacha8 | 6.16 | 6.09 | 6.16 | 0.619 |
| fill 4K cold | aes-ctr | 10.31 | 10.25 | 10.33 | 0.370 |
| fill+crc 4K cold | stamped | 13.28 | 13.25 | 13.30 | 0.287 |
| fill+crc 4K cold | splitmix | 14.65 | 14.64 | 14.65 | 0.260 |
| fill+crc 4K cold | xoshiro | 7.59 | 7.59 | 7.60 | 0.502 |
| fill+crc 4K cold | chacha8 | 5.54 | 5.51 | 5.55 | 0.689 |
| fill+crc 4K cold | aes-ctr | 7.84 | 7.81 | 7.88 | 0.487 |
| verify 4K cold | stamped | 17.63 | 17.63 | 17.66 | 0.216 |
| verify 4K cold | splitmix | 16.36 | 16.32 | 16.49 | 0.233 |
| verify 4K cold | xoshiro | 8.33 | 8.30 | 8.34 | 0.458 |
| verify 4K cold | chacha8 | 5.74 | 5.73 | 5.76 | 0.665 |
| verify 4K cold | aes-ctr | 9.55 | 9.52 | 9.57 | 0.400 |
| crc 4K cold | crc64nvme | 39.54 | 39.42 | 39.55 | 0.096 |
| copy 4K cold | memcpy | 6.37 | 6.36 | 6.37 | 0.599 |
| fill 4K hot | stamped | 86.72 | 86.22 | 87.60 | 0.044 |
| fill 4K hot | splitmix | 22.10 | 22.08 | 22.15 | 0.173 |
| fill 4K hot | xoshiro | 9.78 | 9.78 | 9.78 | 0.390 |
| fill 4K hot | chacha8 | 6.22 | 6.19 | 6.23 | 0.614 |
| fill 4K hot | aes-ctr | 10.78 | 10.77 | 10.79 | 0.354 |
| fill+crc 4K hot | stamped | 37.58 | 37.55 | 37.59 | 0.102 |
| fill+crc 4K hot | splitmix | 16.31 | 16.29 | 16.31 | 0.234 |
| fill+crc 4K hot | xoshiro | 8.24 | 8.24 | 8.25 | 0.463 |
| fill+crc 4K hot | chacha8 | 5.51 | 5.51 | 5.53 | 0.692 |
| fill+crc 4K hot | aes-ctr | 8.93 | 8.90 | 8.93 | 0.427 |
| verify 4K hot | stamped | 54.00 | 53.85 | 54.01 | 0.071 |
| verify 4K hot | splitmix | 19.53 | 19.53 | 19.53 | 0.195 |
| verify 4K hot | xoshiro | 9.00 | 9.00 | 9.01 | 0.424 |
| verify 4K hot | chacha8 | 5.96 | 5.96 | 5.96 | 0.640 |
| verify 4K hot | aes-ctr | 9.97 | 9.96 | 9.97 | 0.383 |
| crc 4K hot | crc64nvme | 55.97 | 55.95 | 56.02 | 0.068 |
| copy 4K hot | memcpy | 149.1 | 149.0 | 149.1 | 0.026 |
| fill 64K cold | stamped | 11.61 | 11.59 | 11.62 | 5.26 |
| fill 64K cold | splitmix | 19.84 | 19.83 | 19.87 | 3.08 |
| fill 64K cold | xoshiro | 8.67 | 8.66 | 8.68 | 7.04 |
| fill 64K cold | chacha8 | 6.43 | 6.43 | 6.45 | 9.49 |
| fill 64K cold | aes-ctr | 8.51 | 8.47 | 8.52 | 7.17 |
| fill+crc 64K cold | stamped | 12.07 | 12.02 | 12.07 | 5.06 |
| fill+crc 64K cold | splitmix | 15.64 | 15.64 | 15.67 | 3.90 |
| fill+crc 64K cold | xoshiro | 6.97 | 6.96 | 6.98 | 8.75 |
| fill+crc 64K cold | chacha8 | 5.90 | 5.90 | 5.90 | 10.34 |
| fill+crc 64K cold | aes-ctr | 7.65 | 7.61 | 7.66 | 7.98 |
| verify 64K cold | stamped | 10.71 | 10.70 | 10.76 | 5.70 |
| verify 64K cold | splitmix | 11.39 | 11.37 | 11.42 | 5.36 |
| verify 64K cold | xoshiro | 7.15 | 7.14 | 7.16 | 8.53 |
| verify 64K cold | chacha8 | 5.14 | 5.13 | 5.14 | 11.87 |
| verify 64K cold | aes-ctr | 7.76 | 7.75 | 7.76 | 7.87 |
| crc 64K cold | crc64nvme | 39.91 | 39.89 | 39.92 | 1.53 |
| copy 64K cold | memcpy | 9.12 | 9.05 | 9.20 | 6.69 |
| fill 64K hot | stamped | 64.75 | 64.75 | 64.78 | 0.943 |
| fill 64K hot | splitmix | 22.83 | 22.83 | 22.84 | 2.67 |
| fill 64K hot | xoshiro | 10.32 | 10.30 | 10.32 | 5.91 |
| fill 64K hot | chacha8 | 6.51 | 6.51 | 6.52 | 9.37 |
| fill 64K hot | aes-ctr | 11.76 | 11.75 | 11.77 | 5.19 |
| fill+crc 64K hot | stamped | 35.10 | 35.09 | 35.11 | 1.74 |
| fill+crc 64K hot | splitmix | 17.57 | 17.57 | 17.57 | 3.47 |
| fill+crc 64K hot | xoshiro | 9.05 | 9.05 | 9.05 | 6.74 |
| fill+crc 64K hot | chacha8 | 5.97 | 5.96 | 5.97 | 10.23 |
| fill+crc 64K hot | aes-ctr | 10.10 | 10.10 | 10.11 | 6.04 |
| verify 64K hot | stamped | 34.97 | 34.95 | 34.98 | 1.75 |
| verify 64K hot | splitmix | 17.59 | 17.57 | 17.60 | 3.47 |
| verify 64K hot | xoshiro | 9.05 | 9.05 | 9.05 | 6.74 |
| verify 64K hot | chacha8 | 5.97 | 5.97 | 5.97 | 10.22 |
| verify 64K hot | aes-ctr | 10.11 | 10.10 | 10.12 | 6.04 |
| crc 64K hot | crc64nvme | 75.80 | 75.79 | 75.84 | 0.805 |
| copy 64K hot | memcpy | 75.19 | 75.17 | 75.30 | 0.812 |
| fill 1M cold | stamped | 12.09 | 12.09 | 12.13 | 80.76 |
| fill 1M cold | splitmix | 19.97 | 19.97 | 19.98 | 48.90 |
| fill 1M cold | xoshiro | 8.87 | 8.83 | 8.90 | 110.1 |
| fill 1M cold | chacha8 | 6.42 | 6.42 | 6.43 | 152.1 |
| fill 1M cold | aes-ctr | 8.55 | 8.54 | 8.56 | 114.2 |
| fill+crc 1M cold | stamped | 10.89 | 10.87 | 10.90 | 89.68 |
| fill+crc 1M cold | splitmix | 15.11 | 15.11 | 15.11 | 64.62 |
| fill+crc 1M cold | xoshiro | 7.55 | 7.55 | 7.55 | 129.3 |
| fill+crc 1M cold | chacha8 | 5.90 | 5.90 | 5.90 | 165.6 |
| fill+crc 1M cold | aes-ctr | 7.70 | 7.69 | 7.71 | 126.8 |
| verify 1M cold | stamped | 17.53 | 17.51 | 17.64 | 55.70 |
| verify 1M cold | splitmix | 14.29 | 14.25 | 14.32 | 68.33 |
| verify 1M cold | xoshiro | 8.01 | 8.01 | 8.02 | 121.9 |
| verify 1M cold | chacha8 | 5.54 | 5.53 | 5.55 | 176.2 |
| verify 1M cold | aes-ctr | 8.82 | 8.80 | 8.85 | 110.7 |
| crc 1M cold | crc64nvme | 39.50 | 39.48 | 39.52 | 24.72 |
| copy 1M cold | memcpy | 9.99 | 9.88 | 10.01 | 97.74 |
| fill 1M hot | stamped | 55.05 | 55.02 | 55.06 | 17.74 |
| fill 1M hot | splitmix | 22.85 | 22.85 | 22.87 | 42.73 |
| fill 1M hot | xoshiro | 10.28 | 10.28 | 10.29 | 94.96 |
| fill 1M hot | chacha8 | 6.53 | 6.53 | 6.53 | 149.5 |
| fill 1M hot | aes-ctr | 11.59 | 11.59 | 11.60 | 84.22 |
| fill+crc 1M hot | stamped | 31.79 | 31.78 | 31.79 | 30.72 |
| fill+crc 1M hot | splitmix | 17.59 | 17.59 | 17.60 | 55.51 |
| fill+crc 1M hot | xoshiro | 9.04 | 9.03 | 9.04 | 108.1 |
| fill+crc 1M hot | chacha8 | 6.03 | 6.03 | 6.03 | 161.9 |
| fill+crc 1M hot | aes-ctr | 10.00 | 10.00 | 10.00 | 97.66 |
| verify 1M hot | stamped | 29.78 | 29.72 | 29.79 | 32.80 |
| verify 1M hot | splitmix | 16.75 | 16.75 | 16.75 | 58.30 |
| verify 1M hot | xoshiro | 8.82 | 8.81 | 8.82 | 110.8 |
| verify 1M hot | chacha8 | 5.93 | 5.93 | 5.93 | 164.7 |
| verify 1M hot | aes-ctr | 9.74 | 9.74 | 9.75 | 100.2 |
| crc 1M hot | crc64nvme | 76.03 | 76.02 | 76.03 | 12.84 |
| copy 1M hot | memcpy | 62.55 | 62.52 | 62.57 | 15.61 |

### 2. Against a server that discards, round 4

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build znver1 (avx2 aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | fill+crc aes-ctr | 4999 | 204.8 | 1.000 | 1.00 | 5000 | 120.4 | 0.561 | 253.1 | 0 | 0 |
| put 1M plain | fill aes-ctr | 5326 | 192.2 | 1.000 | 1.00 | 5327 | 120.6 | 0.600 | 313.3 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 3778 | 271.0 | 1.000 | 1.00 | 3778 | 121.7 | 0.422 | 393.5 | 0 | 0 |
| put 1M plain | fill chacha8 | 3977 | 257.5 | 1.000 | 1.00 | 3977 | 121.4 | 0.444 | 377.4 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 4788 | 213.8 | 1.000 | 1.00 | 4788 | 121.6 | 0.541 | 338.7 | 0 | 0 |
| put 1M plain | fill xoshiro | 5098 | 200.8 | 1.000 | 1.00 | 5099 | 121.0 | 0.575 | 320.1 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 6256 | 163.7 | 1.000 | 1.00 | 6257 | 120.8 | 0.711 | 285.1 | 0 | 0 |
| put 1M plain | fill splitmix | 6810 | 150.4 | 1.000 | 1.00 | 6811 | 120.4 | 0.773 | 273.6 | 0 | 0 |
| put 1M plain | fill+crc stamped | 6763 | 151.4 | 1.000 | 1.00 | 6763 | 121.5 | 0.775 | 274.9 | 0 | 0 |
| put 1M plain | fill stamped | 7404 | 138.3 | 1.00 | 1.00 | 7403 | 121.1 | 0.848 | 257.8 | 0 | 0 |
| put 1M plain | pattern | 7075 | 144.7 | 1.000 | 1.00 | 7076 | 148.8 | 1.000 | 296.4 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 6294 | 153.1 | 0.941 | 0.958 | 6687 | 146.3 | 0.871 | 301.2 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 4213 | 225.8 | 0.929 | 0.945 | 4536 | 145.6 | 0.572 | 371.3 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 5824 | 163.8 | 0.931 | 0.949 | 6253 | 149.4 | 0.822 | 313.6 | 0 | 0 |
| get 1M plain | regenerate splitmix | 5615 | 130.7 | 0.717 | 0.737 | 7836 | 187.4 | 1.000 | 320.9 | 0 | 0 |
| get 1M plain | regenerate stamped | 5381 | 132.4 | 0.696 | 0.714 | 7734 | 195.3 | 0.999 | 323.8 | 0 | 0 |
| get 1M plain | crc | 5551 | 116.0 | 0.629 | 0.637 | 8827 | 189.5 | 0.999 | 287.7 | 0 | 0 |
| get 1M plain | none | 5729 | 106.4 | 0.595 | 0.591 | 9627 | 183.7 | 1.000 | 265.2 | 0 | 0 |
| put 1M ktls | fill+crc aes-ctr | 2014 | 348.9 | 0.686 | 0.685 | 2935 | 399.5 | 0.759 | 771.8 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 2792 | 366.2 | 0.999 | 0.998 | 2796 | 349.3 | 0.925 | 726.1 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 1546 | 408.2 | 0.616 | 0.619 | 2508 | 502.2 | 0.731 | 935.0 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 2433 | 411.9 | 0.979 | 0.978 | 2486 | 334.2 | 0.767 | 756.6 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 2019 | 357.2 | 0.704 | 0.706 | 2866 | 395.8 | 0.754 | 775.8 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 2748 | 372.3 | 0.999 | 0.998 | 2751 | 348.9 | 0.909 | 739.1 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 1756 | 301.4 | 0.517 | 0.515 | 3398 | 442.3 | 0.732 | 763.7 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 1790 | 291.3 | 0.509 | 0.513 | 3515 | 444.7 | 0.751 | 756.1 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 2920 | 329.3 | 0.939 | 0.941 | 3110 | 335.7 | 0.930 | 676.7 | 0 | 1.00 |
| put 1M ktls | fill stamped | 1810 | 280.2 | 0.495 | 0.497 | 3655 | 441.0 | 0.752 | 735.3 | 0 | 1.00 |
| put 1M ktls | pattern | 1724 | 325.3 | 0.548 | 0.549 | 3148 | 439.2 | 0.712 | 781.6 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 1292 | 500.9 | 0.632 | 0.657 | 2044 | 352.8 | 0.418 | 889.1 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 1188 | 568.8 | 0.660 | 0.684 | 1800 | 354.5 | 0.385 | 989.4 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 1267 | 514.2 | 0.636 | 0.659 | 1991 | 353.2 | 0.410 | 900.5 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 1354 | 463.9 | 0.614 | 0.640 | 2207 | 350.4 | 0.437 | 843.8 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 1370 | 452.0 | 0.605 | 0.631 | 2265 | 353.2 | 0.446 | 839.8 | 0 | 1.00 |
| get 1M ktls | crc | 1449 | 415.5 | 0.588 | 0.616 | 2464 | 348.7 | 0.467 | 801.2 | 0 | 1.00 |
| get 1M ktls | none | 1491 | 396.9 | 0.578 | 0.611 | 2580 | 346.7 | 0.478 | 790.8 | 0 | 1.00 |

