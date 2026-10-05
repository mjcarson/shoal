### Checks, round 3

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
| describe fixed | expand | 1.00 | 7658961 |
| describe uniform | expand | 1.00 | 8762178 |
| describe doublings | expand | 1.00 | 9729621 |
| describe table | expand | 1.00 | 6620686 |

### 1. Making bytes on one core, GiB/s, round 3

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build znver1 (avx2 aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa znver1

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 10.98 | 10.64 | 10.99 | 0.347 |
| fill 4K cold | splitmix | 19.57 | 19.34 | 19.57 | 0.195 |
| fill 4K cold | xoshiro | 8.04 | 7.97 | 8.07 | 0.475 |
| fill 4K cold | chacha8 | 6.13 | 6.13 | 6.13 | 0.622 |
| fill 4K cold | aes-ctr | 10.29 | 10.28 | 10.29 | 0.371 |
| fill+crc 4K cold | stamped | 13.31 | 13.27 | 13.31 | 0.287 |
| fill+crc 4K cold | splitmix | 14.59 | 14.58 | 14.59 | 0.261 |
| fill+crc 4K cold | xoshiro | 7.37 | 7.34 | 7.37 | 0.518 |
| fill+crc 4K cold | chacha8 | 5.51 | 5.51 | 5.51 | 0.692 |
| fill+crc 4K cold | aes-ctr | 7.85 | 7.74 | 7.88 | 0.486 |
| verify 4K cold | stamped | 17.74 | 17.66 | 17.77 | 0.215 |
| verify 4K cold | splitmix | 16.34 | 16.21 | 16.38 | 0.234 |
| verify 4K cold | xoshiro | 8.29 | 8.24 | 8.29 | 0.460 |
| verify 4K cold | chacha8 | 5.70 | 5.69 | 5.71 | 0.669 |
| verify 4K cold | aes-ctr | 9.61 | 9.59 | 9.61 | 0.397 |
| crc 4K cold | crc64nvme | 38.57 | 38.56 | 38.61 | 0.099 |
| copy 4K cold | memcpy | 6.36 | 6.35 | 6.37 | 0.600 |
| fill 4K hot | stamped | 87.44 | 87.32 | 87.84 | 0.044 |
| fill 4K hot | splitmix | 22.14 | 22.12 | 22.15 | 0.172 |
| fill 4K hot | xoshiro | 9.88 | 9.87 | 9.88 | 0.386 |
| fill 4K hot | chacha8 | 6.19 | 6.18 | 6.19 | 0.616 |
| fill 4K hot | aes-ctr | 10.85 | 10.84 | 10.85 | 0.352 |
| fill+crc 4K hot | stamped | 37.78 | 37.77 | 37.81 | 0.101 |
| fill+crc 4K hot | splitmix | 16.29 | 16.28 | 16.30 | 0.234 |
| fill+crc 4K hot | xoshiro | 8.34 | 8.34 | 8.34 | 0.457 |
| fill+crc 4K hot | chacha8 | 5.49 | 5.49 | 5.50 | 0.695 |
| fill+crc 4K hot | aes-ctr | 9.04 | 9.04 | 9.04 | 0.422 |
| verify 4K hot | stamped | 53.97 | 53.93 | 54.09 | 0.071 |
| verify 4K hot | splitmix | 19.44 | 19.44 | 19.45 | 0.196 |
| verify 4K hot | xoshiro | 9.20 | 9.20 | 9.20 | 0.415 |
| verify 4K hot | chacha8 | 5.94 | 5.94 | 5.94 | 0.642 |
| verify 4K hot | aes-ctr | 10.07 | 10.00 | 10.08 | 0.379 |
| crc 4K hot | crc64nvme | 55.99 | 55.77 | 56.03 | 0.068 |
| copy 4K hot | memcpy | 88.61 | 86.81 | 93.60 | 0.043 |
| fill 64K cold | stamped | 11.55 | 11.49 | 11.57 | 5.28 |
| fill 64K cold | splitmix | 19.81 | 19.80 | 19.82 | 3.08 |
| fill 64K cold | xoshiro | 8.50 | 8.48 | 8.50 | 7.18 |
| fill 64K cold | chacha8 | 6.44 | 6.43 | 6.44 | 9.48 |
| fill 64K cold | aes-ctr | 8.62 | 8.57 | 8.62 | 7.08 |
| fill+crc 64K cold | stamped | 12.04 | 12.04 | 12.06 | 5.07 |
| fill+crc 64K cold | splitmix | 15.62 | 15.61 | 15.62 | 3.91 |
| fill+crc 64K cold | xoshiro | 7.26 | 7.26 | 7.27 | 8.41 |
| fill+crc 64K cold | chacha8 | 5.86 | 5.86 | 5.87 | 10.41 |
| fill+crc 64K cold | aes-ctr | 7.71 | 7.69 | 7.73 | 7.91 |
| verify 64K cold | stamped | 10.76 | 10.71 | 10.76 | 5.67 |
| verify 64K cold | splitmix | 11.40 | 11.39 | 11.41 | 5.35 |
| verify 64K cold | xoshiro | 7.25 | 7.25 | 7.25 | 8.42 |
| verify 64K cold | chacha8 | 5.13 | 5.12 | 5.14 | 11.90 |
| verify 64K cold | aes-ctr | 7.75 | 7.74 | 7.75 | 7.88 |
| crc 64K cold | crc64nvme | 39.07 | 39.06 | 39.15 | 1.56 |
| copy 64K cold | memcpy | 9.11 | 9.09 | 9.12 | 6.70 |
| fill 64K hot | stamped | 64.77 | 64.70 | 64.77 | 0.942 |
| fill 64K hot | splitmix | 22.79 | 22.77 | 22.79 | 2.68 |
| fill 64K hot | xoshiro | 10.44 | 10.44 | 10.44 | 5.85 |
| fill 64K hot | chacha8 | 6.51 | 6.51 | 6.51 | 9.38 |
| fill 64K hot | aes-ctr | 11.76 | 11.76 | 11.76 | 5.19 |
| fill+crc 64K hot | stamped | 35.04 | 35.04 | 35.05 | 1.74 |
| fill+crc 64K hot | splitmix | 17.52 | 17.49 | 17.52 | 3.48 |
| fill+crc 64K hot | xoshiro | 9.13 | 9.13 | 9.13 | 6.68 |
| fill+crc 64K hot | chacha8 | 5.96 | 5.96 | 5.97 | 10.23 |
| fill+crc 64K hot | aes-ctr | 10.09 | 10.09 | 10.09 | 6.05 |
| verify 64K hot | stamped | 34.98 | 34.97 | 34.98 | 1.75 |
| verify 64K hot | splitmix | 17.55 | 17.54 | 17.55 | 3.48 |
| verify 64K hot | xoshiro | 9.14 | 9.13 | 9.14 | 6.68 |
| verify 64K hot | chacha8 | 5.97 | 5.97 | 5.97 | 10.22 |
| verify 64K hot | aes-ctr | 10.11 | 10.11 | 10.11 | 6.04 |
| crc 64K hot | crc64nvme | 75.60 | 75.55 | 75.61 | 0.807 |
| copy 64K hot | memcpy | 74.37 | 74.36 | 74.41 | 0.821 |
| fill 1M cold | stamped | 12.13 | 11.98 | 12.15 | 80.48 |
| fill 1M cold | splitmix | 19.95 | 19.94 | 19.95 | 48.96 |
| fill 1M cold | xoshiro | 8.71 | 8.64 | 8.75 | 112.2 |
| fill 1M cold | chacha8 | 6.45 | 6.44 | 6.45 | 151.5 |
| fill 1M cold | aes-ctr | 8.53 | 8.53 | 8.53 | 114.5 |
| fill+crc 1M cold | stamped | 10.83 | 10.83 | 10.84 | 90.13 |
| fill+crc 1M cold | splitmix | 15.10 | 15.10 | 15.10 | 64.68 |
| fill+crc 1M cold | xoshiro | 7.95 | 7.93 | 7.95 | 122.9 |
| fill+crc 1M cold | chacha8 | 5.89 | 5.88 | 5.89 | 165.9 |
| fill+crc 1M cold | aes-ctr | 7.70 | 7.70 | 7.70 | 126.8 |
| verify 1M cold | stamped | 17.75 | 17.57 | 17.79 | 55.03 |
| verify 1M cold | splitmix | 14.29 | 14.27 | 14.35 | 68.33 |
| verify 1M cold | xoshiro | 8.12 | 8.12 | 8.12 | 120.3 |
| verify 1M cold | chacha8 | 5.53 | 5.52 | 5.53 | 176.7 |
| verify 1M cold | aes-ctr | 8.78 | 8.78 | 8.81 | 111.2 |
| crc 1M cold | crc64nvme | 40.28 | 40.27 | 40.33 | 24.25 |
| copy 1M cold | memcpy | 10.03 | 10.01 | 10.04 | 97.35 |
| fill 1M hot | stamped | 55.03 | 54.96 | 55.06 | 17.74 |
| fill 1M hot | splitmix | 22.81 | 22.80 | 22.81 | 42.82 |
| fill 1M hot | xoshiro | 10.41 | 10.41 | 10.42 | 93.80 |
| fill 1M hot | chacha8 | 6.50 | 6.50 | 6.51 | 150.2 |
| fill 1M hot | aes-ctr | 11.60 | 11.59 | 11.60 | 84.17 |
| fill+crc 1M hot | stamped | 31.74 | 31.74 | 31.75 | 30.77 |
| fill+crc 1M hot | splitmix | 17.56 | 17.55 | 17.57 | 55.61 |
| fill+crc 1M hot | xoshiro | 9.12 | 9.12 | 9.13 | 107.0 |
| fill+crc 1M hot | chacha8 | 6.00 | 6.00 | 6.00 | 162.8 |
| fill+crc 1M hot | aes-ctr | 10.00 | 10.00 | 10.00 | 97.64 |
| verify 1M hot | stamped | 29.78 | 29.66 | 29.78 | 32.79 |
| verify 1M hot | splitmix | 16.72 | 16.71 | 16.72 | 58.42 |
| verify 1M hot | xoshiro | 8.88 | 8.88 | 8.88 | 109.9 |
| verify 1M hot | chacha8 | 5.89 | 5.89 | 5.90 | 165.7 |
| verify 1M hot | aes-ctr | 9.71 | 9.71 | 9.71 | 100.6 |
| crc 1M hot | crc64nvme | 76.33 | 76.33 | 76.37 | 12.79 |
| copy 1M hot | memcpy | 62.16 | 62.15 | 62.19 | 15.71 |

### 2. Against a server that discards, round 3

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build znver1 (avx2 aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | pattern | 6269 | 163.3 | 1.000 | 1.00 | 6270 | 167.9 | 1.000 | 336.7 | 0 | 0 |
| put 1M plain | fill stamped | 6450 | 158.8 | 1.000 | 1.00 | 6450 | 140.4 | 0.856 | 298.4 | 0 | 0 |
| put 1M plain | fill+crc stamped | 5724 | 178.9 | 1.000 | 1.00 | 5725 | 148.1 | 0.800 | 328.4 | 0 | 0 |
| put 1M plain | fill splitmix | 5819 | 176.0 | 1.000 | 1.00 | 5820 | 147.5 | 0.811 | 327.2 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 5446 | 188.0 | 1.000 | 1.00 | 5447 | 146.7 | 0.753 | 337.6 | 0 | 0 |
| put 1M plain | fill xoshiro | 4591 | 223.0 | 1.000 | 1.00 | 4591 | 142.7 | 0.613 | 362.2 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 4258 | 240.5 | 1.000 | 1.00 | 4258 | 148.1 | 0.589 | 389.5 | 0 | 0 |
| put 1M plain | fill chacha8 | 3627 | 282.3 | 1.000 | 1.00 | 3627 | 146.6 | 0.492 | 436.4 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 3461 | 295.8 | 1.000 | 1.00 | 3461 | 147.3 | 0.471 | 450.2 | 0 | 0 |
| put 1M plain | fill aes-ctr | 4691 | 218.3 | 1.000 | 1.00 | 4691 | 145.2 | 0.638 | 366.7 | 0 | 0 |
| put 1M plain | fill+crc aes-ctr | 4429 | 231.2 | 1.000 | 1.00 | 4430 | 145.6 | 0.603 | 374.5 | 0 | 0 |
| get 1M plain | none | 5754 | 105.9 | 0.595 | 0.613 | 9673 | 182.8 | 1.000 | 271.5 | 0 | 0 |
| get 1M plain | crc | 5549 | 115.5 | 0.626 | 0.633 | 8863 | 189.6 | 1.000 | 282.3 | 0 | 0 |
| get 1M plain | regenerate stamped | 5385 | 131.8 | 0.693 | 0.699 | 7767 | 195.3 | 1.000 | 311.8 | 0 | 0 |
| get 1M plain | regenerate splitmix | 5626 | 130.3 | 0.716 | 0.733 | 7857 | 187.0 | 1.000 | 308.3 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 5769 | 165.3 | 0.931 | 0.949 | 6194 | 148.8 | 0.811 | 312.7 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 4211 | 225.7 | 0.928 | 0.945 | 4538 | 145.9 | 0.573 | 368.5 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 6289 | 153.3 | 0.942 | 0.958 | 6679 | 146.5 | 0.872 | 298.3 | 0 | 0 |
| put 1M ktls | pattern | 2780 | 368.3 | 1.000 | 1.00 | 2781 | 354.1 | 0.934 | 727.7 | 0 | 1.00 |
| put 1M ktls | fill stamped | 1770 | 281.4 | 0.486 | 0.488 | 3639 | 454.1 | 0.758 | 750.8 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 1768 | 292.6 | 0.505 | 0.505 | 3500 | 444.7 | 0.741 | 741.1 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 2075 | 298.3 | 0.604 | 0.611 | 3433 | 393.2 | 0.770 | 705.5 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 2916 | 342.8 | 0.976 | 0.978 | 2987 | 339.4 | 0.939 | 693.0 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 1897 | 344.3 | 0.638 | 0.638 | 2974 | 418.0 | 0.748 | 781.3 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 1632 | 355.8 | 0.567 | 0.567 | 2878 | 482.5 | 0.742 | 854.5 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 1978 | 402.1 | 0.777 | 0.775 | 2547 | 394.0 | 0.734 | 805.4 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 1738 | 408.3 | 0.693 | 0.694 | 2508 | 437.7 | 0.716 | 858.8 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 1722 | 335.8 | 0.565 | 0.567 | 3050 | 454.3 | 0.737 | 815.5 | 0 | 1.00 |
| put 1M ktls | fill+crc aes-ctr | 2728 | 375.1 | 0.999 | 0.998 | 2730 | 342.1 | 0.884 | 732.6 | 0 | 1.00 |
| get 1M ktls | none | 1737 | 344.2 | 0.584 | 0.603 | 2975 | 346.7 | 0.561 | 715.5 | 0 | 1.00 |
| get 1M ktls | crc | 1469 | 412.4 | 0.592 | 0.620 | 2483 | 341.3 | 0.463 | 784.6 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 1379 | 449.9 | 0.606 | 0.633 | 2276 | 350.5 | 0.445 | 837.6 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 1484 | 420.8 | 0.610 | 0.633 | 2433 | 355.2 | 0.488 | 808.5 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 1277 | 512.4 | 0.639 | 0.663 | 1998 | 349.8 | 0.409 | 901.5 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 1279 | 531.3 | 0.664 | 0.681 | 1927 | 359.2 | 0.422 | 912.6 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 1292 | 502.6 | 0.634 | 0.659 | 2038 | 350.0 | 0.415 | 881.2 | 0 | 1.00 |

