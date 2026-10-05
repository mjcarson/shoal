### Checks, round 2

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
| describe fixed | expand | 1.00 | 7781046 |
| describe uniform | expand | 1.00 | 8881925 |
| describe doublings | expand | 1.00 | 10387107 |
| describe table | expand | 1.00 | 8034008 |

### 1. Making bytes on one core, GiB/s, round 2

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build znver1 (avx2 aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa znver1

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 10.82 | 10.47 | 10.91 | 0.352 |
| fill 4K cold | splitmix | 19.62 | 19.61 | 19.64 | 0.194 |
| fill 4K cold | xoshiro | 7.95 | 7.92 | 7.99 | 0.480 |
| fill 4K cold | chacha8 | 6.15 | 6.06 | 6.17 | 0.620 |
| fill 4K cold | aes-ctr | 10.26 | 10.22 | 10.28 | 0.372 |
| fill+crc 4K cold | stamped | 13.26 | 13.21 | 13.32 | 0.288 |
| fill+crc 4K cold | splitmix | 14.66 | 14.65 | 14.66 | 0.260 |
| fill+crc 4K cold | xoshiro | 7.24 | 7.24 | 7.25 | 0.527 |
| fill+crc 4K cold | chacha8 | 5.53 | 5.51 | 5.53 | 0.690 |
| fill+crc 4K cold | aes-ctr | 7.79 | 7.75 | 7.83 | 0.490 |
| verify 4K cold | stamped | 17.60 | 17.47 | 17.74 | 0.217 |
| verify 4K cold | splitmix | 16.62 | 16.59 | 16.65 | 0.230 |
| verify 4K cold | xoshiro | 8.31 | 8.30 | 8.33 | 0.459 |
| verify 4K cold | chacha8 | 5.77 | 5.76 | 5.77 | 0.661 |
| verify 4K cold | aes-ctr | 9.63 | 9.63 | 9.64 | 0.396 |
| crc 4K cold | crc64nvme | 39.84 | 38.51 | 39.92 | 0.096 |
| copy 4K cold | memcpy | 6.54 | 6.44 | 6.55 | 0.583 |
| fill 4K hot | stamped | 87.12 | 86.83 | 87.40 | 0.044 |
| fill 4K hot | splitmix | 22.19 | 22.17 | 22.22 | 0.172 |
| fill 4K hot | xoshiro | 9.92 | 9.91 | 9.92 | 0.385 |
| fill 4K hot | chacha8 | 6.20 | 6.19 | 6.20 | 0.615 |
| fill 4K hot | aes-ctr | 10.84 | 10.83 | 10.84 | 0.352 |
| fill+crc 4K hot | stamped | 37.94 | 37.77 | 38.02 | 0.101 |
| fill+crc 4K hot | splitmix | 16.31 | 16.29 | 16.31 | 0.234 |
| fill+crc 4K hot | xoshiro | 8.37 | 8.37 | 8.38 | 0.456 |
| fill+crc 4K hot | chacha8 | 5.51 | 5.50 | 5.51 | 0.693 |
| fill+crc 4K hot | aes-ctr | 9.03 | 9.02 | 9.03 | 0.422 |
| verify 4K hot | stamped | 53.91 | 53.77 | 54.15 | 0.071 |
| verify 4K hot | splitmix | 19.53 | 19.52 | 19.53 | 0.195 |
| verify 4K hot | xoshiro | 9.22 | 9.21 | 9.22 | 0.414 |
| verify 4K hot | chacha8 | 5.93 | 5.92 | 5.93 | 0.643 |
| verify 4K hot | aes-ctr | 10.00 | 9.99 | 10.02 | 0.381 |
| crc 4K hot | crc64nvme | 55.79 | 55.73 | 55.88 | 0.068 |
| copy 4K hot | memcpy | 82.88 | 82.84 | 84.64 | 0.046 |
| fill 64K cold | stamped | 11.53 | 11.44 | 11.54 | 5.29 |
| fill 64K cold | splitmix | 19.87 | 19.86 | 19.88 | 3.07 |
| fill 64K cold | xoshiro | 8.47 | 8.47 | 8.49 | 7.21 |
| fill 64K cold | chacha8 | 6.45 | 6.44 | 6.45 | 9.47 |
| fill 64K cold | aes-ctr | 8.44 | 8.43 | 8.45 | 7.24 |
| fill+crc 64K cold | stamped | 12.13 | 12.13 | 12.13 | 5.03 |
| fill+crc 64K cold | splitmix | 15.70 | 15.68 | 15.71 | 3.89 |
| fill+crc 64K cold | xoshiro | 7.27 | 7.27 | 7.28 | 8.39 |
| fill+crc 64K cold | chacha8 | 5.91 | 5.91 | 5.92 | 10.32 |
| fill+crc 64K cold | aes-ctr | 7.60 | 7.60 | 7.63 | 8.03 |
| verify 64K cold | stamped | 11.07 | 11.06 | 11.07 | 5.51 |
| verify 64K cold | splitmix | 12.06 | 11.95 | 12.08 | 5.06 |
| verify 64K cold | xoshiro | 7.72 | 7.71 | 7.73 | 7.91 |
| verify 64K cold | chacha8 | 5.19 | 5.19 | 5.19 | 11.76 |
| verify 64K cold | aes-ctr | 7.78 | 7.78 | 7.78 | 7.85 |
| crc 64K cold | crc64nvme | 39.04 | 38.94 | 39.07 | 1.56 |
| copy 64K cold | memcpy | 9.35 | 9.31 | 9.36 | 6.53 |
| fill 64K hot | stamped | 64.78 | 64.77 | 64.82 | 0.942 |
| fill 64K hot | splitmix | 22.86 | 22.85 | 22.86 | 2.67 |
| fill 64K hot | xoshiro | 10.47 | 10.45 | 10.47 | 5.83 |
| fill 64K hot | chacha8 | 6.52 | 6.52 | 6.52 | 9.36 |
| fill 64K hot | aes-ctr | 11.77 | 11.76 | 11.78 | 5.18 |
| fill+crc 64K hot | stamped | 35.09 | 35.09 | 35.09 | 1.74 |
| fill+crc 64K hot | splitmix | 17.57 | 17.56 | 17.57 | 3.47 |
| fill+crc 64K hot | xoshiro | 9.17 | 9.16 | 9.17 | 6.66 |
| fill+crc 64K hot | chacha8 | 6.03 | 6.03 | 6.04 | 10.12 |
| fill+crc 64K hot | aes-ctr | 10.11 | 10.11 | 10.11 | 6.04 |
| verify 64K hot | stamped | 35.00 | 34.98 | 35.01 | 1.74 |
| verify 64K hot | splitmix | 17.59 | 17.58 | 17.59 | 3.47 |
| verify 64K hot | xoshiro | 9.17 | 9.17 | 9.17 | 6.66 |
| verify 64K hot | chacha8 | 6.06 | 6.06 | 6.06 | 10.07 |
| verify 64K hot | aes-ctr | 10.11 | 10.11 | 10.11 | 6.04 |
| crc 64K hot | crc64nvme | 75.81 | 75.80 | 75.83 | 0.805 |
| copy 64K hot | memcpy | 74.42 | 74.39 | 74.47 | 0.820 |
| fill 1M cold | stamped | 12.17 | 12.11 | 12.20 | 80.24 |
| fill 1M cold | splitmix | 20.02 | 20.02 | 20.03 | 48.77 |
| fill 1M cold | xoshiro | 8.67 | 8.66 | 8.67 | 112.6 |
| fill 1M cold | chacha8 | 6.44 | 6.44 | 6.44 | 151.7 |
| fill 1M cold | aes-ctr | 8.54 | 8.52 | 8.54 | 114.4 |
| fill+crc 1M cold | stamped | 10.85 | 10.84 | 10.85 | 90.04 |
| fill+crc 1M cold | splitmix | 15.01 | 15.00 | 15.01 | 65.07 |
| fill+crc 1M cold | xoshiro | 7.83 | 7.82 | 7.83 | 124.8 |
| fill+crc 1M cold | chacha8 | 5.88 | 5.88 | 5.88 | 166.2 |
| fill+crc 1M cold | aes-ctr | 7.69 | 7.68 | 7.69 | 127.0 |
| verify 1M cold | stamped | 17.44 | 17.37 | 17.46 | 56.00 |
| verify 1M cold | splitmix | 14.18 | 14.17 | 14.18 | 68.88 |
| verify 1M cold | xoshiro | 8.09 | 8.09 | 8.10 | 120.7 |
| verify 1M cold | chacha8 | 5.54 | 5.53 | 5.55 | 176.2 |
| verify 1M cold | aes-ctr | 8.82 | 8.81 | 8.83 | 110.7 |
| crc 1M cold | crc64nvme | 40.34 | 40.27 | 40.36 | 24.21 |
| copy 1M cold | memcpy | 10.44 | 10.37 | 10.46 | 93.58 |
| fill 1M hot | stamped | 54.81 | 54.78 | 54.84 | 17.82 |
| fill 1M hot | splitmix | 22.86 | 22.85 | 22.87 | 42.72 |
| fill 1M hot | xoshiro | 10.43 | 10.42 | 10.43 | 93.64 |
| fill 1M hot | chacha8 | 6.52 | 6.52 | 6.52 | 149.8 |
| fill 1M hot | aes-ctr | 11.53 | 11.53 | 11.54 | 84.68 |
| fill+crc 1M hot | stamped | 31.66 | 31.65 | 31.69 | 30.85 |
| fill+crc 1M hot | splitmix | 17.63 | 17.61 | 17.63 | 55.40 |
| fill+crc 1M hot | xoshiro | 9.15 | 9.15 | 9.15 | 106.7 |
| fill+crc 1M hot | chacha8 | 6.01 | 6.01 | 6.01 | 162.5 |
| fill+crc 1M hot | aes-ctr | 9.99 | 9.99 | 9.99 | 97.75 |
| verify 1M hot | stamped | 29.74 | 29.73 | 29.78 | 32.84 |
| verify 1M hot | splitmix | 16.84 | 16.82 | 16.84 | 57.99 |
| verify 1M hot | xoshiro | 8.92 | 8.92 | 8.92 | 109.5 |
| verify 1M hot | chacha8 | 5.92 | 5.92 | 5.92 | 165.0 |
| verify 1M hot | aes-ctr | 9.70 | 9.70 | 9.71 | 100.7 |
| crc 1M hot | crc64nvme | 76.19 | 76.19 | 76.19 | 12.82 |
| copy 1M hot | memcpy | 62.66 | 62.61 | 62.73 | 15.58 |

### 2. Against a server that discards, round 2

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build znver1 (avx2 aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | fill+crc aes-ctr | 4958 | 206.5 | 1.000 | 1.00 | 4959 | 119.8 | 0.553 | 332.4 | 0 | 0 |
| put 1M plain | fill aes-ctr | 4686 | 218.5 | 1.000 | 1.00 | 4687 | 147.9 | 0.650 | 366.6 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 3766 | 271.9 | 1.000 | 1.00 | 3766 | 120.9 | 0.418 | 391.0 | 0 | 0 |
| put 1M plain | fill chacha8 | 3967 | 258.1 | 1.000 | 1.00 | 3967 | 121.3 | 0.443 | 379.4 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 4781 | 214.1 | 1.000 | 1.00 | 4782 | 121.1 | 0.538 | 340.9 | 0 | 0 |
| put 1M plain | fill xoshiro | 5093 | 201.0 | 1.000 | 1.00 | 5094 | 120.8 | 0.574 | 337.7 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 5406 | 189.4 | 1.000 | 1.00 | 5406 | 148.9 | 0.759 | 339.4 | 0 | 0 |
| put 1M plain | fill splitmix | 6794 | 150.7 | 1.000 | 1.00 | 6795 | 120.4 | 0.771 | 275.5 | 0 | 0 |
| put 1M plain | fill+crc stamped | 6811 | 150.3 | 1.000 | 1.00 | 6812 | 120.7 | 0.775 | 274.8 | 0 | 0 |
| put 1M plain | fill stamped | 7422 | 138.0 | 1.000 | 1.00 | 7423 | 120.8 | 0.848 | 258.2 | 0 | 0 |
| put 1M plain | pattern | 6219 | 164.6 | 1.000 | 1.00 | 6220 | 169.2 | 1.000 | 339.4 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 5756 | 170.5 | 0.958 | 0.969 | 6007 | 163.2 | 0.890 | 339.4 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 4214 | 225.7 | 0.929 | 0.946 | 4538 | 145.6 | 0.572 | 371.7 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 5826 | 163.7 | 0.931 | 0.949 | 6255 | 149.4 | 0.822 | 313.9 | 0 | 0 |
| get 1M plain | regenerate splitmix | 5621 | 130.1 | 0.714 | 0.733 | 7868 | 187.2 | 1.000 | 311.4 | 0 | 0 |
| get 1M plain | regenerate stamped | 5379 | 132.5 | 0.696 | 0.712 | 7729 | 195.6 | 1.000 | 324.7 | 0 | 0 |
| get 1M plain | crc | 5543 | 116.2 | 0.629 | 0.635 | 8810 | 189.8 | 1.000 | 283.7 | 0 | 0 |
| get 1M plain | none | 5738 | 106.2 | 0.595 | 0.586 | 9641 | 183.4 | 1.000 | 263.7 | 0 | 0 |
| put 1M ktls | fill+crc aes-ctr | 2718 | 376.0 | 0.998 | 0.998 | 2723 | 341.8 | 0.880 | 745.0 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 2054 | 338.7 | 0.679 | 0.678 | 3024 | 397.1 | 0.770 | 752.6 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 1668 | 411.0 | 0.670 | 0.669 | 2491 | 469.8 | 0.738 | 905.9 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 1631 | 398.7 | 0.635 | 0.636 | 2568 | 475.7 | 0.731 | 907.7 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 1792 | 353.7 | 0.619 | 0.618 | 2895 | 443.8 | 0.750 | 835.3 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 2732 | 373.1 | 0.995 | 0.994 | 2745 | 345.6 | 0.895 | 733.7 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 2924 | 342.2 | 0.977 | 0.976 | 2993 | 338.6 | 0.940 | 697.5 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 1778 | 290.5 | 0.505 | 0.508 | 3525 | 442.3 | 0.741 | 755.3 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 1773 | 291.2 | 0.504 | 0.506 | 3517 | 442.7 | 0.740 | 761.0 | 0 | 1.00 |
| put 1M ktls | fill stamped | 2812 | 310.7 | 0.853 | 0.857 | 3296 | 330.5 | 0.881 | 663.3 | 0 | 1.00 |
| put 1M ktls | pattern | 2569 | 357.5 | 0.897 | 0.898 | 2864 | 380.1 | 0.926 | 759.6 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 1457 | 450.7 | 0.641 | 0.657 | 2272 | 357.4 | 0.482 | 854.4 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 1182 | 573.0 | 0.661 | 0.685 | 1787 | 353.0 | 0.381 | 963.2 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 1272 | 513.3 | 0.638 | 0.663 | 1995 | 353.4 | 0.412 | 911.0 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 1478 | 421.8 | 0.609 | 0.629 | 2427 | 357.8 | 0.490 | 809.3 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 1490 | 413.3 | 0.601 | 0.626 | 2478 | 359.4 | 0.496 | 816.2 | 0 | 1.00 |
| get 1M ktls | crc | 1455 | 414.5 | 0.589 | 0.616 | 2471 | 347.1 | 0.466 | 799.6 | 0 | 1.00 |
| get 1M ktls | none | 1493 | 397.5 | 0.579 | 0.611 | 2576 | 345.5 | 0.477 | 783.2 | 0 | 1.00 |

