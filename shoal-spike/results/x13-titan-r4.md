### Checks, round 4

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

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
| describe fixed | expand | 1.00 | 3162225 |
| describe uniform | expand | 1.00 | 3618993 |
| describe doublings | expand | 1.00 | 4048427 |
| describe table | expand | 1.00 | 3308908 |

### 1. Making bytes on one core, GiB/s, round 4

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 5.15 | 5.14 | 5.15 | 0.741 |
| fill 4K cold | splitmix | 4.82 | 4.82 | 4.82 | 0.791 |
| fill 4K cold | xoshiro | 3.13 | 3.13 | 3.14 | 1.22 |
| fill 4K cold | chacha8 | 2.67 | 2.67 | 2.67 | 1.43 |
| fill 4K cold | aes-ctr | 4.71 | 4.66 | 4.72 | 0.810 |
| fill+crc 4K cold | stamped | 3.85 | 3.85 | 3.85 | 0.991 |
| fill+crc 4K cold | splitmix | 3.47 | 3.47 | 3.48 | 1.10 |
| fill+crc 4K cold | xoshiro | 2.90 | 2.90 | 2.91 | 1.31 |
| fill+crc 4K cold | chacha8 | 2.21 | 2.21 | 2.21 | 1.73 |
| fill+crc 4K cold | aes-ctr | 3.24 | 3.24 | 3.26 | 1.18 |
| verify 4K cold | stamped | 6.01 | 5.90 | 6.01 | 0.635 |
| verify 4K cold | splitmix | 3.75 | 3.74 | 3.75 | 1.02 |
| verify 4K cold | xoshiro | 3.40 | 3.40 | 3.40 | 1.12 |
| verify 4K cold | chacha8 | 2.28 | 2.27 | 2.28 | 1.68 |
| verify 4K cold | aes-ctr | 4.43 | 4.43 | 4.43 | 0.862 |
| crc 4K cold | crc64nvme | 11.49 | 11.49 | 11.51 | 0.332 |
| copy 4K cold | memcpy | 5.31 | 5.30 | 5.31 | 0.719 |
| fill 4K hot | stamped | 29.68 | 29.21 | 29.69 | 0.129 |
| fill 4K hot | splitmix | 4.91 | 4.90 | 4.91 | 0.777 |
| fill 4K hot | xoshiro | 4.55 | 4.55 | 4.56 | 0.838 |
| fill 4K hot | chacha8 | 2.67 | 2.67 | 2.67 | 1.43 |
| fill 4K hot | aes-ctr | 6.34 | 6.34 | 6.35 | 0.601 |
| fill+crc 4K hot | stamped | 8.89 | 8.89 | 8.90 | 0.429 |
| fill+crc 4K hot | splitmix | 3.55 | 3.55 | 3.55 | 1.07 |
| fill+crc 4K hot | xoshiro | 3.35 | 3.33 | 3.35 | 1.14 |
| fill+crc 4K hot | chacha8 | 2.21 | 2.21 | 2.21 | 1.73 |
| fill+crc 4K hot | aes-ctr | 4.24 | 4.24 | 4.24 | 0.900 |
| verify 4K hot | stamped | 17.76 | 17.72 | 17.77 | 0.215 |
| verify 4K hot | splitmix | 4.46 | 4.45 | 4.46 | 0.856 |
| verify 4K hot | xoshiro | 4.09 | 4.08 | 4.10 | 0.933 |
| verify 4K hot | chacha8 | 2.52 | 2.52 | 2.52 | 1.52 |
| verify 4K hot | aes-ctr | 5.57 | 5.57 | 5.57 | 0.685 |
| crc 4K hot | crc64nvme | 12.91 | 12.91 | 12.91 | 0.296 |
| copy 4K hot | memcpy | 29.47 | 29.46 | 29.51 | 0.129 |
| fill 64K cold | stamped | 6.41 | 6.41 | 6.41 | 9.53 |
| fill 64K cold | splitmix | 4.89 | 4.89 | 4.90 | 12.48 |
| fill 64K cold | xoshiro | 3.41 | 3.41 | 3.41 | 17.91 |
| fill 64K cold | chacha8 | 2.78 | 2.78 | 2.78 | 21.97 |
| fill 64K cold | aes-ctr | 3.91 | 3.80 | 3.93 | 15.59 |
| fill+crc 64K cold | stamped | 4.35 | 4.35 | 4.36 | 14.02 |
| fill+crc 64K cold | splitmix | 3.55 | 3.55 | 3.55 | 17.20 |
| fill+crc 64K cold | xoshiro | 2.81 | 2.80 | 2.86 | 21.73 |
| fill+crc 64K cold | chacha8 | 2.29 | 2.29 | 2.29 | 26.61 |
| fill+crc 64K cold | aes-ctr | 2.96 | 2.96 | 2.96 | 20.62 |
| verify 64K cold | stamped | 7.15 | 7.04 | 7.16 | 8.54 |
| verify 64K cold | splitmix | 3.74 | 3.74 | 3.74 | 16.31 |
| verify 64K cold | xoshiro | 3.80 | 3.80 | 3.80 | 16.06 |
| verify 64K cold | chacha8 | 2.33 | 2.33 | 2.33 | 26.15 |
| verify 64K cold | aes-ctr | 4.60 | 4.59 | 4.61 | 13.28 |
| crc 64K cold | crc64nvme | 11.51 | 11.50 | 11.52 | 5.30 |
| copy 64K cold | memcpy | 6.90 | 6.85 | 6.90 | 8.85 |
| fill 64K hot | stamped | 27.14 | 27.13 | 27.15 | 2.25 |
| fill 64K hot | splitmix | 4.98 | 4.98 | 4.99 | 12.25 |
| fill 64K hot | xoshiro | 4.96 | 4.96 | 4.96 | 12.30 |
| fill 64K hot | chacha8 | 2.76 | 2.76 | 2.76 | 22.11 |
| fill 64K hot | aes-ctr | 6.73 | 6.72 | 6.76 | 9.06 |
| fill+crc 64K hot | stamped | 8.89 | 8.89 | 8.89 | 6.87 |
| fill+crc 64K hot | splitmix | 3.63 | 3.62 | 3.63 | 16.84 |
| fill+crc 64K hot | xoshiro | 3.66 | 3.66 | 3.66 | 16.69 |
| fill+crc 64K hot | chacha8 | 2.29 | 2.28 | 2.29 | 26.70 |
| fill+crc 64K hot | aes-ctr | 4.47 | 4.46 | 4.48 | 13.67 |
| verify 64K hot | stamped | 15.19 | 15.18 | 15.20 | 4.02 |
| verify 64K hot | splitmix | 4.35 | 4.35 | 4.35 | 14.04 |
| verify 64K hot | xoshiro | 4.39 | 4.39 | 4.39 | 13.91 |
| verify 64K hot | chacha8 | 2.55 | 2.55 | 2.55 | 23.93 |
| verify 64K hot | aes-ctr | 5.58 | 5.57 | 5.58 | 10.94 |
| crc 64K hot | crc64nvme | 13.17 | 13.17 | 13.18 | 4.63 |
| copy 64K hot | memcpy | 30.15 | 30.14 | 30.17 | 2.02 |
| fill 1M cold | stamped | 6.51 | 6.50 | 6.51 | 150.1 |
| fill 1M cold | splitmix | 4.90 | 4.90 | 4.90 | 199.3 |
| fill 1M cold | xoshiro | 3.44 | 3.43 | 3.44 | 284.2 |
| fill 1M cold | chacha8 | 2.79 | 2.75 | 2.79 | 350.4 |
| fill 1M cold | aes-ctr | 4.28 | 4.19 | 4.28 | 228.0 |
| fill+crc 1M cold | stamped | 4.50 | 4.50 | 4.50 | 217.1 |
| fill+crc 1M cold | splitmix | 3.54 | 3.54 | 3.54 | 275.5 |
| fill+crc 1M cold | xoshiro | 3.00 | 2.98 | 3.04 | 325.2 |
| fill+crc 1M cold | chacha8 | 2.29 | 2.29 | 2.29 | 425.9 |
| fill+crc 1M cold | aes-ctr | 3.23 | 3.23 | 3.23 | 302.4 |
| verify 1M cold | stamped | 6.98 | 6.98 | 6.99 | 139.8 |
| verify 1M cold | splitmix | 3.68 | 3.68 | 3.68 | 265.6 |
| verify 1M cold | xoshiro | 3.72 | 3.71 | 3.72 | 262.8 |
| verify 1M cold | chacha8 | 2.32 | 2.32 | 2.32 | 420.5 |
| verify 1M cold | aes-ctr | 4.49 | 4.48 | 4.49 | 217.6 |
| crc 1M cold | crc64nvme | 11.57 | 11.55 | 11.57 | 84.43 |
| copy 1M cold | memcpy | 6.71 | 6.53 | 6.72 | 145.4 |
| fill 1M hot | stamped | 27.02 | 27.00 | 27.03 | 36.14 |
| fill 1M hot | splitmix | 4.99 | 4.99 | 5.00 | 195.6 |
| fill 1M hot | xoshiro | 4.97 | 4.97 | 4.98 | 196.3 |
| fill 1M hot | chacha8 | 2.77 | 2.77 | 2.78 | 352.3 |
| fill 1M hot | aes-ctr | 6.70 | 6.70 | 6.71 | 145.7 |
| fill+crc 1M hot | stamped | 8.87 | 8.87 | 8.88 | 110.1 |
| fill+crc 1M hot | splitmix | 3.62 | 3.62 | 3.62 | 269.6 |
| fill+crc 1M hot | xoshiro | 3.66 | 3.66 | 3.66 | 267.1 |
| fill+crc 1M hot | chacha8 | 2.29 | 2.29 | 2.29 | 426.6 |
| fill+crc 1M hot | aes-ctr | 4.46 | 4.46 | 4.47 | 219.1 |
| verify 1M hot | stamped | 12.82 | 12.70 | 12.82 | 76.19 |
| verify 1M hot | splitmix | 4.33 | 4.22 | 4.34 | 225.4 |
| verify 1M hot | xoshiro | 4.38 | 3.74 | 4.39 | 222.8 |
| verify 1M hot | chacha8 | 2.55 | 2.29 | 2.56 | 382.3 |
| verify 1M hot | aes-ctr | 5.57 | 5.52 | 5.58 | 175.4 |
| crc 1M hot | crc64nvme | 13.21 | 13.21 | 13.21 | 73.94 |
| copy 1M hot | memcpy | 29.71 | 29.70 | 29.74 | 32.87 |

### 3a. Making bytes on several cores, no wire, round 4

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | GiB/s, all | GiB/s, least core |
| --- | --- | --- | --- |
| fill+crc 1M cold x1 | aes-ctr | 3.22 | 3.22 |
| fill+crc 1M cold x1 | chacha8 | 2.29 | 2.29 |
| fill+crc 1M cold x1 | xoshiro | 2.94 | 2.94 |
| fill+crc 1M cold x1 | splitmix | 3.55 | 3.55 |
| fill+crc 1M cold x1 | stamped | 4.40 | 4.40 |
| fill+crc 1M cold x2 | aes-ctr | 5.99 | 3.00 |
| fill+crc 1M cold x2 | chacha8 | 4.37 | 2.17 |
| fill+crc 1M cold x2 | xoshiro | 5.60 | 2.78 |
| fill+crc 1M cold x2 | splitmix | 6.34 | 3.16 |
| fill+crc 1M cold x2 | stamped | 6.23 | 3.11 |
| fill+crc 1M cold x4 | aes-ctr | 8.33 | 2.04 |
| fill+crc 1M cold x4 | chacha8 | 6.77 | 1.67 |
| fill+crc 1M cold x4 | xoshiro | 8.47 | 2.07 |
| fill+crc 1M cold x4 | splitmix | 9.54 | 2.30 |
| fill+crc 1M cold x4 | stamped | 7.05 | 1.72 |

### 2. Against a server that discards, round 4

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | fill+crc aes-ctr | 1861 | 550.3 | 1.000 | 1.00 | 1861 | 293.3 | 0.524 | 862.8 | 0 | 0 |
| put 1M plain | fill aes-ctr | 2145 | 476.7 | 0.999 | 1.00 | 2148 | 275.3 | 0.568 | 758.8 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 1411 | 725.8 | 1.000 | 1.00 | 1411 | 333.1 | 0.450 | 1065 | 0 | 0 |
| put 1M plain | fill chacha8 | 1550 | 660.7 | 1.000 | 1.00 | 1550 | 321.4 | 0.477 | 985.8 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 1698 | 603.2 | 1.000 | 1.00 | 1698 | 305.4 | 0.497 | 930.0 | 0 | 0 |
| put 1M plain | fill xoshiro | 1958 | 523.1 | 1.000 | 1.00 | 1958 | 303.4 | 0.570 | 834.8 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 1817 | 563.4 | 1.000 | 1.00 | 1817 | 307.1 | 0.536 | 874.4 | 0 | 0 |
| put 1M plain | fill splitmix | 2091 | 489.7 | 1.000 | 1.00 | 2091 | 306.5 | 0.617 | 806.9 | 0 | 0 |
| put 1M plain | fill+crc stamped | 2186 | 468.4 | 1.000 | 1.00 | 2186 | 300.3 | 0.631 | 788.8 | 0 | 0 |
| put 1M plain | fill stamped | 2527 | 405.3 | 1.000 | 1.00 | 2527 | 309.7 | 0.754 | 726.2 | 0 | 0 |
| put 1M plain | pattern | 3575 | 286.5 | 1.000 | 1.00 | 3575 | 289.1 | 1.000 | 586.0 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 3279 | 298.3 | 0.955 | 0.967 | 3433 | 315.0 | 0.999 | 617.5 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 1675 | 564.8 | 0.924 | 0.940 | 1813 | 337.1 | 0.541 | 898.6 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 2487 | 376.3 | 0.914 | 0.938 | 2721 | 325.8 | 0.782 | 703.3 | 0 | 0 |
| get 1M plain | regenerate splitmix | 2422 | 388.8 | 0.919 | 0.942 | 2634 | 341.2 | 0.796 | 861.5 | 0 | 0 |
| get 1M plain | regenerate stamped | 2966 | 270.2 | 0.783 | 0.820 | 3789 | 348.9 | 1.000 | 612.3 | 0 | 0 |
| get 1M plain | crc | 3232 | 203.2 | 0.641 | 0.676 | 5039 | 319.7 | 1.000 | 503.7 | 0 | 0 |
| get 1M plain | none | 3256 | 180.3 | 0.573 | 0.591 | 5680 | 317.5 | 1.000 | 455.9 | 0 | 0 |
| put 1M ktls | fill+crc aes-ctr | 704.7 | 1039 | 0.715 | 0.719 | 985.3 | 1052 | 0.714 | 2150 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 718.3 | 969.7 | 0.680 | 0.684 | 1056 | 1043 | 0.722 | 2081 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 728.9 | 1228 | 0.874 | 0.877 | 833.9 | 1020 | 0.716 | 2287 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 718.1 | 1155 | 0.810 | 0.813 | 886.5 | 1041 | 0.720 | 2227 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 630.7 | 1079 | 0.665 | 0.669 | 949.1 | 1153 | 0.700 | 2325 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 964.2 | 1036 | 0.975 | 0.976 | 988.6 | 961.2 | 0.895 | 2076 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 580.1 | 1070 | 0.606 | 0.613 | 956.6 | 1230 | 0.687 | 2365 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 1007 | 1015 | 0.998 | 1.00 | 1009 | 952.8 | 0.927 | 1992 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 625.1 | 1004 | 0.613 | 0.615 | 1020 | 1153 | 0.695 | 2198 | 0 | 1.00 |
| put 1M ktls | fill stamped | 556.3 | 961.2 | 0.522 | 0.532 | 1065 | 1294 | 0.693 | 2367 | 0 | 1.00 |
| put 1M ktls | pattern | 1015 | 792.7 | 0.786 | 0.789 | 1292 | 860.5 | 0.843 | 1702 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 561.3 | 1201 | 0.659 | 0.671 | 852.3 | 880.6 | 0.473 | 2123 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 502.1 | 1363 | 0.669 | 0.688 | 751.0 | 943.9 | 0.453 | 2333 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 549.7 | 1231 | 0.661 | 0.676 | 831.7 | 895.0 | 0.470 | 2216 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 872.1 | 1157 | 0.986 | 1.00 | 884.9 | 1181 | 0.995 | 2403 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 569.3 | 1180 | 0.656 | 0.670 | 867.7 | 879.9 | 0.479 | 2090 | 0 | 1.00 |
| get 1M ktls | crc | 906.6 | 1076 | 0.953 | 1.00 | 951.5 | 1136 | 0.996 | 2286 | 0 | 1.00 |
| get 1M ktls | none | 666.0 | 940.1 | 0.611 | 0.628 | 1089 | 862.2 | 0.551 | 1835 | 0 | 1.00 |

### 3b. The put from several client cores, round 4

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain x1 | fill+crc aes-ctr | 1846 | 554.6 | 1.000 | 1.00 | 1846 | 300.3 | 0.532 | 875.1 | 0 | 0 |
| put 1M plain x1 | fill+crc chacha8 | 1398 | 732.6 | 1.000 | 1.00 | 1398 | 335.9 | 0.449 | 1090 | 0 | 0 |
| put 1M plain x1 | fill+crc xoshiro | 1695 | 604.1 | 1.000 | 1.00 | 1695 | 311.5 | 0.507 | 915.9 | 0 | 0 |
| put 1M plain x1 | fill+crc splitmix | 1761 | 581.1 | 1.000 | 1.00 | 1762 | 314.8 | 0.531 | 1077 | 0 | 0 |
| put 1M plain x2 | fill+crc aes-ctr | 2887 | 709.1 | 1.000 | 1.00 | 1444 | 400.0 | 0.568 | 1128 | 0 | 0 |
| put 1M plain x2 | fill+crc chacha8 | 2505 | 817.4 | 1.000 | 1.00 | 1253 | 361.9 | 0.444 | 1194 | 0 | 0 |
| put 1M plain x2 | fill+crc xoshiro | 2856 | 716.9 | 1.000 | 1.00 | 1428 | 371.5 | 0.521 | 1103 | 0 | 0 |
| put 1M plain x2 | fill+crc splitmix | 3020 | 678.0 | 1.000 | 1.00 | 1510 | 390.5 | 0.576 | 1073 | 0 | 0 |

### 4. A folder of real files, round 4

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | MiB/s | cpu ms/GiB | device MiB read | from device |
| --- | --- | --- | --- | --- | --- |
| 64K cold | read+sha256 | 312.6 | 1420 | 2048 | 1.00 |
| 64K cold | read+crc | 381.6 | 850.6 | 2048 | 1.00 |
| 64K cold | read | 393.2 | 772.8 | 2048 | 1.00 |
| 64K hot | read+sha256 | 1238 | 827.1 | 0 | 0 |
| 64K hot | read+crc | 3801 | 269.4 | 0 | 0 |
| 64K hot | read | 5224 | 196.0 | 0 | 0 |
| 1M cold | read+sha256 | 527.0 | 938.3 | 2048 | 1.00 |
| 1M cold | read+crc | 731.5 | 402.8 | 2048 | 1.00 |
| 1M cold | read | 773.1 | 325.8 | 2048 | 1.00 |
| 1M hot | read+sha256 | 1446 | 708.0 | 0 | 0 |
| 1M hot | read+crc | 5821 | 175.9 | 0 | 0 |
| 1M hot | read | 10524 | 97.27 | 0 | 0 |
| 64M cold | read+sha256 | 596.8 | 869.4 | 2048 | 1.00 |
| 64M cold | read+crc | 859.8 | 337.0 | 2048 | 1.00 |
| 64M cold | read | 859.7 | 263.3 | 2048 | 1.00 |
| 64M hot | read+sha256 | 1475 | 694.3 | 0 | 0 |
| 64M hot | read+crc | 6199 | 165.2 | 0 | 0 |
| 64M hot | read | 11812 | 86.68 | 0 | 0 |

