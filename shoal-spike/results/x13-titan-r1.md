### Checks, round 1

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
| describe fixed | expand | 1.00 | 3102962 |
| describe uniform | expand | 1.00 | 3565406 |
| describe doublings | expand | 1.00 | 4012486 |
| describe table | expand | 1.00 | 3130184 |

### 1. Making bytes on one core, GiB/s, round 1

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 5.12 | 4.31 | 5.13 | 0.745 |
| fill 4K cold | splitmix | 4.82 | 4.76 | 4.83 | 0.792 |
| fill 4K cold | xoshiro | 3.14 | 3.14 | 3.14 | 1.22 |
| fill 4K cold | chacha8 | 2.67 | 2.52 | 2.67 | 1.43 |
| fill 4K cold | aes-ctr | 4.66 | 4.17 | 4.66 | 0.818 |
| fill+crc 4K cold | stamped | 3.80 | 3.80 | 3.81 | 1.00 |
| fill+crc 4K cold | splitmix | 3.47 | 3.46 | 3.48 | 1.10 |
| fill+crc 4K cold | xoshiro | 2.92 | 2.91 | 2.93 | 1.31 |
| fill+crc 4K cold | chacha8 | 2.21 | 2.21 | 2.21 | 1.73 |
| fill+crc 4K cold | aes-ctr | 3.30 | 3.28 | 3.32 | 1.16 |
| verify 4K cold | stamped | 6.01 | 6.01 | 6.07 | 0.634 |
| verify 4K cold | splitmix | 3.71 | 3.71 | 3.72 | 1.03 |
| verify 4K cold | xoshiro | 3.39 | 3.39 | 3.40 | 1.12 |
| verify 4K cold | chacha8 | 2.27 | 2.26 | 2.27 | 1.68 |
| verify 4K cold | aes-ctr | 4.41 | 4.40 | 4.41 | 0.866 |
| crc 4K cold | crc64nvme | 11.50 | 11.36 | 11.50 | 0.332 |
| copy 4K cold | memcpy | 5.09 | 5.09 | 5.10 | 0.749 |
| fill 4K hot | stamped | 29.70 | 29.70 | 29.73 | 0.128 |
| fill 4K hot | splitmix | 4.91 | 4.90 | 4.92 | 0.777 |
| fill 4K hot | xoshiro | 4.56 | 4.55 | 4.57 | 0.837 |
| fill 4K hot | chacha8 | 2.67 | 2.67 | 2.67 | 1.43 |
| fill 4K hot | aes-ctr | 6.34 | 6.33 | 6.36 | 0.601 |
| fill+crc 4K hot | stamped | 8.89 | 8.89 | 8.90 | 0.429 |
| fill+crc 4K hot | splitmix | 3.54 | 3.51 | 3.55 | 1.08 |
| fill+crc 4K hot | xoshiro | 3.36 | 3.35 | 3.36 | 1.14 |
| fill+crc 4K hot | chacha8 | 2.21 | 2.21 | 2.21 | 1.72 |
| fill+crc 4K hot | aes-ctr | 4.24 | 4.24 | 4.25 | 0.899 |
| verify 4K hot | stamped | 17.76 | 17.76 | 17.76 | 0.215 |
| verify 4K hot | splitmix | 4.45 | 4.44 | 4.45 | 0.858 |
| verify 4K hot | xoshiro | 4.08 | 4.07 | 4.09 | 0.936 |
| verify 4K hot | chacha8 | 2.52 | 2.51 | 2.52 | 1.51 |
| verify 4K hot | aes-ctr | 5.56 | 5.56 | 5.57 | 0.686 |
| crc 4K hot | crc64nvme | 12.90 | 12.83 | 12.90 | 0.296 |
| copy 4K hot | memcpy | 38.45 | 38.43 | 38.45 | 0.099 |
| fill 64K cold | stamped | 6.28 | 6.21 | 6.28 | 9.72 |
| fill 64K cold | splitmix | 4.88 | 4.88 | 4.89 | 12.51 |
| fill 64K cold | xoshiro | 3.47 | 3.47 | 3.47 | 17.60 |
| fill 64K cold | chacha8 | 2.78 | 2.78 | 2.78 | 21.99 |
| fill 64K cold | aes-ctr | 3.95 | 3.86 | 3.95 | 15.45 |
| fill+crc 64K cold | stamped | 4.26 | 4.26 | 4.29 | 14.32 |
| fill+crc 64K cold | splitmix | 3.57 | 3.57 | 3.58 | 17.07 |
| fill+crc 64K cold | xoshiro | 2.81 | 2.81 | 2.81 | 21.71 |
| fill+crc 64K cold | chacha8 | 2.29 | 2.29 | 2.29 | 26.61 |
| fill+crc 64K cold | aes-ctr | 2.96 | 2.96 | 2.97 | 20.59 |
| verify 64K cold | stamped | 7.14 | 7.14 | 7.19 | 8.55 |
| verify 64K cold | splitmix | 3.74 | 3.74 | 3.74 | 16.32 |
| verify 64K cold | xoshiro | 3.80 | 3.79 | 3.80 | 16.05 |
| verify 64K cold | chacha8 | 2.33 | 2.30 | 2.33 | 26.16 |
| verify 64K cold | aes-ctr | 4.60 | 4.59 | 4.60 | 13.27 |
| crc 64K cold | crc64nvme | 11.51 | 11.40 | 11.52 | 5.30 |
| copy 64K cold | memcpy | 6.60 | 6.60 | 6.61 | 9.24 |
| fill 64K hot | stamped | 27.14 | 27.13 | 27.17 | 2.25 |
| fill 64K hot | splitmix | 4.98 | 4.98 | 4.98 | 12.26 |
| fill 64K hot | xoshiro | 4.97 | 4.95 | 4.97 | 12.29 |
| fill 64K hot | chacha8 | 2.76 | 2.76 | 2.76 | 22.11 |
| fill 64K hot | aes-ctr | 6.73 | 6.71 | 6.75 | 9.07 |
| fill+crc 64K hot | stamped | 8.89 | 8.88 | 8.89 | 6.87 |
| fill+crc 64K hot | splitmix | 3.61 | 3.61 | 3.61 | 16.89 |
| fill+crc 64K hot | xoshiro | 3.65 | 3.57 | 3.65 | 16.71 |
| fill+crc 64K hot | chacha8 | 2.29 | 2.28 | 2.29 | 26.70 |
| fill+crc 64K hot | aes-ctr | 4.47 | 4.45 | 4.48 | 13.67 |
| verify 64K hot | stamped | 15.17 | 15.16 | 15.18 | 4.02 |
| verify 64K hot | splitmix | 4.35 | 4.34 | 4.35 | 14.04 |
| verify 64K hot | xoshiro | 4.39 | 4.39 | 4.39 | 13.90 |
| verify 64K hot | chacha8 | 2.55 | 2.55 | 2.55 | 23.93 |
| verify 64K hot | aes-ctr | 5.57 | 5.57 | 5.58 | 10.95 |
| crc 64K hot | crc64nvme | 13.17 | 13.17 | 13.21 | 4.63 |
| copy 64K hot | memcpy | 30.55 | 30.55 | 30.58 | 2.00 |
| fill 1M cold | stamped | 6.26 | 6.17 | 6.35 | 155.9 |
| fill 1M cold | splitmix | 4.90 | 4.89 | 4.90 | 199.3 |
| fill 1M cold | xoshiro | 3.50 | 3.50 | 3.50 | 279.0 |
| fill 1M cold | chacha8 | 2.79 | 2.79 | 2.79 | 350.4 |
| fill 1M cold | aes-ctr | 4.26 | 4.26 | 4.27 | 229.0 |
| fill+crc 1M cold | stamped | 4.39 | 4.39 | 4.40 | 222.5 |
| fill+crc 1M cold | splitmix | 3.54 | 3.53 | 3.54 | 276.1 |
| fill+crc 1M cold | xoshiro | 2.90 | 2.89 | 2.90 | 336.8 |
| fill+crc 1M cold | chacha8 | 2.28 | 2.28 | 2.28 | 428.6 |
| fill+crc 1M cold | aes-ctr | 3.20 | 3.19 | 3.20 | 305.6 |
| verify 1M cold | stamped | 6.98 | 6.98 | 7.05 | 139.9 |
| verify 1M cold | splitmix | 3.68 | 3.68 | 3.68 | 265.5 |
| verify 1M cold | xoshiro | 3.71 | 3.71 | 3.72 | 262.9 |
| verify 1M cold | chacha8 | 2.32 | 2.32 | 2.32 | 420.6 |
| verify 1M cold | aes-ctr | 4.49 | 4.38 | 4.49 | 217.5 |
| crc 1M cold | crc64nvme | 11.53 | 11.44 | 11.55 | 84.69 |
| copy 1M cold | memcpy | 6.44 | 6.44 | 6.44 | 151.6 |
| fill 1M hot | stamped | 27.07 | 27.06 | 27.09 | 36.07 |
| fill 1M hot | splitmix | 5.00 | 5.00 | 5.00 | 195.3 |
| fill 1M hot | xoshiro | 4.99 | 4.99 | 4.99 | 195.8 |
| fill 1M hot | chacha8 | 2.78 | 2.78 | 2.78 | 351.5 |
| fill 1M hot | aes-ctr | 6.73 | 6.73 | 6.74 | 145.0 |
| fill+crc 1M hot | stamped | 8.88 | 8.88 | 8.90 | 110.0 |
| fill+crc 1M hot | splitmix | 3.63 | 3.63 | 3.63 | 269.0 |
| fill+crc 1M hot | xoshiro | 3.67 | 3.65 | 3.67 | 266.4 |
| fill+crc 1M hot | chacha8 | 2.30 | 2.29 | 2.30 | 425.2 |
| fill+crc 1M hot | aes-ctr | 4.47 | 4.47 | 4.48 | 218.2 |
| verify 1M hot | stamped | 12.68 | 12.66 | 12.69 | 77.00 |
| verify 1M hot | splitmix | 4.34 | 4.34 | 4.34 | 225.0 |
| verify 1M hot | xoshiro | 4.39 | 4.38 | 4.39 | 222.7 |
| verify 1M hot | chacha8 | 2.56 | 2.56 | 2.56 | 381.9 |
| verify 1M hot | aes-ctr | 5.59 | 5.58 | 5.60 | 174.7 |
| crc 1M hot | crc64nvme | 13.21 | 13.21 | 13.22 | 73.90 |
| copy 1M hot | memcpy | 29.94 | 29.92 | 29.98 | 32.61 |

### 3a. Making bytes on several cores, no wire, round 1

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | GiB/s, all | GiB/s, least core |
| --- | --- | --- | --- |
| fill+crc 1M cold x1 | stamped | 4.35 | 4.35 |
| fill+crc 1M cold x1 | splitmix | 3.50 | 3.50 |
| fill+crc 1M cold x1 | xoshiro | 2.93 | 2.93 |
| fill+crc 1M cold x1 | chacha8 | 2.28 | 2.28 |
| fill+crc 1M cold x1 | aes-ctr | 3.23 | 3.23 |
| fill+crc 1M cold x2 | stamped | 6.50 | 3.25 |
| fill+crc 1M cold x2 | splitmix | 6.33 | 3.16 |
| fill+crc 1M cold x2 | xoshiro | 5.55 | 2.76 |
| fill+crc 1M cold x2 | chacha8 | 4.37 | 2.17 |
| fill+crc 1M cold x2 | aes-ctr | 5.94 | 2.97 |
| fill+crc 1M cold x4 | stamped | 6.49 | 1.59 |
| fill+crc 1M cold x4 | splitmix | 9.24 | 2.26 |
| fill+crc 1M cold x4 | xoshiro | 8.29 | 2.04 |
| fill+crc 1M cold x4 | chacha8 | 6.69 | 1.65 |
| fill+crc 1M cold x4 | aes-ctr | 7.92 | 1.94 |

### 2. Against a server that discards, round 1

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | pattern | 3581 | 285.9 | 1.000 | 1.00 | 3582 | 288.5 | 1.000 | 587.2 | 0 | 0 |
| put 1M plain | fill stamped | 2482 | 412.5 | 1.000 | 1.00 | 2483 | 315.5 | 0.755 | 754.8 | 0 | 0 |
| put 1M plain | fill+crc stamped | 2161 | 473.7 | 1.000 | 1.00 | 2162 | 303.2 | 0.631 | 777.0 | 0 | 0 |
| put 1M plain | fill splitmix | 2032 | 503.7 | 1.000 | 1.00 | 2033 | 316.4 | 0.618 | 990.5 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 1802 | 568.3 | 1.000 | 1.00 | 1802 | 306.6 | 0.530 | 879.7 | 0 | 0 |
| put 1M plain | fill xoshiro | 1943 | 526.9 | 1.000 | 1.00 | 1943 | 308.0 | 0.575 | 843.0 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 1685 | 607.7 | 1.000 | 1.00 | 1685 | 310.8 | 0.502 | 937.0 | 0 | 0 |
| put 1M plain | fill chacha8 | 1539 | 665.3 | 1.000 | 1.00 | 1539 | 325.9 | 0.481 | 995.1 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 1405 | 728.9 | 1.000 | 1.00 | 1405 | 335.8 | 0.451 | 1072 | 0 | 0 |
| put 1M plain | fill aes-ctr | 2133 | 480.1 | 1.000 | 1.00 | 2133 | 298.3 | 0.612 | 801.7 | 0 | 0 |
| put 1M plain | fill+crc aes-ctr | 1841 | 555.9 | 1.000 | 0.998 | 1842 | 297.8 | 0.526 | 874.1 | 0 | 0 |
| get 1M plain | none | 3285 | 179.3 | 0.575 | 0.596 | 5710 | 314.6 | 1.000 | 453.8 | 0 | 0 |
| get 1M plain | crc | 3223 | 203.6 | 0.641 | 0.671 | 5030 | 320.6 | 1.000 | 501.8 | 0 | 0 |
| get 1M plain | regenerate stamped | 2953 | 268.4 | 0.774 | 0.813 | 3815 | 350.4 | 1.000 | 612.9 | 0 | 0 |
| get 1M plain | regenerate splitmix | 2533 | 371.5 | 0.919 | 0.942 | 2756 | 324.2 | 0.792 | 703.2 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 2501 | 373.7 | 0.913 | 0.938 | 2740 | 325.0 | 0.785 | 705.7 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 1648 | 572.2 | 0.921 | 0.940 | 1790 | 346.3 | 0.546 | 1046 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 3287 | 295.8 | 0.950 | 0.965 | 3461 | 314.6 | 1.000 | 611.1 | 0 | 0 |
| put 1M ktls | pattern | 719.1 | 777.5 | 0.546 | 0.548 | 1317 | 1035 | 0.717 | 1888 | 0 | 1.00 |
| put 1M ktls | fill stamped | 712.3 | 921.1 | 0.641 | 0.643 | 1112 | 1062 | 0.729 | 2050 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 627.5 | 986.1 | 0.604 | 0.610 | 1038 | 1166 | 0.705 | 2239 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 670.3 | 976.6 | 0.639 | 0.643 | 1049 | 1149 | 0.742 | 2175 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 628.7 | 1039 | 0.638 | 0.641 | 985.7 | 1150 | 0.696 | 2234 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 1022 | 1000 | 0.998 | 0.998 | 1024 | 944.0 | 0.932 | 2000 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 627.5 | 1054 | 0.646 | 0.649 | 971.8 | 1142 | 0.690 | 2284 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 712.5 | 1129 | 0.785 | 0.787 | 907.3 | 1042 | 0.715 | 2213 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 611.1 | 1197 | 0.714 | 0.715 | 855.7 | 1164 | 0.685 | 2396 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 1017 | 948.9 | 0.943 | 0.944 | 1079 | 923.9 | 0.908 | 1886 | 0 | 1.00 |
| put 1M ktls | fill+crc aes-ctr | 587.9 | 1051 | 0.604 | 0.627 | 974.1 | 1226 | 0.694 | 2901 | 0 | 1.00 |
| get 1M ktls | none | 672.7 | 935.2 | 0.614 | 0.631 | 1095 | 851.2 | 0.549 | 1860 | 0 | 1.00 |
| get 1M ktls | crc | 645.5 | 996.3 | 0.628 | 0.644 | 1028 | 843.8 | 0.522 | 1884 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 568.9 | 1176 | 0.653 | 0.671 | 870.9 | 881.5 | 0.480 | 2102 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 560.7 | 1200 | 0.657 | 0.673 | 853.4 | 886.5 | 0.475 | 2162 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 870.1 | 1162 | 0.987 | 1.00 | 881.1 | 1183 | 0.994 | 2401 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 504.9 | 1355 | 0.668 | 0.686 | 755.6 | 941.0 | 0.454 | 2360 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 562.5 | 1197 | 0.657 | 0.673 | 855.6 | 882.6 | 0.475 | 2126 | 0 | 1.00 |

### 3b. The put from several client cores, round 1

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain x1 | fill+crc splitmix | 1791 | 571.7 | 1.000 | 1.00 | 1791 | 305.9 | 0.526 | 876.9 | 0 | 0 |
| put 1M plain x1 | fill+crc xoshiro | 1692 | 605.1 | 1.000 | 1.00 | 1692 | 310.0 | 0.503 | 932.0 | 0 | 0 |
| put 1M plain x1 | fill+crc chacha8 | 1402 | 730.2 | 1.000 | 1.00 | 1402 | 335.1 | 0.449 | 1094 | 0 | 0 |
| put 1M plain x1 | fill+crc aes-ctr | 1849 | 553.8 | 1.000 | 1.00 | 1849 | 296.2 | 0.525 | 858.2 | 0 | 0 |
| put 1M plain x2 | fill+crc splitmix | 3097 | 661.3 | 1.000 | 1.00 | 1548 | 371.1 | 0.562 | 1037 | 0 | 0 |
| put 1M plain x2 | fill+crc xoshiro | 2762 | 740.7 | 0.999 | 1.00 | 1382 | 376.8 | 0.510 | 1258 | 0 | 0 |
| put 1M plain x2 | fill+crc chacha8 | 2532 | 808.5 | 1.000 | 1.00 | 1267 | 355.6 | 0.445 | 1181 | 0 | 0 |
| put 1M plain x2 | fill+crc aes-ctr | 2904 | 705.3 | 1.00 | 1.00 | 1452 | 394.6 | 0.563 | 1103 | 0 | 0 |

### 4. A folder of real files, round 1

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | MiB/s | cpu ms/GiB | device MiB read | from device |
| --- | --- | --- | --- | --- | --- |
| 64K cold | read | 413.3 | 823.7 | 2048 | 1.00 |
| 64K cold | read+crc | 406.6 | 849.4 | 2048 | 1.00 |
| 64K cold | read+sha256 | 329.7 | 1419 | 2048 | 1.00 |
| 64K hot | read | 5289 | 193.6 | 0 | 0 |
| 64K hot | read+crc | 3808 | 268.9 | 0 | 0 |
| 64K hot | read+sha256 | 1242 | 824.3 | 0 | 0 |
| 1M cold | read | 767.4 | 331.1 | 2048 | 1.00 |
| 1M cold | read+crc | 733.1 | 400.4 | 2048 | 1.00 |
| 1M cold | read+sha256 | 529.9 | 935.4 | 2048 | 1.00 |
| 1M hot | read | 10970 | 93.35 | 0 | 0 |
| 1M hot | read+crc | 5960 | 171.8 | 0 | 0 |
| 1M hot | read+sha256 | 1455 | 703.6 | 0 | 0 |
| 64M cold | read | 860.4 | 263.8 | 2048 | 1.00 |
| 64M cold | read+crc | 859.5 | 335.9 | 2048 | 1.00 |
| 64M cold | read+sha256 | 601.3 | 869.3 | 2048 | 1.00 |
| 64M hot | read | 12376 | 82.73 | 0 | 0 |
| 64M hot | read+crc | 6336 | 161.6 | 0 | 0 |
| 64M hot | read+sha256 | 1481 | 691.5 | 0 | 0 |

