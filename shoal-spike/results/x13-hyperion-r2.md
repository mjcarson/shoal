### Checks, round 2

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

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
| describe fixed | expand | 1.00 | 3071110 |
| describe uniform | expand | 1.00 | 3557155 |
| describe doublings | expand | 1.00 | 4010251 |
| describe table | expand | 1.00 | 3307886 |

### 1. Making bytes on one core, GiB/s, round 2

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 5.16 | 5.15 | 5.17 | 0.739 |
| fill 4K cold | splitmix | 4.82 | 4.77 | 4.82 | 0.791 |
| fill 4K cold | xoshiro | 3.15 | 3.15 | 3.16 | 1.21 |
| fill 4K cold | chacha8 | 2.67 | 2.67 | 2.67 | 1.43 |
| fill 4K cold | aes-ctr | 4.69 | 4.69 | 4.69 | 0.814 |
| fill+crc 4K cold | stamped | 3.83 | 3.83 | 3.83 | 0.997 |
| fill+crc 4K cold | splitmix | 3.47 | 3.47 | 3.48 | 1.10 |
| fill+crc 4K cold | xoshiro | 2.83 | 2.82 | 2.84 | 1.35 |
| fill+crc 4K cold | chacha8 | 2.21 | 2.21 | 2.21 | 1.73 |
| fill+crc 4K cold | aes-ctr | 3.25 | 3.24 | 3.29 | 1.18 |
| verify 4K cold | stamped | 6.03 | 6.02 | 6.03 | 0.633 |
| verify 4K cold | splitmix | 3.76 | 3.76 | 3.76 | 1.01 |
| verify 4K cold | xoshiro | 3.40 | 3.40 | 3.40 | 1.12 |
| verify 4K cold | chacha8 | 2.28 | 2.28 | 2.28 | 1.67 |
| verify 4K cold | aes-ctr | 4.44 | 4.44 | 4.44 | 0.859 |
| crc 4K cold | crc64nvme | 11.49 | 11.32 | 11.51 | 0.332 |
| copy 4K cold | memcpy | 5.11 | 5.09 | 5.13 | 0.746 |
| fill 4K hot | stamped | 29.64 | 29.64 | 29.65 | 0.129 |
| fill 4K hot | splitmix | 4.89 | 4.89 | 4.90 | 0.781 |
| fill 4K hot | xoshiro | 4.55 | 4.55 | 4.56 | 0.838 |
| fill 4K hot | chacha8 | 2.67 | 2.66 | 2.67 | 1.43 |
| fill 4K hot | aes-ctr | 6.34 | 6.33 | 6.34 | 0.602 |
| fill+crc 4K hot | stamped | 8.88 | 8.88 | 8.88 | 0.429 |
| fill+crc 4K hot | splitmix | 3.55 | 3.54 | 3.55 | 1.08 |
| fill+crc 4K hot | xoshiro | 3.35 | 3.35 | 3.35 | 1.14 |
| fill+crc 4K hot | chacha8 | 2.21 | 2.21 | 2.21 | 1.73 |
| fill+crc 4K hot | aes-ctr | 4.24 | 4.23 | 4.24 | 0.900 |
| verify 4K hot | stamped | 17.74 | 17.73 | 17.74 | 0.215 |
| verify 4K hot | splitmix | 4.44 | 4.44 | 4.45 | 0.858 |
| verify 4K hot | xoshiro | 4.10 | 4.10 | 4.11 | 0.930 |
| verify 4K hot | chacha8 | 2.52 | 2.52 | 2.52 | 1.52 |
| verify 4K hot | aes-ctr | 5.56 | 5.56 | 5.57 | 0.686 |
| crc 4K hot | crc64nvme | 12.88 | 12.88 | 12.89 | 0.296 |
| copy 4K hot | memcpy | 38.37 | 38.30 | 38.40 | 0.099 |
| fill 64K cold | stamped | 6.37 | 6.20 | 6.37 | 9.58 |
| fill 64K cold | splitmix | 4.89 | 4.89 | 4.90 | 12.47 |
| fill 64K cold | xoshiro | 3.43 | 3.41 | 3.43 | 17.79 |
| fill 64K cold | chacha8 | 2.78 | 2.78 | 2.78 | 21.95 |
| fill 64K cold | aes-ctr | 3.96 | 3.83 | 3.97 | 15.40 |
| fill+crc 64K cold | stamped | 4.33 | 4.33 | 4.33 | 14.11 |
| fill+crc 64K cold | splitmix | 3.56 | 3.55 | 3.56 | 17.16 |
| fill+crc 64K cold | xoshiro | 2.88 | 2.82 | 2.89 | 21.20 |
| fill+crc 64K cold | chacha8 | 2.29 | 2.29 | 2.30 | 26.60 |
| fill+crc 64K cold | aes-ctr | 2.99 | 2.99 | 2.99 | 20.44 |
| verify 64K cold | stamped | 7.15 | 7.15 | 7.15 | 8.54 |
| verify 64K cold | splitmix | 3.75 | 3.74 | 3.75 | 16.28 |
| verify 64K cold | xoshiro | 3.81 | 3.81 | 3.81 | 16.02 |
| verify 64K cold | chacha8 | 2.34 | 2.34 | 2.34 | 26.11 |
| verify 64K cold | aes-ctr | 4.60 | 4.60 | 4.61 | 13.26 |
| crc 64K cold | crc64nvme | 11.53 | 11.37 | 11.55 | 5.29 |
| copy 64K cold | memcpy | 6.67 | 6.66 | 6.67 | 9.15 |
| fill 64K hot | stamped | 27.08 | 27.08 | 27.08 | 2.25 |
| fill 64K hot | splitmix | 4.98 | 4.97 | 4.98 | 12.27 |
| fill 64K hot | xoshiro | 4.95 | 4.93 | 4.95 | 12.32 |
| fill 64K hot | chacha8 | 2.76 | 2.75 | 2.76 | 22.15 |
| fill 64K hot | aes-ctr | 6.74 | 6.73 | 6.74 | 9.06 |
| fill+crc 64K hot | stamped | 8.87 | 8.87 | 8.88 | 6.88 |
| fill+crc 64K hot | splitmix | 3.59 | 3.59 | 3.61 | 16.98 |
| fill+crc 64K hot | xoshiro | 3.65 | 3.65 | 3.65 | 16.72 |
| fill+crc 64K hot | chacha8 | 2.28 | 2.28 | 2.29 | 26.75 |
| fill+crc 64K hot | aes-ctr | 4.45 | 4.44 | 4.46 | 13.71 |
| verify 64K hot | stamped | 15.19 | 15.19 | 15.20 | 4.02 |
| verify 64K hot | splitmix | 4.34 | 4.34 | 4.34 | 14.05 |
| verify 64K hot | xoshiro | 4.39 | 4.39 | 4.40 | 13.90 |
| verify 64K hot | chacha8 | 2.55 | 2.54 | 2.55 | 23.96 |
| verify 64K hot | aes-ctr | 5.57 | 5.57 | 5.57 | 10.96 |
| crc 64K hot | crc64nvme | 13.16 | 13.15 | 13.16 | 4.64 |
| copy 64K hot | memcpy | 30.51 | 30.46 | 30.53 | 2.00 |
| fill 1M cold | stamped | 6.43 | 6.27 | 6.44 | 151.9 |
| fill 1M cold | splitmix | 4.90 | 4.90 | 4.90 | 199.2 |
| fill 1M cold | xoshiro | 3.45 | 3.44 | 3.46 | 282.8 |
| fill 1M cold | chacha8 | 2.79 | 2.79 | 2.79 | 349.9 |
| fill 1M cold | aes-ctr | 4.29 | 4.19 | 4.29 | 227.6 |
| fill+crc 1M cold | stamped | 4.44 | 4.44 | 4.44 | 219.9 |
| fill+crc 1M cold | splitmix | 3.54 | 3.54 | 3.54 | 276.2 |
| fill+crc 1M cold | xoshiro | 3.03 | 3.03 | 3.04 | 321.9 |
| fill+crc 1M cold | chacha8 | 2.28 | 2.28 | 2.28 | 427.6 |
| fill+crc 1M cold | aes-ctr | 3.24 | 3.23 | 3.24 | 301.6 |
| verify 1M cold | stamped | 6.99 | 6.98 | 6.99 | 139.8 |
| verify 1M cold | splitmix | 3.68 | 3.68 | 3.68 | 265.3 |
| verify 1M cold | xoshiro | 3.72 | 3.72 | 3.72 | 262.5 |
| verify 1M cold | chacha8 | 2.32 | 2.32 | 2.32 | 420.2 |
| verify 1M cold | aes-ctr | 4.50 | 4.50 | 4.50 | 217.2 |
| crc 1M cold | crc64nvme | 11.54 | 11.45 | 11.55 | 84.62 |
| copy 1M cold | memcpy | 6.50 | 6.44 | 6.50 | 150.3 |
| fill 1M hot | stamped | 27.01 | 27.01 | 27.03 | 36.16 |
| fill 1M hot | splitmix | 4.98 | 4.97 | 4.98 | 196.1 |
| fill 1M hot | xoshiro | 4.98 | 4.98 | 4.99 | 195.9 |
| fill 1M hot | chacha8 | 2.78 | 2.78 | 2.78 | 351.4 |
| fill 1M hot | aes-ctr | 6.71 | 6.71 | 6.72 | 145.5 |
| fill+crc 1M hot | stamped | 8.88 | 8.87 | 8.88 | 110.0 |
| fill+crc 1M hot | splitmix | 3.63 | 3.63 | 3.63 | 268.9 |
| fill+crc 1M hot | xoshiro | 3.66 | 3.65 | 3.66 | 267.1 |
| fill+crc 1M hot | chacha8 | 2.29 | 2.29 | 2.29 | 425.8 |
| fill+crc 1M hot | aes-ctr | 4.47 | 4.46 | 4.47 | 218.7 |
| verify 1M hot | stamped | 12.82 | 12.81 | 12.82 | 76.20 |
| verify 1M hot | splitmix | 4.33 | 4.33 | 4.33 | 225.5 |
| verify 1M hot | xoshiro | 4.39 | 4.20 | 4.40 | 222.2 |
| verify 1M hot | chacha8 | 2.56 | 2.56 | 2.57 | 380.7 |
| verify 1M hot | aes-ctr | 5.58 | 5.57 | 5.58 | 175.0 |
| crc 1M hot | crc64nvme | 13.20 | 13.20 | 13.22 | 73.98 |
| copy 1M hot | memcpy | 29.66 | 29.64 | 29.66 | 32.92 |

### 3a. Making bytes on several cores, no wire, round 2

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | GiB/s, all | GiB/s, least core |
| --- | --- | --- | --- |
| fill+crc 1M cold x1 | aes-ctr | 3.22 | 3.22 |
| fill+crc 1M cold x1 | chacha8 | 2.28 | 2.28 |
| fill+crc 1M cold x1 | xoshiro | 2.95 | 2.95 |
| fill+crc 1M cold x1 | splitmix | 3.54 | 3.54 |
| fill+crc 1M cold x1 | stamped | 4.38 | 4.38 |
| fill+crc 1M cold x2 | aes-ctr | 5.98 | 2.99 |
| fill+crc 1M cold x2 | chacha8 | 4.38 | 2.19 |
| fill+crc 1M cold x2 | xoshiro | 5.54 | 2.74 |
| fill+crc 1M cold x2 | splitmix | 6.34 | 3.17 |
| fill+crc 1M cold x2 | stamped | 6.15 | 3.07 |
| fill+crc 1M cold x4 | aes-ctr | 8.11 | 2.00 |
| fill+crc 1M cold x4 | chacha8 | 6.74 | 1.67 |
| fill+crc 1M cold x4 | xoshiro | 8.36 | 2.05 |
| fill+crc 1M cold x4 | splitmix | 9.39 | 2.31 |
| fill+crc 1M cold x4 | stamped | 7.54 | 1.87 |

### 2. Against a server that discards, round 2

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | fill+crc aes-ctr | 1837 | 557.5 | 1.00 | 1.00 | 1837 | 302.4 | 0.534 | 871.8 | 0 | 0 |
| put 1M plain | fill aes-ctr | 2122 | 482.4 | 1.000 | 1.00 | 2123 | 274.6 | 0.560 | 754.6 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 1403 | 729.8 | 1.000 | 1.00 | 1403 | 337.8 | 0.454 | 1068 | 0 | 0 |
| put 1M plain | fill chacha8 | 1536 | 666.4 | 1.000 | 1.00 | 1537 | 324.7 | 0.478 | 1009 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 1682 | 608.7 | 1.00 | 1.00 | 1682 | 297.6 | 0.480 | 921.5 | 0 | 0 |
| put 1M plain | fill xoshiro | 1947 | 525.8 | 1.00 | 1.00 | 1947 | 307.9 | 0.577 | 851.8 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 1790 | 572.2 | 1.00 | 1.00 | 1790 | 310.7 | 0.534 | 885.7 | 0 | 0 |
| put 1M plain | fill splitmix | 2073 | 494.0 | 1.00 | 1.00 | 2073 | 310.8 | 0.620 | 806.2 | 0 | 0 |
| put 1M plain | fill+crc stamped | 2159 | 474.3 | 1.00 | 1.00 | 2159 | 300.2 | 0.624 | 780.5 | 0 | 0 |
| put 1M plain | fill stamped | 2491 | 411.0 | 1.000 | 1.00 | 2491 | 316.1 | 0.760 | 746.5 | 0 | 0 |
| put 1M plain | pattern | 3530 | 290.1 | 1.00 | 1.00 | 3530 | 292.6 | 1.000 | 583.6 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 3268 | 292.2 | 0.933 | 0.952 | 3504 | 316.0 | 1.000 | 603.9 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 1690 | 558.4 | 0.922 | 0.940 | 1834 | 337.2 | 0.547 | 888.0 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 2563 | 368.0 | 0.921 | 0.942 | 2783 | 326.1 | 0.807 | 695.1 | 0 | 0 |
| get 1M plain | regenerate splitmix | 2539 | 371.0 | 0.920 | 0.941 | 2760 | 327.6 | 0.803 | 700.1 | 0 | 0 |
| get 1M plain | regenerate stamped | 2953 | 260.3 | 0.751 | 0.789 | 3934 | 350.0 | 1.000 | 597.7 | 0 | 0 |
| get 1M plain | crc | 3171 | 205.7 | 0.637 | 0.667 | 4979 | 325.7 | 1.000 | 507.4 | 0 | 0 |
| get 1M plain | none | 3210 | 181.8 | 0.570 | 0.598 | 5632 | 321.7 | 1.000 | 470.7 | 0 | 0 |
| put 1M ktls | fill+crc aes-ctr | 998.1 | 1025 | 0.999 | 1.00 | 999.0 | 900.3 | 0.868 | 2004 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 1018 | 945.5 | 0.940 | 0.940 | 1083 | 921.7 | 0.907 | 1877 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 834.1 | 1213 | 0.988 | 0.990 | 844.0 | 995.6 | 0.802 | 2229 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 622.9 | 1124 | 0.684 | 0.685 | 911.2 | 1164 | 0.699 | 2318 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 623.7 | 1047 | 0.638 | 0.641 | 977.8 | 1144 | 0.687 | 2259 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 1026 | 996.0 | 0.998 | 0.998 | 1028 | 943.2 | 0.936 | 1980 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 967.1 | 1059 | 1.000 | 1.00 | 967.4 | 943.2 | 0.882 | 2018 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 1030 | 993.0 | 0.999 | 0.998 | 1031 | 909.2 | 0.906 | 1902 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 580.9 | 1005 | 0.570 | 0.573 | 1019 | 1243 | 0.696 | 2323 | 0 | 1.00 |
| put 1M ktls | fill stamped | 1100 | 929.6 | 0.999 | 0.998 | 1102 | 903.0 | 0.961 | 1870 | 0 | 1.00 |
| put 1M ktls | pattern | 951.5 | 791.2 | 0.735 | 0.737 | 1294 | 890.0 | 0.818 | 1692 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 564.7 | 1196 | 0.659 | 0.673 | 856.5 | 875.2 | 0.474 | 2103 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 507.1 | 1352 | 0.670 | 0.685 | 757.1 | 931.1 | 0.452 | 2314 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 547.9 | 1224 | 0.655 | 0.673 | 836.3 | 901.5 | 0.473 | 2190 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 854.4 | 1173 | 0.978 | 1.00 | 873.3 | 1205 | 0.996 | 2442 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 568.5 | 1179 | 0.655 | 0.673 | 868.2 | 882.9 | 0.481 | 2114 | 0 | 1.00 |
| get 1M ktls | crc | 645.7 | 999.4 | 0.630 | 0.645 | 1025 | 836.7 | 0.519 | 1865 | 0 | 1.00 |
| get 1M ktls | none | 672.9 | 940.7 | 0.618 | 0.636 | 1089 | 837.5 | 0.541 | 1823 | 0 | 1.00 |

### 3b. The put from several client cores, round 2

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain x1 | fill+crc aes-ctr | 1833 | 558.4 | 1.000 | 1.00 | 1834 | 302.1 | 0.532 | 872.3 | 0 | 0 |
| put 1M plain x1 | fill+crc chacha8 | 1404 | 729.3 | 1.000 | 1.00 | 1404 | 337.1 | 0.453 | 1087 | 0 | 0 |
| put 1M plain x1 | fill+crc xoshiro | 1693 | 604.7 | 1.000 | 1.00 | 1694 | 311.0 | 0.505 | 915.5 | 0 | 0 |
| put 1M plain x1 | fill+crc splitmix | 1789 | 572.5 | 1.000 | 1.00 | 1789 | 306.2 | 0.526 | 878.0 | 0 | 0 |
| put 1M plain x2 | fill+crc aes-ctr | 2895 | 707.3 | 1.000 | 1.00 | 1448 | 402.9 | 0.572 | 1119 | 0 | 0 |
| put 1M plain x2 | fill+crc chacha8 | 2515 | 814.2 | 1.000 | 0.999 | 1258 | 363.3 | 0.448 | 1189 | 0 | 0 |
| put 1M plain x2 | fill+crc xoshiro | 2888 | 708.9 | 1.000 | 1.00 | 1444 | 361.3 | 0.524 | 1068 | 0 | 0 |
| put 1M plain x2 | fill+crc splitmix | 3078 | 665.3 | 1.000 | 1.00 | 1539 | 373.2 | 0.563 | 1038 | 0 | 0 |

### 4. A folder of real files, round 2

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | MiB/s | cpu ms/GiB | device MiB read | from device |
| --- | --- | --- | --- | --- | --- |
| 64K cold | read+sha256 | 305.6 | 1420 | 2048 | 1.00 |
| 64K cold | read+crc | 369.1 | 850.5 | 2048 | 1.00 |
| 64K cold | read | 380.7 | 769.2 | 2048 | 1.00 |
| 64K hot | read+sha256 | 1251 | 818.3 | 0 | 0 |
| 64K hot | read+crc | 3882 | 263.8 | 0 | 0 |
| 64K hot | read | 5469 | 187.2 | 0 | 0 |
| 1M cold | read+sha256 | 522.8 | 948.4 | 2048 | 1.00 |
| 1M cold | read+crc | 725.8 | 411.6 | 2048 | 1.00 |
| 1M cold | read | 764.5 | 335.4 | 2048 | 1.00 |
| 1M hot | read+sha256 | 1422 | 719.9 | 0 | 0 |
| 1M hot | read+crc | 5469 | 187.2 | 0 | 0 |
| 1M hot | read | 9714 | 105.4 | 0 | 0 |
| 64M cold | read+sha256 | 598.6 | 882.8 | 2048 | 1.00 |
| 64M cold | read+crc | 859.6 | 350.4 | 2048 | 1.00 |
| 64M cold | read | 860.9 | 274.2 | 2048 | 1.00 |
| 64M hot | read+sha256 | 1445 | 708.7 | 0 | 0 |
| 64M hot | read+crc | 5747 | 178.1 | 0 | 0 |
| 64M hot | read | 10678 | 95.90 | 0 | 0 |

