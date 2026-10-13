### Checks, round 3

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
| describe fixed | expand | 1.00 | 3099003 |
| describe uniform | expand | 1.00 | 3559709 |
| describe doublings | expand | 1.00 | 3970375 |
| describe table | expand | 1.00 | 3310096 |

### 1. Making bytes on one core, GiB/s, round 3

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 5.14 | 5.14 | 5.14 | 0.742 |
| fill 4K cold | splitmix | 4.82 | 4.82 | 4.83 | 0.791 |
| fill 4K cold | xoshiro | 3.11 | 3.11 | 3.11 | 1.23 |
| fill 4K cold | chacha8 | 2.67 | 2.67 | 2.67 | 1.43 |
| fill 4K cold | aes-ctr | 4.67 | 4.64 | 4.68 | 0.816 |
| fill+crc 4K cold | stamped | 3.83 | 3.83 | 3.84 | 0.995 |
| fill+crc 4K cold | splitmix | 3.47 | 3.46 | 3.48 | 1.10 |
| fill+crc 4K cold | xoshiro | 2.93 | 2.93 | 2.94 | 1.30 |
| fill+crc 4K cold | chacha8 | 2.21 | 2.21 | 2.21 | 1.73 |
| fill+crc 4K cold | aes-ctr | 3.30 | 3.30 | 3.31 | 1.16 |
| verify 4K cold | stamped | 6.01 | 6.01 | 6.06 | 0.634 |
| verify 4K cold | splitmix | 3.74 | 3.74 | 3.75 | 1.02 |
| verify 4K cold | xoshiro | 3.40 | 3.39 | 3.40 | 1.12 |
| verify 4K cold | chacha8 | 2.27 | 2.26 | 2.27 | 1.68 |
| verify 4K cold | aes-ctr | 4.41 | 4.41 | 4.42 | 0.864 |
| crc 4K cold | crc64nvme | 11.51 | 11.37 | 11.51 | 0.332 |
| copy 4K cold | memcpy | 5.30 | 5.30 | 5.31 | 0.719 |
| fill 4K hot | stamped | 29.71 | 29.70 | 29.72 | 0.128 |
| fill 4K hot | splitmix | 4.91 | 4.90 | 4.91 | 0.778 |
| fill 4K hot | xoshiro | 4.55 | 4.55 | 4.56 | 0.838 |
| fill 4K hot | chacha8 | 2.67 | 2.67 | 2.67 | 1.43 |
| fill 4K hot | aes-ctr | 6.35 | 6.34 | 6.35 | 0.601 |
| fill+crc 4K hot | stamped | 8.90 | 8.89 | 8.91 | 0.429 |
| fill+crc 4K hot | splitmix | 3.55 | 3.55 | 3.55 | 1.07 |
| fill+crc 4K hot | xoshiro | 3.32 | 3.32 | 3.33 | 1.15 |
| fill+crc 4K hot | chacha8 | 2.21 | 2.21 | 2.21 | 1.72 |
| fill+crc 4K hot | aes-ctr | 4.24 | 4.24 | 4.24 | 0.899 |
| verify 4K hot | stamped | 17.75 | 17.75 | 17.76 | 0.215 |
| verify 4K hot | splitmix | 4.45 | 4.44 | 4.45 | 0.858 |
| verify 4K hot | xoshiro | 4.09 | 4.08 | 4.09 | 0.933 |
| verify 4K hot | chacha8 | 2.52 | 2.52 | 2.53 | 1.51 |
| verify 4K hot | aes-ctr | 5.57 | 5.57 | 5.57 | 0.685 |
| crc 4K hot | crc64nvme | 12.91 | 12.89 | 12.91 | 0.295 |
| copy 4K hot | memcpy | 29.47 | 29.46 | 29.48 | 0.129 |
| fill 64K cold | stamped | 6.32 | 6.29 | 6.39 | 9.66 |
| fill 64K cold | splitmix | 4.88 | 4.88 | 4.88 | 12.51 |
| fill 64K cold | xoshiro | 3.43 | 3.43 | 3.43 | 17.80 |
| fill 64K cold | chacha8 | 2.78 | 2.77 | 2.78 | 21.99 |
| fill 64K cold | aes-ctr | 3.90 | 3.86 | 3.90 | 15.66 |
| fill+crc 64K cold | stamped | 4.30 | 4.30 | 4.34 | 14.18 |
| fill+crc 64K cold | splitmix | 3.58 | 3.57 | 3.58 | 17.07 |
| fill+crc 64K cold | xoshiro | 2.78 | 2.77 | 2.78 | 21.99 |
| fill+crc 64K cold | chacha8 | 2.29 | 2.29 | 2.29 | 26.60 |
| fill+crc 64K cold | aes-ctr | 2.92 | 2.92 | 2.93 | 20.87 |
| verify 64K cold | stamped | 7.16 | 7.13 | 7.17 | 8.52 |
| verify 64K cold | splitmix | 3.74 | 3.74 | 3.75 | 16.30 |
| verify 64K cold | xoshiro | 3.80 | 3.80 | 3.80 | 16.07 |
| verify 64K cold | chacha8 | 2.33 | 2.33 | 2.33 | 26.17 |
| verify 64K cold | aes-ctr | 4.60 | 4.56 | 4.61 | 13.27 |
| crc 64K cold | crc64nvme | 11.51 | 11.41 | 11.52 | 5.30 |
| copy 64K cold | memcpy | 6.89 | 6.89 | 6.89 | 8.86 |
| fill 64K hot | stamped | 27.16 | 27.15 | 27.18 | 2.25 |
| fill 64K hot | splitmix | 4.99 | 4.98 | 4.99 | 12.24 |
| fill 64K hot | xoshiro | 4.95 | 4.95 | 4.96 | 12.32 |
| fill 64K hot | chacha8 | 2.76 | 2.76 | 2.76 | 22.10 |
| fill 64K hot | aes-ctr | 6.73 | 6.71 | 6.76 | 9.06 |
| fill+crc 64K hot | stamped | 8.88 | 8.88 | 8.89 | 6.87 |
| fill+crc 64K hot | splitmix | 3.62 | 3.62 | 3.63 | 16.84 |
| fill+crc 64K hot | xoshiro | 3.66 | 3.63 | 3.66 | 16.69 |
| fill+crc 64K hot | chacha8 | 2.29 | 2.29 | 2.29 | 26.67 |
| fill+crc 64K hot | aes-ctr | 4.46 | 4.45 | 4.48 | 13.69 |
| verify 64K hot | stamped | 15.18 | 15.17 | 15.18 | 4.02 |
| verify 64K hot | splitmix | 4.35 | 4.35 | 4.35 | 14.04 |
| verify 64K hot | xoshiro | 4.39 | 4.38 | 4.40 | 13.90 |
| verify 64K hot | chacha8 | 2.55 | 2.55 | 2.55 | 23.92 |
| verify 64K hot | aes-ctr | 5.58 | 5.58 | 5.58 | 10.93 |
| crc 64K hot | crc64nvme | 13.17 | 13.17 | 13.18 | 4.63 |
| copy 64K hot | memcpy | 30.14 | 30.12 | 30.15 | 2.02 |
| fill 1M cold | stamped | 6.37 | 6.32 | 6.48 | 153.4 |
| fill 1M cold | splitmix | 4.90 | 4.90 | 4.90 | 199.4 |
| fill 1M cold | xoshiro | 3.45 | 3.44 | 3.45 | 282.8 |
| fill 1M cold | chacha8 | 2.79 | 2.78 | 2.79 | 350.6 |
| fill 1M cold | aes-ctr | 4.26 | 4.25 | 4.26 | 229.3 |
| fill+crc 1M cold | stamped | 4.47 | 4.47 | 4.48 | 218.3 |
| fill+crc 1M cold | splitmix | 3.55 | 3.55 | 3.55 | 275.2 |
| fill+crc 1M cold | xoshiro | 2.91 | 2.91 | 2.91 | 335.7 |
| fill+crc 1M cold | chacha8 | 2.29 | 2.29 | 2.29 | 426.1 |
| fill+crc 1M cold | aes-ctr | 3.18 | 3.18 | 3.19 | 306.7 |
| verify 1M cold | stamped | 6.97 | 6.97 | 7.06 | 140.0 |
| verify 1M cold | splitmix | 3.68 | 3.67 | 3.68 | 265.7 |
| verify 1M cold | xoshiro | 3.71 | 3.71 | 3.72 | 262.9 |
| verify 1M cold | chacha8 | 2.32 | 2.31 | 2.32 | 421.0 |
| verify 1M cold | aes-ctr | 4.48 | 4.42 | 4.48 | 218.2 |
| crc 1M cold | crc64nvme | 11.54 | 11.44 | 11.56 | 84.60 |
| copy 1M cold | memcpy | 6.74 | 6.74 | 6.75 | 145.0 |
| fill 1M hot | stamped | 27.15 | 27.11 | 27.17 | 35.97 |
| fill 1M hot | splitmix | 5.00 | 4.99 | 5.00 | 195.2 |
| fill 1M hot | xoshiro | 4.99 | 4.99 | 5.01 | 195.6 |
| fill 1M hot | chacha8 | 2.77 | 2.77 | 2.78 | 352.4 |
| fill 1M hot | aes-ctr | 6.71 | 6.71 | 6.72 | 145.5 |
| fill+crc 1M hot | stamped | 8.89 | 8.89 | 8.90 | 109.9 |
| fill+crc 1M hot | splitmix | 3.63 | 3.62 | 3.63 | 269.1 |
| fill+crc 1M hot | xoshiro | 3.66 | 3.64 | 3.66 | 266.8 |
| fill+crc 1M hot | chacha8 | 2.29 | 2.29 | 2.29 | 426.4 |
| fill+crc 1M hot | aes-ctr | 4.46 | 4.46 | 4.47 | 218.7 |
| verify 1M hot | stamped | 13.62 | 13.61 | 13.62 | 71.68 |
| verify 1M hot | splitmix | 4.32 | 4.28 | 4.33 | 225.8 |
| verify 1M hot | xoshiro | 4.38 | 3.79 | 4.39 | 222.8 |
| verify 1M hot | chacha8 | 2.55 | 2.27 | 2.55 | 382.3 |
| verify 1M hot | aes-ctr | 5.58 | 5.37 | 5.58 | 175.1 |
| crc 1M hot | crc64nvme | 13.20 | 13.20 | 13.21 | 73.96 |
| copy 1M hot | memcpy | 29.74 | 29.74 | 29.75 | 32.83 |

### 3a. Making bytes on several cores, no wire, round 3

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | GiB/s, all | GiB/s, least core |
| --- | --- | --- | --- |
| fill+crc 1M cold x1 | stamped | 4.42 | 4.42 |
| fill+crc 1M cold x1 | splitmix | 3.55 | 3.55 |
| fill+crc 1M cold x1 | xoshiro | 2.93 | 2.93 |
| fill+crc 1M cold x1 | chacha8 | 2.29 | 2.29 |
| fill+crc 1M cold x1 | aes-ctr | 3.21 | 3.21 |
| fill+crc 1M cold x2 | stamped | 6.36 | 3.18 |
| fill+crc 1M cold x2 | splitmix | 6.33 | 3.16 |
| fill+crc 1M cold x2 | xoshiro | 5.59 | 2.77 |
| fill+crc 1M cold x2 | chacha8 | 4.37 | 2.18 |
| fill+crc 1M cold x2 | aes-ctr | 5.98 | 2.99 |
| fill+crc 1M cold x4 | stamped | 6.93 | 1.70 |
| fill+crc 1M cold x4 | splitmix | 9.53 | 2.32 |
| fill+crc 1M cold x4 | xoshiro | 8.45 | 2.06 |
| fill+crc 1M cold x4 | chacha8 | 6.77 | 1.67 |
| fill+crc 1M cold x4 | aes-ctr | 8.33 | 2.05 |

### 2. Against a server that discards, round 3

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | pattern | 3568 | 287.0 | 1.000 | 1.00 | 3568 | 289.7 | 1.000 | 587.7 | 0 | 0 |
| put 1M plain | fill stamped | 2535 | 404.0 | 1.000 | 1.00 | 2535 | 307.7 | 0.752 | 721.4 | 0 | 0 |
| put 1M plain | fill+crc stamped | 2178 | 470.2 | 1.000 | 1.00 | 2178 | 298.0 | 0.624 | 778.6 | 0 | 0 |
| put 1M plain | fill splitmix | 2090 | 489.9 | 1.000 | 1.00 | 2090 | 303.5 | 0.610 | 799.4 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 1816 | 563.7 | 1.000 | 0.998 | 1816 | 301.4 | 0.525 | 885.1 | 0 | 0 |
| put 1M plain | fill xoshiro | 1954 | 523.5 | 0.999 | 1.00 | 1956 | 302.8 | 0.568 | 840.4 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 1697 | 603.2 | 1.000 | 1.00 | 1698 | 303.3 | 0.493 | 909.7 | 0 | 0 |
| put 1M plain | fill chacha8 | 1556 | 658.3 | 1.000 | 1.00 | 1556 | 317.6 | 0.473 | 980.7 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 1414 | 724.0 | 1.000 | 1.00 | 1414 | 331.5 | 0.448 | 1080 | 0 | 0 |
| put 1M plain | fill aes-ctr | 2159 | 474.3 | 1.000 | 1.00 | 2159 | 292.9 | 0.608 | 793.0 | 0 | 0 |
| put 1M plain | fill+crc aes-ctr | 1858 | 551.0 | 1.000 | 1.00 | 1858 | 293.7 | 0.523 | 860.6 | 0 | 0 |
| get 1M plain | none | 3264 | 179.1 | 0.571 | 0.592 | 5718 | 316.5 | 1.000 | 454.1 | 0 | 0 |
| get 1M plain | crc | 3225 | 203.9 | 0.642 | 0.672 | 5022 | 320.4 | 1.000 | 502.2 | 0 | 0 |
| get 1M plain | regenerate stamped | 3026 | 256.9 | 0.759 | 0.804 | 3986 | 341.7 | 1.000 | 604.8 | 0 | 0 |
| get 1M plain | regenerate splitmix | 2478 | 379.9 | 0.919 | 0.942 | 2695 | 336.0 | 0.802 | 842.8 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 2554 | 368.7 | 0.920 | 0.941 | 2777 | 323.2 | 0.796 | 678.1 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 1743 | 587.3 | 1.000 | 1.00 | 1744 | 288.7 | 0.482 | 874.0 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 3320 | 290.4 | 0.942 | 0.959 | 3526 | 311.2 | 0.999 | 603.2 | 0 | 0 |
| put 1M ktls | pattern | 656.3 | 788.0 | 0.505 | 0.508 | 1299 | 1138 | 0.720 | 1994 | 0 | 1.00 |
| put 1M ktls | fill stamped | 960.4 | 927.8 | 0.870 | 0.872 | 1104 | 933.2 | 0.865 | 1910 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 614.1 | 976.8 | 0.586 | 0.588 | 1048 | 1190 | 0.704 | 2217 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 640.7 | 959.9 | 0.601 | 0.604 | 1067 | 1145 | 0.707 | 2154 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 729.5 | 1038 | 0.739 | 0.743 | 986.5 | 1009 | 0.709 | 2136 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 1028 | 995.6 | 0.999 | 1.00 | 1029 | 911.0 | 0.904 | 1956 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 649.5 | 1049 | 0.666 | 0.668 | 975.8 | 1138 | 0.712 | 2248 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 716.3 | 1121 | 0.784 | 0.785 | 913.5 | 1038 | 0.716 | 2190 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 604.1 | 1191 | 0.703 | 0.705 | 859.5 | 1203 | 0.700 | 2424 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 718.9 | 949.9 | 0.667 | 0.675 | 1078 | 1055 | 0.730 | 2321 | 0 | 1.00 |
| put 1M ktls | fill+crc aes-ctr | 614.7 | 1003 | 0.602 | 0.606 | 1021 | 1160 | 0.686 | 2245 | 0 | 1.00 |
| get 1M ktls | none | 675.9 | 934.7 | 0.617 | 0.635 | 1096 | 837.3 | 0.543 | 1806 | 0 | 1.00 |
| get 1M ktls | crc | 647.7 | 995.6 | 0.630 | 0.646 | 1029 | 837.0 | 0.519 | 1878 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 565.2 | 1175 | 0.649 | 0.666 | 871.2 | 892.9 | 0.483 | 2148 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 561.5 | 1199 | 0.658 | 0.671 | 854.0 | 885.5 | 0.476 | 2170 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 551.5 | 1224 | 0.659 | 0.672 | 836.9 | 897.0 | 0.473 | 2213 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 504.5 | 1356 | 0.668 | 0.689 | 755.1 | 940.4 | 0.453 | 2378 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 561.3 | 1198 | 0.657 | 0.672 | 854.8 | 889.1 | 0.478 | 2138 | 0 | 1.00 |

### 3b. The put from several client cores, round 3

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain x1 | fill+crc splitmix | 1813 | 564.7 | 1.000 | 1.00 | 1813 | 300.4 | 0.522 | 880.8 | 0 | 0 |
| put 1M plain x1 | fill+crc xoshiro | 1712 | 597.9 | 1.000 | 1.00 | 1713 | 306.1 | 0.502 | 928.0 | 0 | 0 |
| put 1M plain x1 | fill+crc chacha8 | 1414 | 724.4 | 1.000 | 1.00 | 1414 | 333.2 | 0.451 | 1065 | 0 | 0 |
| put 1M plain x1 | fill+crc aes-ctr | 1833 | 558.6 | 1.000 | 1.00 | 1833 | 299.0 | 0.525 | 1029 | 0 | 0 |
| put 1M plain x2 | fill+crc splitmix | 3160 | 647.6 | 0.999 | 1.00 | 1581 | 344.6 | 0.540 | 1008 | 0 | 0 |
| put 1M plain x2 | fill+crc xoshiro | 2874 | 712.5 | 1.000 | 1.00 | 1437 | 371.9 | 0.526 | 1100 | 0 | 0 |
| put 1M plain x2 | fill+crc chacha8 | 2563 | 798.8 | 1.000 | 1.00 | 1282 | 349.9 | 0.440 | 1168 | 0 | 0 |
| put 1M plain x2 | fill+crc aes-ctr | 2915 | 702.4 | 1.000 | 1.00 | 1458 | 395.5 | 0.574 | 1104 | 0 | 0 |

### 4. A folder of real files, round 3

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | MiB/s | cpu ms/GiB | device MiB read | from device |
| --- | --- | --- | --- | --- | --- |
| 64K cold | read | 391.4 | 783.7 | 2048 | 1.00 |
| 64K cold | read+crc | 380.7 | 857.5 | 2048 | 1.00 |
| 64K cold | read+sha256 | 312.3 | 1427 | 2048 | 1.00 |
| 64K hot | read | 5242 | 195.3 | 0 | 0 |
| 64K hot | read+crc | 3778 | 271.0 | 0 | 0 |
| 64K hot | read+sha256 | 1239 | 826.7 | 0 | 0 |
| 1M cold | read | 768.0 | 336.2 | 2048 | 1.00 |
| 1M cold | read+crc | 730.3 | 411.5 | 2048 | 1.00 |
| 1M cold | read+sha256 | 528.9 | 945.8 | 2048 | 1.00 |
| 1M hot | read | 9483 | 108.0 | 0 | 0 |
| 1M hot | read+crc | 5468 | 187.2 | 0 | 0 |
| 1M hot | read+sha256 | 1426 | 718.2 | 0 | 0 |
| 64M cold | read | 856.2 | 270.9 | 2048 | 1.00 |
| 64M cold | read+crc | 860.0 | 346.9 | 2048 | 1.00 |
| 64M cold | read+sha256 | 600.7 | 880.0 | 2048 | 1.00 |
| 64M hot | read | 10503 | 97.49 | 0 | 0 |
| 64M hot | read+crc | 5759 | 177.8 | 0 | 0 |
| 64M hot | read+sha256 | 1450 | 706.1 | 0 | 0 |

