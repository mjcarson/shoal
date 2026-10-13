### Checks, round 3

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
| describe fixed | expand | 1.00 | 3071421 |
| describe uniform | expand | 1.00 | 3566046 |
| describe doublings | expand | 1.00 | 4004744 |
| describe table | expand | 1.00 | 3271703 |

### 1. Making bytes on one core, GiB/s, round 3

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 5.19 | 5.18 | 5.19 | 0.735 |
| fill 4K cold | splitmix | 4.83 | 4.81 | 4.83 | 0.790 |
| fill 4K cold | xoshiro | 3.12 | 3.12 | 3.12 | 1.22 |
| fill 4K cold | chacha8 | 2.67 | 2.67 | 2.67 | 1.43 |
| fill 4K cold | aes-ctr | 4.67 | 4.67 | 4.68 | 0.816 |
| fill+crc 4K cold | stamped | 3.82 | 3.81 | 3.83 | 0.998 |
| fill+crc 4K cold | splitmix | 3.45 | 3.45 | 3.46 | 1.10 |
| fill+crc 4K cold | xoshiro | 2.85 | 2.83 | 2.85 | 1.34 |
| fill+crc 4K cold | chacha8 | 2.21 | 2.21 | 2.21 | 1.73 |
| fill+crc 4K cold | aes-ctr | 3.25 | 3.23 | 3.28 | 1.18 |
| verify 4K cold | stamped | 6.06 | 6.06 | 6.10 | 0.629 |
| verify 4K cold | splitmix | 3.76 | 3.76 | 3.76 | 1.01 |
| verify 4K cold | xoshiro | 3.40 | 3.40 | 3.40 | 1.12 |
| verify 4K cold | chacha8 | 2.28 | 2.28 | 2.28 | 1.67 |
| verify 4K cold | aes-ctr | 4.43 | 4.43 | 4.43 | 0.862 |
| crc 4K cold | crc64nvme | 11.39 | 11.34 | 11.51 | 0.335 |
| copy 4K cold | memcpy | 5.15 | 5.14 | 5.15 | 0.741 |
| fill 4K hot | stamped | 29.66 | 29.65 | 29.70 | 0.129 |
| fill 4K hot | splitmix | 4.91 | 4.90 | 4.91 | 0.778 |
| fill 4K hot | xoshiro | 4.56 | 4.55 | 4.56 | 0.837 |
| fill 4K hot | chacha8 | 2.67 | 2.66 | 2.67 | 1.43 |
| fill 4K hot | aes-ctr | 6.34 | 6.34 | 6.34 | 0.602 |
| fill+crc 4K hot | stamped | 8.88 | 8.88 | 8.88 | 0.430 |
| fill+crc 4K hot | splitmix | 3.54 | 3.54 | 3.55 | 1.08 |
| fill+crc 4K hot | xoshiro | 3.35 | 3.34 | 3.35 | 1.14 |
| fill+crc 4K hot | chacha8 | 2.21 | 2.21 | 2.21 | 1.72 |
| fill+crc 4K hot | aes-ctr | 4.24 | 4.24 | 4.24 | 0.900 |
| verify 4K hot | stamped | 17.75 | 17.73 | 17.76 | 0.215 |
| verify 4K hot | splitmix | 4.45 | 4.44 | 4.45 | 0.858 |
| verify 4K hot | xoshiro | 4.08 | 4.07 | 4.11 | 0.935 |
| verify 4K hot | chacha8 | 2.52 | 2.52 | 2.52 | 1.51 |
| verify 4K hot | aes-ctr | 5.56 | 5.56 | 5.56 | 0.686 |
| crc 4K hot | crc64nvme | 12.89 | 12.88 | 12.89 | 0.296 |
| copy 4K hot | memcpy | 29.46 | 29.34 | 29.46 | 0.130 |
| fill 64K cold | stamped | 6.39 | 6.28 | 6.39 | 9.55 |
| fill 64K cold | splitmix | 4.90 | 4.89 | 4.90 | 12.47 |
| fill 64K cold | xoshiro | 3.45 | 3.45 | 3.45 | 17.69 |
| fill 64K cold | chacha8 | 2.78 | 2.78 | 2.78 | 21.95 |
| fill 64K cold | aes-ctr | 3.94 | 3.94 | 3.94 | 15.50 |
| fill+crc 64K cold | stamped | 4.31 | 4.28 | 4.31 | 14.17 |
| fill+crc 64K cold | splitmix | 3.58 | 3.58 | 3.58 | 17.04 |
| fill+crc 64K cold | xoshiro | 2.79 | 2.79 | 2.80 | 21.84 |
| fill+crc 64K cold | chacha8 | 2.29 | 2.29 | 2.30 | 26.60 |
| fill+crc 64K cold | aes-ctr | 2.95 | 2.95 | 2.95 | 20.68 |
| verify 64K cold | stamped | 7.17 | 7.17 | 7.18 | 8.51 |
| verify 64K cold | splitmix | 3.75 | 3.74 | 3.75 | 16.29 |
| verify 64K cold | xoshiro | 3.81 | 3.80 | 3.81 | 16.03 |
| verify 64K cold | chacha8 | 2.34 | 2.33 | 2.34 | 26.13 |
| verify 64K cold | aes-ctr | 4.60 | 4.59 | 4.61 | 13.27 |
| crc 64K cold | crc64nvme | 11.40 | 11.37 | 11.50 | 5.35 |
| copy 64K cold | memcpy | 6.69 | 6.69 | 6.69 | 9.12 |
| fill 64K hot | stamped | 27.09 | 27.08 | 27.18 | 2.25 |
| fill 64K hot | splitmix | 4.98 | 4.97 | 4.98 | 12.26 |
| fill 64K hot | xoshiro | 4.98 | 4.96 | 4.99 | 12.26 |
| fill 64K hot | chacha8 | 2.76 | 2.76 | 2.76 | 22.14 |
| fill 64K hot | aes-ctr | 6.72 | 6.68 | 6.74 | 9.08 |
| fill+crc 64K hot | stamped | 8.88 | 8.87 | 8.89 | 6.88 |
| fill+crc 64K hot | splitmix | 3.61 | 3.60 | 3.61 | 16.92 |
| fill+crc 64K hot | xoshiro | 3.65 | 3.65 | 3.65 | 16.71 |
| fill+crc 64K hot | chacha8 | 2.29 | 2.28 | 2.29 | 26.71 |
| fill+crc 64K hot | aes-ctr | 4.44 | 4.44 | 4.46 | 13.74 |
| verify 64K hot | stamped | 15.16 | 15.15 | 15.16 | 4.03 |
| verify 64K hot | splitmix | 4.34 | 4.34 | 4.34 | 14.06 |
| verify 64K hot | xoshiro | 4.39 | 4.37 | 4.39 | 13.92 |
| verify 64K hot | chacha8 | 2.55 | 2.55 | 2.55 | 23.96 |
| verify 64K hot | aes-ctr | 5.57 | 5.57 | 5.58 | 10.96 |
| crc 64K hot | crc64nvme | 13.16 | 13.16 | 13.16 | 4.64 |
| copy 64K hot | memcpy | 30.27 | 30.18 | 30.30 | 2.02 |
| fill 1M cold | stamped | 6.46 | 6.38 | 6.46 | 151.1 |
| fill 1M cold | splitmix | 4.91 | 4.90 | 4.91 | 199.0 |
| fill 1M cold | xoshiro | 3.48 | 3.48 | 3.48 | 280.9 |
| fill 1M cold | chacha8 | 2.79 | 2.79 | 2.79 | 350.0 |
| fill 1M cold | aes-ctr | 4.28 | 4.28 | 4.28 | 228.3 |
| fill+crc 1M cold | stamped | 4.45 | 4.42 | 4.45 | 219.5 |
| fill+crc 1M cold | splitmix | 3.54 | 3.51 | 3.55 | 275.6 |
| fill+crc 1M cold | xoshiro | 2.92 | 2.91 | 2.92 | 335.0 |
| fill+crc 1M cold | chacha8 | 2.29 | 2.29 | 2.29 | 427.3 |
| fill+crc 1M cold | aes-ctr | 3.20 | 3.20 | 3.20 | 305.4 |
| verify 1M cold | stamped | 6.99 | 6.99 | 7.06 | 139.7 |
| verify 1M cold | splitmix | 3.68 | 3.68 | 3.68 | 265.1 |
| verify 1M cold | xoshiro | 3.72 | 3.72 | 3.72 | 262.7 |
| verify 1M cold | chacha8 | 2.32 | 2.32 | 2.32 | 420.4 |
| verify 1M cold | aes-ctr | 4.49 | 4.49 | 4.49 | 217.6 |
| crc 1M cold | crc64nvme | 11.52 | 11.44 | 11.55 | 84.74 |
| copy 1M cold | memcpy | 6.52 | 6.36 | 6.52 | 149.8 |
| fill 1M hot | stamped | 27.01 | 27.01 | 27.02 | 36.15 |
| fill 1M hot | splitmix | 4.99 | 4.99 | 4.99 | 195.8 |
| fill 1M hot | xoshiro | 4.98 | 4.97 | 4.99 | 196.3 |
| fill 1M hot | chacha8 | 2.77 | 2.77 | 2.77 | 352.2 |
| fill 1M hot | aes-ctr | 6.70 | 6.70 | 6.71 | 145.7 |
| fill+crc 1M hot | stamped | 8.90 | 8.90 | 8.90 | 109.8 |
| fill+crc 1M hot | splitmix | 3.62 | 3.62 | 3.62 | 269.4 |
| fill+crc 1M hot | xoshiro | 3.66 | 3.66 | 3.66 | 267.1 |
| fill+crc 1M hot | chacha8 | 2.29 | 2.29 | 2.29 | 426.8 |
| fill+crc 1M hot | aes-ctr | 4.46 | 4.46 | 4.46 | 219.1 |
| verify 1M hot | stamped | 12.68 | 12.68 | 12.70 | 77.03 |
| verify 1M hot | splitmix | 4.34 | 4.33 | 4.34 | 225.2 |
| verify 1M hot | xoshiro | 4.38 | 4.38 | 4.38 | 223.0 |
| verify 1M hot | chacha8 | 2.55 | 2.52 | 2.55 | 382.8 |
| verify 1M hot | aes-ctr | 5.57 | 5.57 | 5.58 | 175.2 |
| crc 1M hot | crc64nvme | 13.20 | 13.19 | 13.20 | 74.00 |
| copy 1M hot | memcpy | 29.72 | 29.64 | 29.74 | 32.86 |

### 3a. Making bytes on several cores, no wire, round 3

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | GiB/s, all | GiB/s, least core |
| --- | --- | --- | --- |
| fill+crc 1M cold x1 | stamped | 4.38 | 4.38 |
| fill+crc 1M cold x1 | splitmix | 3.54 | 3.54 |
| fill+crc 1M cold x1 | xoshiro | 2.95 | 2.95 |
| fill+crc 1M cold x1 | chacha8 | 2.28 | 2.28 |
| fill+crc 1M cold x1 | aes-ctr | 3.22 | 3.22 |
| fill+crc 1M cold x2 | stamped | 6.56 | 3.28 |
| fill+crc 1M cold x2 | splitmix | 6.37 | 3.18 |
| fill+crc 1M cold x2 | xoshiro | 5.56 | 2.75 |
| fill+crc 1M cold x2 | chacha8 | 4.38 | 2.18 |
| fill+crc 1M cold x2 | aes-ctr | 6.00 | 3.00 |
| fill+crc 1M cold x4 | stamped | 6.84 | 1.68 |
| fill+crc 1M cold x4 | splitmix | 9.40 | 2.32 |
| fill+crc 1M cold x4 | xoshiro | 8.37 | 2.06 |
| fill+crc 1M cold x4 | chacha8 | 6.74 | 1.67 |
| fill+crc 1M cold x4 | aes-ctr | 8.11 | 2.00 |

### 2. Against a server that discards, round 3

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | pattern | 3526 | 290.4 | 1.000 | 1.00 | 3526 | 292.9 | 1.000 | 590.0 | 0 | 0 |
| put 1M plain | fill stamped | 2506 | 408.6 | 1.00 | 1.00 | 2506 | 314.3 | 0.760 | 710.1 | 0 | 0 |
| put 1M plain | fill+crc stamped | 2171 | 471.6 | 1.000 | 1.00 | 2171 | 299.8 | 0.627 | 772.4 | 0 | 0 |
| put 1M plain | fill splitmix | 2088 | 490.4 | 1.000 | 1.00 | 2088 | 306.6 | 0.616 | 809.0 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 1810 | 565.5 | 1.000 | 1.00 | 1811 | 288.9 | 0.502 | 863.0 | 0 | 0 |
| put 1M plain | fill xoshiro | 1967 | 520.7 | 1.000 | 1.00 | 1967 | 278.3 | 0.526 | 799.6 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 1690 | 606.0 | 1.000 | 1.00 | 1690 | 306.7 | 0.497 | 908.9 | 0 | 0 |
| put 1M plain | fill chacha8 | 1547 | 661.8 | 1.000 | 1.00 | 1547 | 320.3 | 0.475 | 983.5 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 1407 | 727.8 | 1.000 | 1.00 | 1407 | 335.1 | 0.452 | 1080 | 0 | 0 |
| put 1M plain | fill aes-ctr | 2141 | 478.3 | 1.00 | 1.00 | 2141 | 298.3 | 0.615 | 791.0 | 0 | 0 |
| put 1M plain | fill+crc aes-ctr | 1836 | 557.5 | 1.000 | 1.00 | 1837 | 300.1 | 0.529 | 851.9 | 0 | 0 |
| get 1M plain | none | 3230 | 181.7 | 0.573 | 0.606 | 5634 | 319.8 | 1.000 | 462.8 | 0 | 0 |
| get 1M plain | crc | 3169 | 205.3 | 0.635 | 0.670 | 4987 | 326.1 | 1.00 | 518.3 | 0 | 0 |
| get 1M plain | regenerate stamped | 2928 | 267.4 | 0.765 | 0.802 | 3829 | 353.1 | 1.000 | 618.3 | 0 | 0 |
| get 1M plain | regenerate splitmix | 2545 | 370.7 | 0.922 | 0.942 | 2762 | 327.4 | 0.805 | 695.1 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 2553 | 369.3 | 0.921 | 0.941 | 2773 | 326.7 | 0.805 | 684.0 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 1690 | 559.4 | 0.923 | 0.940 | 1831 | 336.5 | 0.546 | 887.1 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 3241 | 295.2 | 0.934 | 0.952 | 3468 | 318.0 | 0.997 | 616.0 | 0 | 0 |
| put 1M ktls | pattern | 945.7 | 792.9 | 0.732 | 0.735 | 1292 | 894.1 | 0.816 | 1728 | 0 | 1.00 |
| put 1M ktls | fill stamped | 1098 | 930.9 | 0.999 | 0.998 | 1100 | 905.0 | 0.962 | 1847 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 630.3 | 977.1 | 0.601 | 0.605 | 1048 | 1161 | 0.705 | 2177 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 743.3 | 961.9 | 0.698 | 0.703 | 1065 | 1037 | 0.744 | 2003 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 966.6 | 1057 | 0.998 | 0.998 | 968.9 | 936.2 | 0.874 | 2034 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 608.3 | 981.9 | 0.583 | 0.586 | 1043 | 1211 | 0.710 | 2279 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 961.5 | 1065 | 1.000 | 1.00 | 961.7 | 931.4 | 0.865 | 2015 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 627.9 | 1119 | 0.686 | 0.689 | 915.1 | 1136 | 0.687 | 2283 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 842.9 | 1212 | 0.997 | 0.998 | 845.2 | 932.9 | 0.759 | 2177 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 683.3 | 940.1 | 0.627 | 0.631 | 1089 | 1155 | 0.761 | 2158 | 0 | 1.00 |
| put 1M ktls | fill+crc aes-ctr | 1002 | 1021 | 0.999 | 0.998 | 1003 | 941.5 | 0.912 | 1988 | 0 | 1.00 |
| get 1M ktls | none | 641.9 | 967.8 | 0.607 | 0.627 | 1058 | 831.6 | 0.512 | 1825 | 0 | 1.00 |
| get 1M ktls | crc | 647.1 | 994.9 | 0.629 | 0.643 | 1029 | 838.4 | 0.521 | 1861 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 568.2 | 1178 | 0.654 | 0.669 | 869.1 | 883.9 | 0.481 | 2122 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 559.1 | 1197 | 0.654 | 0.667 | 855.4 | 894.5 | 0.479 | 2153 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 552.7 | 1219 | 0.658 | 0.671 | 840.1 | 890.3 | 0.471 | 2134 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 505.3 | 1354 | 0.668 | 0.686 | 756.3 | 936.9 | 0.453 | 2322 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 606.1 | 1259 | 0.745 | 0.769 | 813.1 | 907.3 | 0.528 | 2226 | 0 | 1.00 |

### 3b. The put from several client cores, round 3

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain x1 | fill+crc splitmix | 1803 | 567.8 | 1.000 | 1.00 | 1803 | 307.5 | 0.532 | 884.6 | 0 | 0 |
| put 1M plain x1 | fill+crc xoshiro | 1707 | 599.8 | 1.000 | 1.00 | 1707 | 279.4 | 0.457 | 874.4 | 0 | 0 |
| put 1M plain x1 | fill+crc chacha8 | 1404 | 729.1 | 1.000 | 1.00 | 1405 | 337.2 | 0.453 | 1067 | 0 | 0 |
| put 1M plain x1 | fill+crc aes-ctr | 1839 | 556.8 | 1.000 | 1.00 | 1839 | 298.2 | 0.527 | 855.2 | 0 | 0 |
| put 1M plain x2 | fill+crc splitmix | 3185 | 643.0 | 1.000 | 1.00 | 1592 | 349.7 | 0.551 | 999.8 | 0 | 0 |
| put 1M plain x2 | fill+crc xoshiro | 2878 | 711.0 | 0.999 | 0.999 | 1440 | 378.5 | 0.534 | 1095 | 0 | 0 |
| put 1M plain x2 | fill+crc chacha8 | 2513 | 814.9 | 1.00 | 1.00 | 1257 | 360.7 | 0.444 | 1172 | 0 | 0 |
| put 1M plain x2 | fill+crc aes-ctr | 2953 | 693.6 | 1.00 | 1.00 | 1476 | 391.4 | 0.569 | 1087 | 0 | 0 |

### 4. A folder of real files, round 3

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | MiB/s | cpu ms/GiB | device MiB read | from device |
| --- | --- | --- | --- | --- | --- |
| 64K cold | read | 378.3 | 783.1 | 2048 | 1.00 |
| 64K cold | read+crc | 369.0 | 858.7 | 2048 | 1.00 |
| 64K cold | read+sha256 | 306.0 | 1425 | 2048 | 1.00 |
| 64K hot | read | 5365 | 190.8 | 0 | 0 |
| 64K hot | read+crc | 3853 | 265.8 | 0 | 0 |
| 64K hot | read+sha256 | 1248 | 820.3 | 0 | 0 |
| 1M cold | read | 758.6 | 337.9 | 2048 | 1.00 |
| 1M cold | read+crc | 722.9 | 411.6 | 2048 | 1.00 |
| 1M cold | read+sha256 | 524.6 | 946.0 | 2048 | 1.00 |
| 1M hot | read | 9476 | 108.0 | 0 | 0 |
| 1M hot | read+crc | 5460 | 187.5 | 0 | 0 |
| 1M hot | read+sha256 | 1424 | 718.9 | 0 | 0 |
| 64M cold | read | 856.1 | 274.1 | 2048 | 1.00 |
| 64M cold | read+crc | 859.5 | 349.8 | 2048 | 1.00 |
| 64M cold | read+sha256 | 601.0 | 881.9 | 2048 | 1.00 |
| 64M hot | read | 10382 | 98.64 | 0 | 0 |
| 64M hot | read+crc | 5731 | 178.7 | 0 | 0 |
| 64M hot | read+sha256 | 1447 | 707.8 | 0 | 0 |

