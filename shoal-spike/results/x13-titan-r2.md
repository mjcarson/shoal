### Checks, round 2

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
| describe fixed | expand | 1.00 | 3151350 |
| describe uniform | expand | 1.00 | 3627609 |
| describe doublings | expand | 1.00 | 4058312 |
| describe table | expand | 1.00 | 3313492 |

### 1. Making bytes on one core, GiB/s, round 2

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 5.20 | 5.19 | 5.20 | 0.734 |
| fill 4K cold | splitmix | 4.82 | 4.73 | 4.82 | 0.792 |
| fill 4K cold | xoshiro | 3.13 | 3.13 | 3.14 | 1.22 |
| fill 4K cold | chacha8 | 2.67 | 2.67 | 2.67 | 1.43 |
| fill 4K cold | aes-ctr | 4.69 | 4.69 | 4.69 | 0.813 |
| fill+crc 4K cold | stamped | 3.87 | 3.86 | 3.87 | 0.987 |
| fill+crc 4K cold | splitmix | 3.47 | 3.46 | 3.48 | 1.10 |
| fill+crc 4K cold | xoshiro | 2.91 | 2.90 | 2.92 | 1.31 |
| fill+crc 4K cold | chacha8 | 2.21 | 2.21 | 2.21 | 1.73 |
| fill+crc 4K cold | aes-ctr | 3.15 | 3.12 | 3.16 | 1.21 |
| verify 4K cold | stamped | 6.02 | 6.02 | 6.02 | 0.634 |
| verify 4K cold | splitmix | 3.74 | 3.72 | 3.74 | 1.02 |
| verify 4K cold | xoshiro | 3.37 | 3.28 | 3.40 | 1.13 |
| verify 4K cold | chacha8 | 2.28 | 2.27 | 2.28 | 1.67 |
| verify 4K cold | aes-ctr | 4.43 | 4.43 | 4.43 | 0.862 |
| crc 4K cold | crc64nvme | 11.49 | 11.46 | 11.49 | 0.332 |
| copy 4K cold | memcpy | 5.36 | 5.35 | 5.36 | 0.712 |
| fill 4K hot | stamped | 29.68 | 29.67 | 29.69 | 0.129 |
| fill 4K hot | splitmix | 4.90 | 4.90 | 4.91 | 0.778 |
| fill 4K hot | xoshiro | 4.55 | 4.55 | 4.56 | 0.838 |
| fill 4K hot | chacha8 | 2.67 | 2.67 | 2.67 | 1.43 |
| fill 4K hot | aes-ctr | 6.34 | 6.34 | 6.35 | 0.602 |
| fill+crc 4K hot | stamped | 8.89 | 8.89 | 8.91 | 0.429 |
| fill+crc 4K hot | splitmix | 3.55 | 3.55 | 3.55 | 1.08 |
| fill+crc 4K hot | xoshiro | 3.34 | 3.34 | 3.35 | 1.14 |
| fill+crc 4K hot | chacha8 | 2.21 | 2.21 | 2.21 | 1.73 |
| fill+crc 4K hot | aes-ctr | 4.23 | 4.21 | 4.24 | 0.901 |
| verify 4K hot | stamped | 17.74 | 17.73 | 17.74 | 0.215 |
| verify 4K hot | splitmix | 4.44 | 4.44 | 4.44 | 0.859 |
| verify 4K hot | xoshiro | 4.08 | 4.07 | 4.09 | 0.936 |
| verify 4K hot | chacha8 | 2.52 | 2.52 | 2.52 | 1.51 |
| verify 4K hot | aes-ctr | 5.57 | 5.56 | 5.57 | 0.685 |
| crc 4K hot | crc64nvme | 12.91 | 12.89 | 12.91 | 0.296 |
| copy 4K hot | memcpy | 38.45 | 38.42 | 38.45 | 0.099 |
| fill 64K cold | stamped | 6.44 | 6.44 | 6.45 | 9.47 |
| fill 64K cold | splitmix | 4.87 | 4.81 | 4.88 | 12.52 |
| fill 64K cold | xoshiro | 3.41 | 3.41 | 3.41 | 17.90 |
| fill 64K cold | chacha8 | 2.77 | 2.77 | 2.78 | 22.00 |
| fill 64K cold | aes-ctr | 3.92 | 3.80 | 3.92 | 15.57 |
| fill+crc 64K cold | stamped | 4.38 | 4.38 | 4.38 | 13.95 |
| fill+crc 64K cold | splitmix | 3.55 | 3.55 | 3.55 | 17.21 |
| fill+crc 64K cold | xoshiro | 2.84 | 2.82 | 2.86 | 21.52 |
| fill+crc 64K cold | chacha8 | 2.29 | 2.29 | 2.30 | 26.61 |
| fill+crc 64K cold | aes-ctr | 2.95 | 2.95 | 2.95 | 20.70 |
| verify 64K cold | stamped | 7.15 | 7.14 | 7.16 | 8.53 |
| verify 64K cold | splitmix | 3.74 | 3.73 | 3.75 | 16.31 |
| verify 64K cold | xoshiro | 3.80 | 3.77 | 3.80 | 16.07 |
| verify 64K cold | chacha8 | 2.33 | 2.33 | 2.33 | 26.17 |
| verify 64K cold | aes-ctr | 4.59 | 4.59 | 4.60 | 13.29 |
| crc 64K cold | crc64nvme | 11.50 | 11.49 | 11.50 | 5.31 |
| copy 64K cold | memcpy | 6.95 | 6.94 | 6.95 | 8.78 |
| fill 64K hot | stamped | 27.12 | 27.10 | 27.13 | 2.25 |
| fill 64K hot | splitmix | 4.98 | 4.98 | 4.98 | 12.25 |
| fill 64K hot | xoshiro | 4.96 | 4.96 | 4.97 | 12.30 |
| fill 64K hot | chacha8 | 2.76 | 2.76 | 2.76 | 22.11 |
| fill 64K hot | aes-ctr | 6.72 | 6.70 | 6.73 | 9.08 |
| fill+crc 64K hot | stamped | 8.89 | 8.88 | 8.89 | 6.87 |
| fill+crc 64K hot | splitmix | 3.62 | 3.62 | 3.62 | 16.86 |
| fill+crc 64K hot | xoshiro | 3.65 | 3.65 | 3.66 | 16.71 |
| fill+crc 64K hot | chacha8 | 2.28 | 2.24 | 2.29 | 26.71 |
| fill+crc 64K hot | aes-ctr | 4.44 | 4.44 | 4.45 | 13.73 |
| verify 64K hot | stamped | 15.21 | 15.19 | 15.21 | 4.01 |
| verify 64K hot | splitmix | 4.35 | 4.35 | 4.35 | 14.04 |
| verify 64K hot | xoshiro | 4.39 | 4.39 | 4.39 | 13.90 |
| verify 64K hot | chacha8 | 2.55 | 2.55 | 2.55 | 23.93 |
| verify 64K hot | aes-ctr | 5.57 | 5.57 | 5.58 | 10.95 |
| crc 64K hot | crc64nvme | 13.18 | 13.18 | 13.19 | 4.63 |
| copy 64K hot | memcpy | 30.55 | 30.54 | 30.56 | 2.00 |
| fill 1M cold | stamped | 6.55 | 6.36 | 6.55 | 149.2 |
| fill 1M cold | splitmix | 4.90 | 4.90 | 4.90 | 199.3 |
| fill 1M cold | xoshiro | 3.44 | 3.43 | 3.44 | 284.1 |
| fill 1M cold | chacha8 | 2.79 | 2.78 | 2.79 | 350.5 |
| fill 1M cold | aes-ctr | 4.28 | 4.19 | 4.28 | 228.4 |
| fill+crc 1M cold | stamped | 4.52 | 4.52 | 4.52 | 216.2 |
| fill+crc 1M cold | splitmix | 3.54 | 3.54 | 3.54 | 275.6 |
| fill+crc 1M cold | xoshiro | 3.01 | 3.00 | 3.02 | 324.4 |
| fill+crc 1M cold | chacha8 | 2.29 | 2.29 | 2.29 | 425.6 |
| fill+crc 1M cold | aes-ctr | 3.23 | 3.23 | 3.23 | 302.5 |
| verify 1M cold | stamped | 6.99 | 6.98 | 6.99 | 139.8 |
| verify 1M cold | splitmix | 3.68 | 3.64 | 3.68 | 265.7 |
| verify 1M cold | xoshiro | 3.71 | 3.71 | 3.71 | 263.1 |
| verify 1M cold | chacha8 | 2.32 | 2.32 | 2.32 | 420.5 |
| verify 1M cold | aes-ctr | 4.49 | 4.49 | 4.49 | 217.5 |
| crc 1M cold | crc64nvme | 11.51 | 11.51 | 11.54 | 84.84 |
| copy 1M cold | memcpy | 6.73 | 6.72 | 6.73 | 145.1 |
| fill 1M hot | stamped | 27.14 | 27.13 | 27.15 | 35.99 |
| fill 1M hot | splitmix | 5.00 | 5.00 | 5.01 | 195.1 |
| fill 1M hot | xoshiro | 4.98 | 4.98 | 4.98 | 196.0 |
| fill 1M hot | chacha8 | 2.78 | 2.78 | 2.78 | 351.5 |
| fill 1M hot | aes-ctr | 6.71 | 6.70 | 6.72 | 145.6 |
| fill+crc 1M hot | stamped | 8.90 | 8.89 | 8.90 | 109.8 |
| fill+crc 1M hot | splitmix | 3.62 | 3.62 | 3.62 | 269.4 |
| fill+crc 1M hot | xoshiro | 3.66 | 3.66 | 3.67 | 266.7 |
| fill+crc 1M hot | chacha8 | 2.29 | 2.29 | 2.29 | 425.8 |
| fill+crc 1M hot | aes-ctr | 4.47 | 4.47 | 4.47 | 218.7 |
| verify 1M hot | stamped | 13.59 | 13.58 | 13.60 | 71.84 |
| verify 1M hot | splitmix | 4.33 | 4.33 | 4.33 | 225.6 |
| verify 1M hot | xoshiro | 4.39 | 4.38 | 4.39 | 222.7 |
| verify 1M hot | chacha8 | 2.56 | 2.55 | 2.56 | 382.0 |
| verify 1M hot | aes-ctr | 5.57 | 5.55 | 5.57 | 175.3 |
| crc 1M hot | crc64nvme | 13.17 | 12.20 | 13.20 | 74.15 |
| copy 1M hot | memcpy | 28.75 | 28.41 | 29.71 | 33.97 |

### 3a. Making bytes on several cores, no wire, round 2

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | GiB/s, all | GiB/s, least core |
| --- | --- | --- | --- |
| fill+crc 1M cold x1 | aes-ctr | 3.21 | 3.21 |
| fill+crc 1M cold x1 | chacha8 | 2.29 | 2.29 |
| fill+crc 1M cold x1 | xoshiro | 2.94 | 2.94 |
| fill+crc 1M cold x1 | splitmix | 3.55 | 3.55 |
| fill+crc 1M cold x1 | stamped | 4.40 | 4.40 |
| fill+crc 1M cold x2 | aes-ctr | 5.98 | 2.99 |
| fill+crc 1M cold x2 | chacha8 | 4.37 | 2.17 |
| fill+crc 1M cold x2 | xoshiro | 5.60 | 2.78 |
| fill+crc 1M cold x2 | splitmix | 6.33 | 3.16 |
| fill+crc 1M cold x2 | stamped | 6.28 | 3.13 |
| fill+crc 1M cold x4 | aes-ctr | 8.37 | 2.04 |
| fill+crc 1M cold x4 | chacha8 | 6.77 | 1.67 |
| fill+crc 1M cold x4 | xoshiro | 8.47 | 2.09 |
| fill+crc 1M cold x4 | splitmix | 9.53 | 2.35 |
| fill+crc 1M cold x4 | stamped | 6.89 | 1.68 |

### 2. Against a server that discards, round 2

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | fill+crc aes-ctr | 1878 | 545.3 | 1.000 | 1.00 | 1878 | 292.8 | 0.527 | 850.7 | 0 | 0 |
| put 1M plain | fill aes-ctr | 2164 | 473.3 | 1.000 | 1.00 | 2164 | 292.2 | 0.608 | 778.0 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 1392 | 735.4 | 1.000 | 1.00 | 1392 | 332.5 | 0.442 | 1312 | 0 | 0 |
| put 1M plain | fill chacha8 | 1553 | 659.3 | 1.000 | 1.00 | 1553 | 321.5 | 0.478 | 993.0 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 1699 | 602.6 | 1.000 | 1.00 | 1699 | 304.9 | 0.497 | 926.6 | 0 | 0 |
| put 1M plain | fill xoshiro | 1964 | 521.3 | 1.00 | 1.00 | 1964 | 301.7 | 0.569 | 830.8 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 1821 | 562.1 | 1.000 | 1.00 | 1822 | 303.4 | 0.530 | 865.7 | 0 | 0 |
| put 1M plain | fill splitmix | 2098 | 488.1 | 1.000 | 1.00 | 2098 | 303.6 | 0.612 | 800.5 | 0 | 0 |
| put 1M plain | fill+crc stamped | 2200 | 465.4 | 1.000 | 1.00 | 2200 | 298.0 | 0.630 | 771.6 | 0 | 0 |
| put 1M plain | fill stamped | 2538 | 403.4 | 1.000 | 1.00 | 2539 | 308.3 | 0.754 | 723.7 | 0 | 0 |
| put 1M plain | pattern | 3585 | 285.7 | 1.00 | 1.00 | 3585 | 288.3 | 1.000 | 585.0 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 3328 | 288.6 | 0.938 | 0.957 | 3548 | 310.7 | 1.000 | 598.6 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 1694 | 556.8 | 0.921 | 0.938 | 1839 | 332.4 | 0.540 | 881.4 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 2549 | 369.3 | 0.919 | 0.940 | 2773 | 323.4 | 0.795 | 696.3 | 0 | 0 |
| get 1M plain | regenerate splitmix | 2567 | 366.4 | 0.918 | 0.940 | 2795 | 320.3 | 0.793 | 686.1 | 0 | 0 |
| get 1M plain | regenerate stamped | 3001 | 255.4 | 0.748 | 0.795 | 4010 | 344.8 | 1.000 | 605.8 | 0 | 0 |
| get 1M plain | crc | 3229 | 203.4 | 0.641 | 0.668 | 5035 | 320.0 | 1.000 | 499.1 | 0 | 0 |
| get 1M plain | none | 3276 | 179.0 | 0.572 | 0.589 | 5722 | 315.4 | 1.000 | 452.6 | 0 | 0 |
| put 1M ktls | fill+crc aes-ctr | 615.1 | 1015 | 0.609 | 0.612 | 1009 | 1194 | 0.707 | 2287 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 999.2 | 947.0 | 0.924 | 0.926 | 1081 | 928.2 | 0.896 | 1922 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 609.9 | 1192 | 0.710 | 0.713 | 858.8 | 1167 | 0.685 | 2401 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 714.5 | 1123 | 0.784 | 0.786 | 911.6 | 1038 | 0.715 | 2201 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 960.4 | 1065 | 0.999 | 1.00 | 961.7 | 940.2 | 0.872 | 2049 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 993.3 | 995.9 | 0.966 | 0.966 | 1028 | 905.9 | 0.869 | 1942 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 967.8 | 1057 | 0.999 | 1.00 | 968.8 | 938.4 | 0.877 | 2045 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 664.1 | 965.2 | 0.626 | 0.629 | 1061 | 1131 | 0.724 | 2146 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 1019 | 1002 | 0.997 | 0.998 | 1022 | 948.0 | 0.934 | 1961 | 0 | 1.00 |
| put 1M ktls | fill stamped | 626.1 | 935.3 | 0.572 | 0.588 | 1095 | 1172 | 0.706 | 2688 | 0 | 1.00 |
| put 1M ktls | pattern | 718.3 | 774.7 | 0.543 | 0.547 | 1322 | 1033 | 0.715 | 1879 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 882.6 | 1142 | 0.984 | 1.00 | 896.8 | 1167 | 0.996 | 2343 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 504.3 | 1352 | 0.666 | 0.683 | 757.6 | 945.6 | 0.456 | 2339 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 551.1 | 1219 | 0.656 | 0.669 | 840.2 | 898.5 | 0.473 | 2203 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 560.6 | 1197 | 0.655 | 0.671 | 855.3 | 892.4 | 0.478 | 2177 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 568.3 | 1174 | 0.652 | 0.664 | 872.2 | 885.3 | 0.481 | 2137 | 0 | 1.00 |
| get 1M ktls | crc | 640.5 | 993.6 | 0.622 | 0.636 | 1031 | 856.1 | 0.526 | 1889 | 0 | 1.00 |
| get 1M ktls | none | 667.1 | 935.1 | 0.609 | 0.627 | 1095 | 856.7 | 0.548 | 1833 | 0 | 1.00 |

### 3b. The put from several client cores, round 2

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain x1 | fill+crc aes-ctr | 1875 | 546.0 | 1.000 | 1.00 | 1876 | 292.5 | 0.526 | 855.2 | 0 | 0 |
| put 1M plain x1 | fill+crc chacha8 | 1412 | 724.8 | 1.000 | 1.00 | 1413 | 329.6 | 0.445 | 1077 | 0 | 0 |
| put 1M plain x1 | fill+crc xoshiro | 1709 | 599.0 | 1.00 | 1.00 | 1709 | 303.3 | 0.497 | 902.0 | 0 | 0 |
| put 1M plain x1 | fill+crc splitmix | 1813 | 564.7 | 1.00 | 1.00 | 1813 | 301.6 | 0.525 | 858.2 | 0 | 0 |
| put 1M plain x2 | fill+crc aes-ctr | 2954 | 693.3 | 1.000 | 1.00 | 1477 | 390.1 | 0.568 | 1092 | 0 | 0 |
| put 1M plain x2 | fill+crc chacha8 | 2542 | 805.6 | 1.000 | 1.00 | 1271 | 357.2 | 0.447 | 1188 | 0 | 0 |
| put 1M plain x2 | fill+crc xoshiro | 2892 | 708.1 | 1.000 | 1.00 | 1446 | 368.5 | 0.521 | 1092 | 0 | 0 |
| put 1M plain x2 | fill+crc splitmix | 3179 | 644.2 | 1.00 | 1.00 | 1589 | 348.4 | 0.551 | 997.1 | 0 | 0 |

### 4. A folder of real files, round 2

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg titan znver1

| cell | side | MiB/s | cpu ms/GiB | device MiB read | from device |
| --- | --- | --- | --- | --- | --- |
| 64K cold | read+sha256 | 312.7 | 1420 | 2048 | 1.00 |
| 64K cold | read+crc | 381.5 | 850.2 | 2048 | 1.00 |
| 64K cold | read | 393.1 | 772.9 | 2048 | 1.00 |
| 64K hot | read+sha256 | 1243 | 823.5 | 0 | 0 |
| 64K hot | read+crc | 3847 | 266.2 | 0 | 0 |
| 64K hot | read | 5375 | 190.5 | 0 | 0 |
| 1M cold | read+sha256 | 524.6 | 943.1 | 2048 | 1.00 |
| 1M cold | read+crc | 730.5 | 407.6 | 2048 | 1.00 |
| 1M cold | read | 767.2 | 333.0 | 2048 | 1.00 |
| 1M hot | read+sha256 | 1436 | 712.8 | 0 | 0 |
| 1M hot | read+crc | 5630 | 181.8 | 0 | 0 |
| 1M hot | read | 10011 | 102.3 | 0 | 0 |
| 64M cold | read+sha256 | 601.0 | 876.5 | 2048 | 1.00 |
| 64M cold | read+crc | 859.5 | 343.8 | 2048 | 1.00 |
| 64M cold | read | 860.5 | 269.7 | 2048 | 1.00 |
| 64M hot | read+sha256 | 1461 | 700.9 | 0 | 0 |
| 64M hot | read+crc | 5963 | 171.7 | 0 | 0 |
| 64M hot | read | 11145 | 91.87 | 0 | 0 |

