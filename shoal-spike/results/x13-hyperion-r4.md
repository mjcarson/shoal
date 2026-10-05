### Checks, round 4

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
| describe fixed | expand | 1.00 | 3062386 |
| describe uniform | expand | 1.00 | 3566809 |
| describe doublings | expand | 1.00 | 4030288 |
| describe table | expand | 1.00 | 3249196 |

### 1. Making bytes on one core, GiB/s, round 4

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 5.19 | 5.18 | 5.19 | 0.735 |
| fill 4K cold | splitmix | 4.82 | 4.81 | 4.82 | 0.791 |
| fill 4K cold | xoshiro | 3.15 | 3.15 | 3.15 | 1.21 |
| fill 4K cold | chacha8 | 2.67 | 2.67 | 2.67 | 1.43 |
| fill 4K cold | aes-ctr | 4.68 | 4.67 | 4.68 | 0.816 |
| fill+crc 4K cold | stamped | 3.84 | 3.84 | 3.84 | 0.994 |
| fill+crc 4K cold | splitmix | 3.43 | 3.43 | 3.44 | 1.11 |
| fill+crc 4K cold | xoshiro | 2.85 | 2.85 | 2.86 | 1.34 |
| fill+crc 4K cold | chacha8 | 2.21 | 2.21 | 2.21 | 1.73 |
| fill+crc 4K cold | aes-ctr | 3.22 | 3.21 | 3.29 | 1.18 |
| verify 4K cold | stamped | 6.06 | 6.05 | 6.06 | 0.630 |
| verify 4K cold | splitmix | 3.75 | 3.75 | 3.75 | 1.02 |
| verify 4K cold | xoshiro | 3.40 | 3.40 | 3.40 | 1.12 |
| verify 4K cold | chacha8 | 2.28 | 2.27 | 2.28 | 1.67 |
| verify 4K cold | aes-ctr | 4.44 | 4.44 | 4.44 | 0.860 |
| crc 4K cold | crc64nvme | 11.48 | 11.45 | 11.50 | 0.332 |
| copy 4K cold | memcpy | 5.12 | 5.02 | 5.12 | 0.745 |
| fill 4K hot | stamped | 29.66 | 29.64 | 29.67 | 0.129 |
| fill 4K hot | splitmix | 4.90 | 4.90 | 4.91 | 0.779 |
| fill 4K hot | xoshiro | 4.55 | 4.55 | 4.55 | 0.838 |
| fill 4K hot | chacha8 | 2.67 | 2.66 | 2.67 | 1.43 |
| fill 4K hot | aes-ctr | 6.34 | 6.34 | 6.34 | 0.601 |
| fill+crc 4K hot | stamped | 8.88 | 8.88 | 8.88 | 0.430 |
| fill+crc 4K hot | splitmix | 3.54 | 3.54 | 3.55 | 1.08 |
| fill+crc 4K hot | xoshiro | 3.34 | 3.34 | 3.35 | 1.14 |
| fill+crc 4K hot | chacha8 | 2.21 | 2.20 | 2.21 | 1.73 |
| fill+crc 4K hot | aes-ctr | 4.24 | 4.24 | 4.24 | 0.899 |
| verify 4K hot | stamped | 17.74 | 17.74 | 17.74 | 0.215 |
| verify 4K hot | splitmix | 4.44 | 4.44 | 4.45 | 0.859 |
| verify 4K hot | xoshiro | 4.11 | 4.09 | 4.13 | 0.927 |
| verify 4K hot | chacha8 | 2.52 | 2.52 | 2.52 | 1.52 |
| verify 4K hot | aes-ctr | 5.56 | 5.56 | 5.57 | 0.686 |
| crc 4K hot | crc64nvme | 12.88 | 12.88 | 12.89 | 0.296 |
| copy 4K hot | memcpy | 38.39 | 38.37 | 38.40 | 0.099 |
| fill 64K cold | stamped | 6.38 | 6.38 | 6.38 | 9.56 |
| fill 64K cold | splitmix | 4.89 | 4.89 | 4.89 | 12.48 |
| fill 64K cold | xoshiro | 3.43 | 3.41 | 3.43 | 17.81 |
| fill 64K cold | chacha8 | 2.78 | 2.78 | 2.78 | 21.97 |
| fill 64K cold | aes-ctr | 3.95 | 3.81 | 3.95 | 15.43 |
| fill+crc 64K cold | stamped | 4.34 | 4.33 | 4.34 | 14.08 |
| fill+crc 64K cold | splitmix | 3.55 | 3.55 | 3.56 | 17.17 |
| fill+crc 64K cold | xoshiro | 2.86 | 2.82 | 2.88 | 21.34 |
| fill+crc 64K cold | chacha8 | 2.29 | 2.27 | 2.29 | 26.60 |
| fill+crc 64K cold | aes-ctr | 2.98 | 2.98 | 2.98 | 20.48 |
| verify 64K cold | stamped | 7.16 | 7.16 | 7.17 | 8.52 |
| verify 64K cold | splitmix | 3.75 | 3.75 | 3.75 | 16.27 |
| verify 64K cold | xoshiro | 3.81 | 3.80 | 3.81 | 16.03 |
| verify 64K cold | chacha8 | 2.34 | 2.33 | 2.34 | 26.13 |
| verify 64K cold | aes-ctr | 4.60 | 4.59 | 4.60 | 13.27 |
| crc 64K cold | crc64nvme | 11.50 | 11.32 | 11.51 | 5.31 |
| copy 64K cold | memcpy | 6.68 | 6.68 | 6.68 | 9.14 |
| fill 64K hot | stamped | 27.09 | 27.08 | 27.09 | 2.25 |
| fill 64K hot | splitmix | 4.98 | 4.97 | 4.98 | 12.27 |
| fill 64K hot | xoshiro | 4.96 | 4.96 | 4.96 | 12.31 |
| fill 64K hot | chacha8 | 2.76 | 2.76 | 2.76 | 22.13 |
| fill 64K hot | aes-ctr | 6.75 | 6.74 | 6.76 | 9.04 |
| fill+crc 64K hot | stamped | 8.88 | 8.87 | 8.88 | 6.88 |
| fill+crc 64K hot | splitmix | 3.61 | 3.60 | 3.62 | 16.92 |
| fill+crc 64K hot | xoshiro | 3.65 | 3.65 | 3.65 | 16.72 |
| fill+crc 64K hot | chacha8 | 2.28 | 2.28 | 2.29 | 26.73 |
| fill+crc 64K hot | aes-ctr | 4.46 | 4.45 | 4.50 | 13.69 |
| verify 64K hot | stamped | 15.16 | 15.16 | 15.18 | 4.03 |
| verify 64K hot | splitmix | 4.34 | 4.34 | 4.35 | 14.05 |
| verify 64K hot | xoshiro | 4.39 | 4.38 | 4.39 | 13.91 |
| verify 64K hot | chacha8 | 2.55 | 2.55 | 2.55 | 23.97 |
| verify 64K hot | aes-ctr | 5.57 | 5.56 | 5.57 | 10.96 |
| crc 64K hot | crc64nvme | 13.16 | 13.16 | 13.16 | 4.64 |
| copy 64K hot | memcpy | 30.53 | 30.32 | 30.53 | 2.00 |
| fill 1M cold | stamped | 6.46 | 6.46 | 6.47 | 151.1 |
| fill 1M cold | splitmix | 4.90 | 4.90 | 4.91 | 199.1 |
| fill 1M cold | xoshiro | 3.45 | 3.45 | 3.46 | 282.9 |
| fill 1M cold | chacha8 | 2.79 | 2.79 | 2.79 | 350.4 |
| fill 1M cold | aes-ctr | 4.29 | 4.19 | 4.29 | 227.6 |
| fill+crc 1M cold | stamped | 4.46 | 4.46 | 4.46 | 218.9 |
| fill+crc 1M cold | splitmix | 3.54 | 3.53 | 3.54 | 276.2 |
| fill+crc 1M cold | xoshiro | 3.02 | 2.98 | 3.03 | 323.6 |
| fill+crc 1M cold | chacha8 | 2.28 | 2.21 | 2.28 | 427.5 |
| fill+crc 1M cold | aes-ctr | 3.24 | 3.24 | 3.24 | 301.5 |
| verify 1M cold | stamped | 6.99 | 6.99 | 6.99 | 139.7 |
| verify 1M cold | splitmix | 3.68 | 3.68 | 3.69 | 265.2 |
| verify 1M cold | xoshiro | 3.72 | 3.72 | 3.72 | 262.6 |
| verify 1M cold | chacha8 | 2.32 | 2.32 | 2.32 | 420.5 |
| verify 1M cold | aes-ctr | 4.49 | 4.49 | 4.49 | 217.5 |
| crc 1M cold | crc64nvme | 11.46 | 11.34 | 11.51 | 85.25 |
| copy 1M cold | memcpy | 6.53 | 6.53 | 6.54 | 149.5 |
| fill 1M hot | stamped | 27.12 | 27.11 | 27.13 | 36.01 |
| fill 1M hot | splitmix | 4.99 | 4.99 | 4.99 | 195.8 |
| fill 1M hot | xoshiro | 4.97 | 4.97 | 4.97 | 196.7 |
| fill 1M hot | chacha8 | 2.77 | 2.77 | 2.77 | 352.3 |
| fill 1M hot | aes-ctr | 6.71 | 6.71 | 6.72 | 145.6 |
| fill+crc 1M hot | stamped | 8.88 | 8.88 | 8.89 | 109.9 |
| fill+crc 1M hot | splitmix | 3.62 | 3.62 | 3.63 | 269.5 |
| fill+crc 1M hot | xoshiro | 3.65 | 3.65 | 3.66 | 267.3 |
| fill+crc 1M hot | chacha8 | 2.29 | 2.29 | 2.29 | 426.6 |
| fill+crc 1M hot | aes-ctr | 4.46 | 4.46 | 4.47 | 218.9 |
| verify 1M hot | stamped | 12.53 | 12.34 | 12.53 | 77.93 |
| verify 1M hot | splitmix | 4.33 | 4.31 | 4.33 | 225.7 |
| verify 1M hot | xoshiro | 4.38 | 4.38 | 4.39 | 223.0 |
| verify 1M hot | chacha8 | 2.56 | 2.55 | 2.56 | 382.2 |
| verify 1M hot | aes-ctr | 5.57 | 5.56 | 5.57 | 175.3 |
| crc 1M hot | crc64nvme | 13.18 | 13.17 | 13.20 | 74.10 |
| copy 1M hot | memcpy | 29.70 | 29.69 | 29.73 | 32.88 |

### 3a. Making bytes on several cores, no wire, round 4

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | GiB/s, all | GiB/s, least core |
| --- | --- | --- | --- |
| fill+crc 1M cold x1 | aes-ctr | 3.22 | 3.22 |
| fill+crc 1M cold x1 | chacha8 | 2.28 | 2.28 |
| fill+crc 1M cold x1 | xoshiro | 2.94 | 2.94 |
| fill+crc 1M cold x1 | splitmix | 3.54 | 3.54 |
| fill+crc 1M cold x1 | stamped | 4.38 | 4.38 |
| fill+crc 1M cold x2 | aes-ctr | 5.98 | 2.99 |
| fill+crc 1M cold x2 | chacha8 | 4.38 | 2.18 |
| fill+crc 1M cold x2 | xoshiro | 5.55 | 2.74 |
| fill+crc 1M cold x2 | splitmix | 6.35 | 3.17 |
| fill+crc 1M cold x2 | stamped | 6.44 | 3.22 |
| fill+crc 1M cold x4 | aes-ctr | 8.13 | 2.00 |
| fill+crc 1M cold x4 | chacha8 | 6.74 | 1.67 |
| fill+crc 1M cold x4 | xoshiro | 8.34 | 2.03 |
| fill+crc 1M cold x4 | splitmix | 9.40 | 2.28 |
| fill+crc 1M cold x4 | stamped | 6.71 | 1.65 |

### 2. Against a server that discards, round 4

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | fill+crc aes-ctr | 1837 | 557.1 | 0.999 | 0.998 | 1838 | 301.5 | 0.532 | 872.8 | 0 | 0 |
| put 1M plain | fill aes-ctr | 2126 | 481.6 | 1.000 | 1.00 | 2126 | 285.4 | 0.583 | 768.8 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 1405 | 728.5 | 1.000 | 1.00 | 1406 | 337.8 | 0.455 | 1065 | 0 | 0 |
| put 1M plain | fill chacha8 | 1534 | 667.5 | 1.000 | 1.00 | 1534 | 327.8 | 0.482 | 1009 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 1683 | 608.3 | 1.000 | 1.00 | 1683 | 294.1 | 0.474 | 916.0 | 0 | 0 |
| put 1M plain | fill xoshiro | 1956 | 523.5 | 1.000 | 1.00 | 1956 | 283.0 | 0.532 | 807.1 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 1788 | 572.7 | 1.000 | 1.00 | 1788 | 308.8 | 0.530 | 879.8 | 0 | 0 |
| put 1M plain | fill splitmix | 2076 | 493.2 | 1.000 | 1.00 | 2076 | 310.6 | 0.621 | 803.9 | 0 | 0 |
| put 1M plain | fill+crc stamped | 2148 | 476.7 | 1.000 | 1.00 | 2148 | 301.2 | 0.623 | 784.5 | 0 | 0 |
| put 1M plain | fill stamped | 2473 | 414.1 | 1.000 | 1.00 | 2473 | 318.0 | 0.758 | 755.3 | 0 | 0 |
| put 1M plain | pattern | 3524 | 290.6 | 1.00 | 1.00 | 3524 | 293.2 | 1.000 | 585.2 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 3263 | 290.8 | 0.927 | 0.946 | 3522 | 316.7 | 1.000 | 605.5 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 1686 | 558.6 | 0.920 | 0.936 | 1833 | 336.8 | 0.545 | 902.3 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 2552 | 368.5 | 0.919 | 0.939 | 2779 | 326.3 | 0.804 | 698.0 | 0 | 0 |
| get 1M plain | regenerate splitmix | 2558 | 367.8 | 0.919 | 0.940 | 2784 | 325.2 | 0.803 | 692.9 | 0 | 0 |
| get 1M plain | regenerate stamped | 2981 | 255.9 | 0.745 | 0.784 | 4002 | 346.8 | 1.000 | 591.4 | 0 | 0 |
| get 1M plain | crc | 3180 | 204.9 | 0.636 | 0.666 | 4997 | 324.9 | 1.000 | 505.5 | 0 | 0 |
| get 1M plain | none | 3206 | 182.0 | 0.570 | 0.586 | 5627 | 322.2 | 1.000 | 464.9 | 0 | 0 |
| put 1M ktls | fill+crc aes-ctr | 716.1 | 1007 | 0.704 | 0.707 | 1017 | 1054 | 0.728 | 2116 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 616.9 | 946.9 | 0.570 | 0.572 | 1081 | 1146 | 0.681 | 2121 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 542.7 | 1186 | 0.629 | 0.630 | 863.2 | 1310 | 0.685 | 2581 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 891.0 | 1143 | 0.994 | 0.994 | 896.0 | 914.0 | 0.786 | 2100 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 605.3 | 1057 | 0.625 | 0.629 | 968.7 | 1223 | 0.713 | 2354 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 1031 | 993.0 | 1.000 | 1.00 | 1031 | 896.6 | 0.893 | 1920 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 638.7 | 1033 | 0.644 | 0.648 | 991.6 | 1147 | 0.706 | 2222 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 1036 | 985.8 | 0.998 | 0.998 | 1039 | 935.9 | 0.938 | 1934 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 627.1 | 983.6 | 0.602 | 0.605 | 1041 | 1176 | 0.711 | 2224 | 0 | 1.00 |
| put 1M ktls | fill stamped | 945.6 | 932.2 | 0.861 | 0.864 | 1098 | 936.0 | 0.855 | 1916 | 0 | 1.00 |
| put 1M ktls | pattern | 941.6 | 801.5 | 0.737 | 0.739 | 1278 | 897.6 | 0.816 | 1705 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 870.0 | 1154 | 0.980 | 1.00 | 887.5 | 1183 | 0.996 | 2367 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 506.7 | 1351 | 0.669 | 0.682 | 757.9 | 935.4 | 0.454 | 2303 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 553.5 | 1220 | 0.659 | 0.673 | 839.5 | 893.5 | 0.474 | 2183 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 561.3 | 1196 | 0.655 | 0.671 | 856.5 | 892.5 | 0.480 | 2156 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 569.5 | 1172 | 0.652 | 0.665 | 873.5 | 886.6 | 0.484 | 2085 | 0 | 1.00 |
| get 1M ktls | crc | 648.5 | 994.8 | 0.630 | 0.645 | 1029 | 841.0 | 0.523 | 1869 | 0 | 1.00 |
| get 1M ktls | none | 675.5 | 936.0 | 0.617 | 0.633 | 1094 | 836.6 | 0.543 | 1816 | 0 | 1.00 |

### 3b. The put from several client cores, round 4

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain x1 | fill+crc aes-ctr | 1859 | 550.4 | 0.999 | 1.00 | 1860 | 299.3 | 0.534 | 863.5 | 0 | 0 |
| put 1M plain x1 | fill+crc chacha8 | 1410 | 726.4 | 1.000 | 1.00 | 1410 | 336.3 | 0.454 | 1065 | 0 | 0 |
| put 1M plain x1 | fill+crc xoshiro | 1698 | 603.1 | 1.000 | 1.00 | 1698 | 306.7 | 0.500 | 907.0 | 0 | 0 |
| put 1M plain x1 | fill+crc splitmix | 1792 | 571.6 | 1.000 | 1.00 | 1792 | 306.6 | 0.527 | 876.7 | 0 | 0 |
| put 1M plain x2 | fill+crc aes-ctr | 2903 | 705.1 | 0.999 | 0.999 | 1452 | 399.6 | 0.570 | 1115 | 0 | 0 |
| put 1M plain x2 | fill+crc chacha8 | 2502 | 818.3 | 1.000 | 1.00 | 1251 | 371.1 | 0.459 | 1199 | 0 | 0 |
| put 1M plain x2 | fill+crc xoshiro | 2891 | 708.4 | 1.000 | 1.00 | 1446 | 362.9 | 0.524 | 1069 | 0 | 0 |
| put 1M plain x2 | fill+crc splitmix | 3161 | 648.0 | 1.00 | 1.00 | 1580 | 351.1 | 0.543 | 1002 | 0 | 0 |

### 4. A folder of real files, round 4

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | MiB/s | cpu ms/GiB | device MiB read | from device |
| --- | --- | --- | --- | --- | --- |
| 64K cold | read+sha256 | 306.2 | 1423 | 2048 | 1.00 |
| 64K cold | read+crc | 369.4 | 850.8 | 2048 | 1.00 |
| 64K cold | read | 380.8 | 775.4 | 2048 | 1.00 |
| 64K hot | read+sha256 | 1240 | 825.7 | 0 | 0 |
| 64K hot | read+crc | 3788 | 270.3 | 0 | 0 |
| 64K hot | read | 5250 | 195.0 | 0 | 0 |
| 1M cold | read+sha256 | 524.4 | 944.3 | 2048 | 1.00 |
| 1M cold | read+crc | 727.1 | 407.3 | 2048 | 1.00 |
| 1M cold | read | 769.0 | 331.2 | 2048 | 1.00 |
| 1M hot | read+sha256 | 1429 | 716.5 | 0 | 0 |
| 1M hot | read+crc | 5559 | 184.2 | 0 | 0 |
| 1M hot | read | 9882 | 103.6 | 0 | 0 |
| 64M cold | read+sha256 | 598.7 | 878.0 | 2048 | 1.00 |
| 64M cold | read+crc | 859.4 | 345.5 | 2048 | 1.00 |
| 64M cold | read | 861.0 | 269.6 | 2048 | 1.00 |
| 64M hot | read+sha256 | 1451 | 705.6 | 0 | 0 |
| 64M hot | read+crc | 5857 | 174.8 | 0 | 0 |
| 64M hot | read | 10899 | 93.95 | 0 | 0 |

