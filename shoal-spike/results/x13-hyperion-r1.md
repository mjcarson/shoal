### Checks, round 1

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
| describe fixed | expand | 1.00 | 3123607 |
| describe uniform | expand | 1.00 | 3572329 |
| describe doublings | expand | 1.00 | 4023020 |
| describe table | expand | 1.00 | 3310685 |

### 1. Making bytes on one core, GiB/s, round 1

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 5.18 | 5.15 | 5.18 | 0.737 |
| fill 4K cold | splitmix | 4.84 | 4.83 | 4.84 | 0.789 |
| fill 4K cold | xoshiro | 3.14 | 3.14 | 3.14 | 1.21 |
| fill 4K cold | chacha8 | 2.67 | 2.67 | 2.67 | 1.43 |
| fill 4K cold | aes-ctr | 4.69 | 4.63 | 4.69 | 0.814 |
| fill+crc 4K cold | stamped | 3.82 | 3.82 | 3.83 | 0.998 |
| fill+crc 4K cold | splitmix | 3.47 | 3.47 | 3.47 | 1.10 |
| fill+crc 4K cold | xoshiro | 2.87 | 2.86 | 2.88 | 1.33 |
| fill+crc 4K cold | chacha8 | 2.21 | 2.21 | 2.21 | 1.73 |
| fill+crc 4K cold | aes-ctr | 3.28 | 3.26 | 3.31 | 1.16 |
| verify 4K cold | stamped | 6.05 | 6.04 | 6.10 | 0.631 |
| verify 4K cold | splitmix | 3.76 | 3.76 | 3.76 | 1.01 |
| verify 4K cold | xoshiro | 3.40 | 3.40 | 3.40 | 1.12 |
| verify 4K cold | chacha8 | 2.28 | 2.27 | 2.28 | 1.68 |
| verify 4K cold | aes-ctr | 4.43 | 4.41 | 4.43 | 0.862 |
| crc 4K cold | crc64nvme | 11.49 | 11.35 | 11.50 | 0.332 |
| copy 4K cold | memcpy | 5.11 | 5.10 | 5.11 | 0.747 |
| fill 4K hot | stamped | 29.67 | 29.67 | 29.70 | 0.129 |
| fill 4K hot | splitmix | 4.90 | 4.90 | 4.91 | 0.778 |
| fill 4K hot | xoshiro | 4.55 | 4.55 | 4.55 | 0.838 |
| fill 4K hot | chacha8 | 2.67 | 2.66 | 2.67 | 1.43 |
| fill 4K hot | aes-ctr | 6.34 | 6.33 | 6.34 | 0.602 |
| fill+crc 4K hot | stamped | 8.88 | 8.88 | 8.88 | 0.429 |
| fill+crc 4K hot | splitmix | 3.54 | 3.54 | 3.54 | 1.08 |
| fill+crc 4K hot | xoshiro | 3.35 | 3.24 | 3.36 | 1.14 |
| fill+crc 4K hot | chacha8 | 2.21 | 2.20 | 2.21 | 1.73 |
| fill+crc 4K hot | aes-ctr | 4.24 | 4.23 | 4.24 | 0.899 |
| verify 4K hot | stamped | 17.75 | 17.74 | 17.75 | 0.215 |
| verify 4K hot | splitmix | 4.44 | 4.44 | 4.44 | 0.859 |
| verify 4K hot | xoshiro | 4.10 | 4.09 | 4.12 | 0.931 |
| verify 4K hot | chacha8 | 2.52 | 2.51 | 2.52 | 1.52 |
| verify 4K hot | aes-ctr | 5.56 | 5.56 | 5.56 | 0.686 |
| crc 4K hot | crc64nvme | 12.89 | 12.89 | 12.89 | 0.296 |
| copy 4K hot | memcpy | 29.44 | 29.43 | 29.45 | 0.130 |
| fill 64K cold | stamped | 6.28 | 6.18 | 6.36 | 9.72 |
| fill 64K cold | splitmix | 4.90 | 4.89 | 4.91 | 12.44 |
| fill 64K cold | xoshiro | 3.49 | 3.49 | 3.49 | 17.47 |
| fill 64K cold | chacha8 | 2.78 | 2.78 | 2.78 | 21.94 |
| fill 64K cold | aes-ctr | 3.96 | 3.96 | 3.96 | 15.42 |
| fill+crc 64K cold | stamped | 4.29 | 4.29 | 4.32 | 14.22 |
| fill+crc 64K cold | splitmix | 3.58 | 3.58 | 3.58 | 17.05 |
| fill+crc 64K cold | xoshiro | 2.81 | 2.81 | 2.81 | 21.72 |
| fill+crc 64K cold | chacha8 | 2.29 | 2.29 | 2.30 | 26.60 |
| fill+crc 64K cold | aes-ctr | 2.96 | 2.96 | 2.96 | 20.60 |
| verify 64K cold | stamped | 7.16 | 7.15 | 7.16 | 8.53 |
| verify 64K cold | splitmix | 3.74 | 3.74 | 3.74 | 16.32 |
| verify 64K cold | xoshiro | 3.80 | 3.80 | 3.81 | 16.05 |
| verify 64K cold | chacha8 | 2.33 | 2.33 | 2.34 | 26.17 |
| verify 64K cold | aes-ctr | 4.61 | 4.46 | 4.61 | 13.25 |
| crc 64K cold | crc64nvme | 11.50 | 11.40 | 11.51 | 5.31 |
| copy 64K cold | memcpy | 6.63 | 6.63 | 6.63 | 9.20 |
| fill 64K hot | stamped | 27.09 | 27.08 | 27.13 | 2.25 |
| fill 64K hot | splitmix | 4.98 | 4.98 | 4.98 | 12.26 |
| fill 64K hot | xoshiro | 4.95 | 4.95 | 4.98 | 12.33 |
| fill 64K hot | chacha8 | 2.76 | 2.76 | 2.76 | 22.14 |
| fill 64K hot | aes-ctr | 6.70 | 6.70 | 6.72 | 9.11 |
| fill+crc 64K hot | stamped | 8.87 | 8.87 | 8.88 | 6.88 |
| fill+crc 64K hot | splitmix | 3.60 | 3.60 | 3.61 | 16.94 |
| fill+crc 64K hot | xoshiro | 3.66 | 3.63 | 3.66 | 16.69 |
| fill+crc 64K hot | chacha8 | 2.28 | 2.28 | 2.28 | 26.75 |
| fill+crc 64K hot | aes-ctr | 4.44 | 4.44 | 4.46 | 13.74 |
| verify 64K hot | stamped | 15.15 | 15.14 | 15.15 | 4.03 |
| verify 64K hot | splitmix | 4.34 | 4.34 | 4.34 | 14.06 |
| verify 64K hot | xoshiro | 4.38 | 4.38 | 4.40 | 13.93 |
| verify 64K hot | chacha8 | 2.55 | 2.55 | 2.55 | 23.98 |
| verify 64K hot | aes-ctr | 5.59 | 5.58 | 5.60 | 10.92 |
| crc 64K hot | crc64nvme | 13.15 | 13.15 | 13.16 | 4.64 |
| copy 64K hot | memcpy | 30.28 | 30.15 | 30.28 | 2.02 |
| fill 1M cold | stamped | 6.35 | 6.25 | 6.42 | 153.9 |
| fill 1M cold | splitmix | 4.91 | 4.90 | 4.91 | 198.9 |
| fill 1M cold | xoshiro | 3.53 | 3.53 | 3.54 | 276.5 |
| fill 1M cold | chacha8 | 2.79 | 2.79 | 2.79 | 349.9 |
| fill 1M cold | aes-ctr | 4.27 | 4.27 | 4.28 | 228.5 |
| fill+crc 1M cold | stamped | 4.43 | 4.43 | 4.43 | 220.7 |
| fill+crc 1M cold | splitmix | 3.54 | 3.54 | 3.54 | 276.1 |
| fill+crc 1M cold | xoshiro | 2.91 | 2.91 | 2.91 | 336.0 |
| fill+crc 1M cold | chacha8 | 2.28 | 2.28 | 2.28 | 428.0 |
| fill+crc 1M cold | aes-ctr | 3.20 | 3.20 | 3.20 | 305.1 |
| verify 1M cold | stamped | 7.00 | 6.98 | 7.06 | 139.5 |
| verify 1M cold | splitmix | 3.68 | 3.68 | 3.69 | 265.1 |
| verify 1M cold | xoshiro | 3.72 | 3.72 | 3.72 | 262.5 |
| verify 1M cold | chacha8 | 2.32 | 2.32 | 2.32 | 420.3 |
| verify 1M cold | aes-ctr | 4.48 | 4.44 | 4.49 | 218.0 |
| crc 1M cold | crc64nvme | 11.53 | 11.41 | 11.53 | 84.71 |
| copy 1M cold | memcpy | 6.46 | 6.46 | 6.46 | 151.2 |
| fill 1M hot | stamped | 27.03 | 27.01 | 27.06 | 36.13 |
| fill 1M hot | splitmix | 4.99 | 4.99 | 4.99 | 195.7 |
| fill 1M hot | xoshiro | 5.00 | 5.00 | 5.00 | 195.2 |
| fill 1M hot | chacha8 | 2.78 | 2.78 | 2.78 | 351.4 |
| fill 1M hot | aes-ctr | 6.68 | 6.67 | 6.70 | 146.1 |
| fill+crc 1M hot | stamped | 8.88 | 8.88 | 8.89 | 109.9 |
| fill+crc 1M hot | splitmix | 3.63 | 3.63 | 3.63 | 269.1 |
| fill+crc 1M hot | xoshiro | 3.66 | 3.66 | 3.66 | 266.5 |
| fill+crc 1M hot | chacha8 | 2.29 | 2.29 | 2.29 | 425.6 |
| fill+crc 1M hot | aes-ctr | 4.46 | 4.46 | 4.46 | 219.0 |
| verify 1M hot | stamped | 11.95 | 11.95 | 11.97 | 81.70 |
| verify 1M hot | splitmix | 4.33 | 4.33 | 4.33 | 225.4 |
| verify 1M hot | xoshiro | 4.39 | 4.39 | 4.39 | 222.5 |
| verify 1M hot | chacha8 | 2.56 | 2.56 | 2.56 | 381.8 |
| verify 1M hot | aes-ctr | 5.59 | 5.59 | 5.59 | 174.7 |
| crc 1M hot | crc64nvme | 13.21 | 13.20 | 13.22 | 73.93 |
| copy 1M hot | memcpy | 29.92 | 29.90 | 29.93 | 32.64 |

### 3a. Making bytes on several cores, no wire, round 1

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | GiB/s, all | GiB/s, least core |
| --- | --- | --- | --- |
| fill+crc 1M cold x1 | stamped | 4.36 | 4.36 |
| fill+crc 1M cold x1 | splitmix | 3.54 | 3.54 |
| fill+crc 1M cold x1 | xoshiro | 2.94 | 2.94 |
| fill+crc 1M cold x1 | chacha8 | 2.28 | 2.28 |
| fill+crc 1M cold x1 | aes-ctr | 3.23 | 3.23 |
| fill+crc 1M cold x2 | stamped | 6.19 | 3.10 |
| fill+crc 1M cold x2 | splitmix | 6.32 | 3.16 |
| fill+crc 1M cold x2 | xoshiro | 5.54 | 2.74 |
| fill+crc 1M cold x2 | chacha8 | 4.37 | 2.17 |
| fill+crc 1M cold x2 | aes-ctr | 5.96 | 2.98 |
| fill+crc 1M cold x4 | stamped | 7.01 | 1.72 |
| fill+crc 1M cold x4 | splitmix | 9.28 | 2.28 |
| fill+crc 1M cold x4 | xoshiro | 8.29 | 2.05 |
| fill+crc 1M cold x4 | chacha8 | 6.70 | 1.66 |
| fill+crc 1M cold x4 | aes-ctr | 8.00 | 1.93 |

### 2. Against a server that discards, round 1

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | pattern | 3506 | 292.1 | 1.000 | 1.00 | 3506 | 294.7 | 1.000 | 594.0 | 0 | 0 |
| put 1M plain | fill stamped | 2468 | 414.9 | 1.000 | 1.00 | 2468 | 320.7 | 0.763 | 752.5 | 0 | 0 |
| put 1M plain | fill+crc stamped | 2166 | 472.9 | 1.00 | 1.00 | 2165 | 303.1 | 0.632 | 774.5 | 0 | 0 |
| put 1M plain | fill splitmix | 2067 | 495.4 | 1.00 | 1.00 | 2067 | 311.2 | 0.619 | 810.3 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 1789 | 572.4 | 1.000 | 1.00 | 1789 | 303.7 | 0.522 | 890.5 | 0 | 0 |
| put 1M plain | fill xoshiro | 1928 | 531.2 | 1.00 | 1.00 | 1928 | 308.3 | 0.572 | 853.0 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 1680 | 609.6 | 1.000 | 1.00 | 1680 | 283.9 | 0.457 | 893.7 | 0 | 0 |
| put 1M plain | fill chacha8 | 1533 | 668.0 | 1.000 | 1.00 | 1533 | 329.4 | 0.484 | 996.7 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 1396 | 733.7 | 1.000 | 1.00 | 1396 | 338.4 | 0.452 | 1078 | 0 | 0 |
| put 1M plain | fill aes-ctr | 2114 | 484.5 | 1.000 | 1.00 | 2114 | 304.2 | 0.619 | 804.2 | 0 | 0 |
| put 1M plain | fill+crc aes-ctr | 1824 | 561.5 | 1.00 | 1.00 | 1824 | 305.2 | 0.535 | 876.9 | 0 | 0 |
| get 1M plain | none | 3223 | 181.6 | 0.572 | 0.595 | 5638 | 320.5 | 1.000 | 459.9 | 0 | 0 |
| get 1M plain | crc | 3141 | 207.4 | 0.636 | 0.665 | 4938 | 328.9 | 1.000 | 509.8 | 0 | 0 |
| get 1M plain | regenerate stamped | 2906 | 265.3 | 0.753 | 0.792 | 3859 | 355.7 | 1.000 | 618.6 | 0 | 0 |
| get 1M plain | regenerate splitmix | 2559 | 368.2 | 0.920 | 0.942 | 2781 | 327.7 | 0.810 | 700.1 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 2570 | 366.6 | 0.920 | 0.942 | 2794 | 328.8 | 0.816 | 687.7 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 1690 | 558.3 | 0.921 | 0.939 | 1834 | 338.9 | 0.550 | 890.8 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 3251 | 294.3 | 0.934 | 0.952 | 3480 | 317.8 | 1.000 | 608.3 | 0 | 0 |
| put 1M ktls | pattern | 717.9 | 779.3 | 0.546 | 0.550 | 1314 | 1038 | 0.718 | 1874 | 0 | 1.00 |
| put 1M ktls | fill stamped | 1085 | 931.0 | 0.986 | 0.988 | 1100 | 918.3 | 0.964 | 1880 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 633.1 | 977.2 | 0.604 | 0.608 | 1048 | 1163 | 0.710 | 2173 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 767.7 | 966.3 | 0.724 | 0.729 | 1060 | 1006 | 0.745 | 1977 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 625.1 | 1032 | 0.630 | 0.633 | 991.9 | 1143 | 0.689 | 2247 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 627.7 | 975.1 | 0.598 | 0.600 | 1050 | 1161 | 0.703 | 2192 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 961.1 | 1064 | 0.999 | 0.998 | 962.1 | 937.1 | 0.870 | 2022 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 733.7 | 1122 | 0.804 | 0.808 | 912.5 | 1012 | 0.716 | 2171 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 621.3 | 1191 | 0.723 | 0.727 | 859.4 | 1141 | 0.683 | 2356 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 618.7 | 944.2 | 0.570 | 0.572 | 1085 | 1159 | 0.691 | 2181 | 0 | 1.00 |
| put 1M ktls | fill+crc aes-ctr | 999.6 | 1023 | 0.998 | 1.00 | 1001 | 942.0 | 0.910 | 2003 | 0 | 1.00 |
| get 1M ktls | none | 677.7 | 934.0 | 0.618 | 0.634 | 1096 | 833.6 | 0.543 | 1777 | 0 | 1.00 |
| get 1M ktls | crc | 649.3 | 994.2 | 0.630 | 0.646 | 1030 | 831.6 | 0.518 | 1858 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 569.5 | 1177 | 0.655 | 0.669 | 870.0 | 880.9 | 0.480 | 2139 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 559.3 | 1199 | 0.655 | 0.669 | 854.0 | 893.2 | 0.479 | 2156 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 649.3 | 1242 | 0.787 | 0.807 | 824.8 | 905.3 | 0.565 | 2208 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 506.9 | 1353 | 0.670 | 0.685 | 756.7 | 931.9 | 0.452 | 2311 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 564.9 | 1194 | 0.659 | 0.671 | 857.7 | 874.7 | 0.473 | 2095 | 0 | 1.00 |

### 3b. The put from several client cores, round 1

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain x1 | fill+crc splitmix | 1789 | 572.4 | 1.000 | 1.00 | 1789 | 306.3 | 0.526 | 888.4 | 0 | 0 |
| put 1M plain x1 | fill+crc xoshiro | 1697 | 603.4 | 1.000 | 1.00 | 1697 | 292.4 | 0.476 | 907.3 | 0 | 0 |
| put 1M plain x1 | fill+crc chacha8 | 1403 | 729.9 | 1.00 | 1.00 | 1403 | 337.8 | 0.454 | 1061 | 0 | 0 |
| put 1M plain x1 | fill+crc aes-ctr | 1840 | 556.5 | 1.00 | 1.00 | 1840 | 300.8 | 0.532 | 859.1 | 0 | 0 |
| put 1M plain x2 | fill+crc splitmix | 3071 | 666.6 | 1.000 | 1.00 | 1536 | 375.4 | 0.563 | 1053 | 0 | 0 |
| put 1M plain x2 | fill+crc xoshiro | 2847 | 719.0 | 0.999 | 1.00 | 1424 | 390.1 | 0.545 | 1116 | 0 | 0 |
| put 1M plain x2 | fill+crc chacha8 | 2492 | 821.6 | 1.000 | 1.00 | 1246 | 376.7 | 0.464 | 1206 | 0 | 0 |
| put 1M plain x2 | fill+crc aes-ctr | 2879 | 711.4 | 1.00 | 1.00 | 1439 | 392.5 | 0.563 | 1102 | 0 | 0 |

### 4. A folder of real files, round 1

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · build znver1 (avx2 aes sha) · crc x86-sse-pclmulqdq · leg hyperion znver1

| cell | side | MiB/s | cpu ms/GiB | device MiB read | from device |
| --- | --- | --- | --- | --- | --- |
| 64K cold | read | 388.2 | 838.7 | 2048 | 1.00 |
| 64K cold | read+crc | 384.5 | 853.6 | 2048 | 1.00 |
| 64K cold | read+sha256 | 315.1 | 1423 | 2048 | 1.00 |
| 64K hot | read | 5385 | 190.1 | 0 | 0 |
| 64K hot | read+crc | 3861 | 265.2 | 0 | 0 |
| 64K hot | read+sha256 | 1249 | 819.9 | 0 | 0 |
| 1M cold | read | 771.6 | 341.3 | 2048 | 1.00 |
| 1M cold | read+crc | 728.5 | 413.4 | 2048 | 1.00 |
| 1M cold | read+sha256 | 527.0 | 945.8 | 2048 | 1.00 |
| 1M hot | read | 9515 | 107.6 | 0 | 0 |
| 1M hot | read+crc | 5462 | 187.5 | 0 | 0 |
| 1M hot | read+sha256 | 1425 | 718.7 | 0 | 0 |
| 64M cold | read | 861.2 | 278.0 | 2048 | 1.00 |
| 64M cold | read+crc | 859.9 | 349.6 | 2048 | 1.00 |
| 64M cold | read+sha256 | 600.4 | 882.9 | 2048 | 1.00 |
| 64M hot | read | 10427 | 98.20 | 0 | 0 |
| 64M hot | read+crc | 5714 | 179.2 | 0 | 0 |
| 64M hot | read+sha256 | 1447 | 707.8 | 0 | 0 |

