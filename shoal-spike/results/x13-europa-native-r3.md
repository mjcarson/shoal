### Checks, round 3

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

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
| describe fixed | expand | 1.00 | 7441977 |
| describe uniform | expand | 1.00 | 8754897 |
| describe doublings | expand | 1.00 | 10403750 |
| describe table | expand | 1.00 | 5667672 |

### 1. Making bytes on one core, GiB/s, round 3

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 10.99 | 10.56 | 11.00 | 0.347 |
| fill 4K cold | splitmix | 22.88 | 21.32 | 22.97 | 0.167 |
| fill 4K cold | xoshiro | 15.25 | 15.17 | 15.30 | 0.250 |
| fill 4K cold | chacha8 | 6.77 | 6.75 | 6.77 | 0.564 |
| fill 4K cold | aes-ctr | 10.27 | 10.26 | 10.30 | 0.371 |
| fill+crc 4K cold | stamped | 13.66 | 13.65 | 13.67 | 0.279 |
| fill+crc 4K cold | splitmix | 22.56 | 22.52 | 22.62 | 0.169 |
| fill+crc 4K cold | xoshiro | 12.93 | 12.93 | 12.96 | 0.295 |
| fill+crc 4K cold | chacha8 | 6.24 | 6.24 | 6.25 | 0.611 |
| fill+crc 4K cold | aes-ctr | 7.78 | 7.74 | 7.82 | 0.491 |
| verify 4K cold | stamped | 17.00 | 16.97 | 17.04 | 0.224 |
| verify 4K cold | splitmix | 31.88 | 31.86 | 31.96 | 0.120 |
| verify 4K cold | xoshiro | 14.35 | 14.28 | 14.36 | 0.266 |
| verify 4K cold | chacha8 | 6.47 | 6.45 | 6.47 | 0.590 |
| verify 4K cold | aes-ctr | 9.69 | 9.66 | 9.69 | 0.394 |
| crc 4K cold | crc64nvme | 39.64 | 39.60 | 39.66 | 0.096 |
| copy 4K cold | memcpy | 6.38 | 6.37 | 6.38 | 0.598 |
| fill 4K hot | stamped | 86.68 | 86.02 | 87.14 | 0.044 |
| fill 4K hot | splitmix | 59.38 | 59.37 | 59.39 | 0.064 |
| fill 4K hot | xoshiro | 20.72 | 20.71 | 20.72 | 0.184 |
| fill 4K hot | chacha8 | 6.77 | 6.76 | 6.77 | 0.564 |
| fill 4K hot | aes-ctr | 10.86 | 10.84 | 10.86 | 0.351 |
| fill+crc 4K hot | stamped | 38.88 | 38.82 | 39.08 | 0.098 |
| fill+crc 4K hot | splitmix | 32.49 | 32.49 | 32.53 | 0.117 |
| fill+crc 4K hot | xoshiro | 15.80 | 15.80 | 15.81 | 0.241 |
| fill+crc 4K hot | chacha8 | 6.30 | 6.30 | 6.31 | 0.606 |
| fill+crc 4K hot | aes-ctr | 9.38 | 9.37 | 9.38 | 0.407 |
| verify 4K hot | stamped | 53.78 | 53.74 | 53.90 | 0.071 |
| verify 4K hot | splitmix | 42.71 | 42.71 | 42.73 | 0.089 |
| verify 4K hot | xoshiro | 18.16 | 18.15 | 18.16 | 0.210 |
| verify 4K hot | chacha8 | 6.61 | 6.61 | 6.61 | 0.577 |
| verify 4K hot | aes-ctr | 10.15 | 10.14 | 10.16 | 0.376 |
| crc 4K hot | crc64nvme | 68.09 | 68.07 | 68.09 | 0.056 |
| copy 4K hot | memcpy | 86.14 | 84.70 | 87.22 | 0.044 |
| fill 64K cold | stamped | 12.22 | 12.14 | 12.26 | 4.99 |
| fill 64K cold | splitmix | 22.98 | 22.81 | 23.00 | 2.66 |
| fill 64K cold | xoshiro | 15.33 | 15.29 | 15.46 | 3.98 |
| fill 64K cold | chacha8 | 6.91 | 6.91 | 6.91 | 8.83 |
| fill 64K cold | aes-ctr | 8.57 | 8.48 | 8.61 | 7.12 |
| fill+crc 64K cold | stamped | 12.18 | 12.17 | 12.18 | 5.01 |
| fill+crc 64K cold | splitmix | 18.57 | 18.46 | 18.89 | 3.29 |
| fill+crc 64K cold | xoshiro | 11.67 | 11.65 | 11.71 | 5.23 |
| fill+crc 64K cold | chacha8 | 6.38 | 6.38 | 6.38 | 9.56 |
| fill+crc 64K cold | aes-ctr | 7.77 | 7.74 | 7.77 | 7.86 |
| verify 64K cold | stamped | 10.79 | 10.73 | 10.81 | 5.66 |
| verify 64K cold | splitmix | 15.77 | 15.76 | 15.80 | 3.87 |
| verify 64K cold | xoshiro | 11.58 | 11.57 | 11.58 | 5.27 |
| verify 64K cold | chacha8 | 5.34 | 5.34 | 5.35 | 11.42 |
| verify 64K cold | aes-ctr | 7.76 | 7.76 | 7.76 | 7.86 |
| crc 64K cold | crc64nvme | 37.57 | 37.56 | 37.58 | 1.62 |
| copy 64K cold | memcpy | 9.16 | 9.09 | 9.16 | 6.66 |
| fill 64K hot | stamped | 64.83 | 64.79 | 64.85 | 0.941 |
| fill 64K hot | splitmix | 59.50 | 59.42 | 59.50 | 1.03 |
| fill 64K hot | xoshiro | 23.65 | 23.64 | 23.65 | 2.58 |
| fill 64K hot | chacha8 | 7.06 | 7.06 | 7.06 | 8.64 |
| fill 64K hot | aes-ctr | 11.76 | 11.76 | 11.76 | 5.19 |
| fill+crc 64K hot | stamped | 35.19 | 35.18 | 35.20 | 1.73 |
| fill+crc 64K hot | splitmix | 33.54 | 33.53 | 33.54 | 1.82 |
| fill+crc 64K hot | xoshiro | 17.91 | 17.91 | 17.92 | 3.41 |
| fill+crc 64K hot | chacha8 | 6.49 | 6.49 | 6.49 | 9.40 |
| fill+crc 64K hot | aes-ctr | 10.19 | 10.19 | 10.20 | 5.99 |
| verify 64K hot | stamped | 34.96 | 34.95 | 34.96 | 1.75 |
| verify 64K hot | splitmix | 33.42 | 33.41 | 33.43 | 1.83 |
| verify 64K hot | xoshiro | 17.91 | 17.91 | 17.91 | 3.41 |
| verify 64K hot | chacha8 | 6.49 | 6.49 | 6.49 | 9.40 |
| verify 64K hot | aes-ctr | 10.19 | 10.19 | 10.19 | 5.99 |
| crc 64K hot | crc64nvme | 76.28 | 76.22 | 76.30 | 0.800 |
| copy 64K hot | memcpy | 74.46 | 74.40 | 74.46 | 0.820 |
| fill 1M cold | stamped | 12.45 | 12.43 | 12.48 | 78.41 |
| fill 1M cold | splitmix | 22.92 | 22.91 | 23.13 | 42.60 |
| fill 1M cold | xoshiro | 15.22 | 14.98 | 15.22 | 64.18 |
| fill 1M cold | chacha8 | 6.91 | 6.90 | 6.91 | 141.4 |
| fill 1M cold | aes-ctr | 8.53 | 8.53 | 8.53 | 114.4 |
| fill+crc 1M cold | stamped | 10.96 | 10.94 | 10.98 | 89.08 |
| fill+crc 1M cold | splitmix | 18.03 | 18.02 | 18.03 | 54.17 |
| fill+crc 1M cold | xoshiro | 13.04 | 13.02 | 13.06 | 74.86 |
| fill+crc 1M cold | chacha8 | 6.37 | 6.37 | 6.37 | 153.4 |
| fill+crc 1M cold | aes-ctr | 7.71 | 7.71 | 7.71 | 126.7 |
| verify 1M cold | stamped | 17.55 | 17.41 | 17.62 | 55.65 |
| verify 1M cold | splitmix | 23.31 | 23.06 | 23.33 | 41.90 |
| verify 1M cold | xoshiro | 14.40 | 14.34 | 14.41 | 67.82 |
| verify 1M cold | chacha8 | 5.95 | 5.95 | 5.96 | 164.1 |
| verify 1M cold | aes-ctr | 8.82 | 8.81 | 8.82 | 110.8 |
| crc 1M cold | crc64nvme | 40.45 | 40.41 | 40.50 | 24.14 |
| copy 1M cold | memcpy | 10.00 | 9.97 | 10.02 | 97.68 |
| fill 1M hot | stamped | 54.92 | 54.89 | 55.02 | 17.78 |
| fill 1M hot | splitmix | 59.87 | 59.87 | 59.89 | 16.31 |
| fill 1M hot | xoshiro | 23.51 | 23.50 | 23.53 | 41.54 |
| fill 1M hot | chacha8 | 7.07 | 7.07 | 7.07 | 138.1 |
| fill 1M hot | aes-ctr | 11.61 | 11.61 | 11.61 | 84.09 |
| fill+crc 1M hot | stamped | 31.89 | 31.89 | 31.90 | 30.62 |
| fill+crc 1M hot | splitmix | 33.57 | 33.56 | 33.57 | 29.09 |
| fill+crc 1M hot | xoshiro | 17.85 | 17.83 | 17.85 | 54.71 |
| fill+crc 1M hot | chacha8 | 6.50 | 6.50 | 6.50 | 150.3 |
| fill+crc 1M hot | aes-ctr | 10.08 | 10.08 | 10.08 | 96.91 |
| verify 1M hot | stamped | 29.88 | 29.86 | 29.89 | 32.68 |
| verify 1M hot | splitmix | 30.65 | 30.65 | 30.67 | 31.86 |
| verify 1M hot | xoshiro | 17.02 | 16.95 | 17.03 | 57.39 |
| verify 1M hot | chacha8 | 6.37 | 6.37 | 6.37 | 153.3 |
| verify 1M hot | aes-ctr | 9.75 | 9.75 | 9.76 | 100.1 |
| crc 1M hot | crc64nvme | 76.02 | 75.99 | 76.08 | 12.85 |
| copy 1M hot | memcpy | 62.35 | 62.33 | 62.43 | 15.66 |

### 3a. Making bytes on several cores, no wire, round 3

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | GiB/s, all | GiB/s, least core |
| --- | --- | --- | --- |
| fill+crc 1M cold x1 | stamped | 10.94 | 10.94 |
| fill+crc 1M cold x1 | splitmix | 17.97 | 17.97 |
| fill+crc 1M cold x1 | xoshiro | 13.16 | 13.16 |
| fill+crc 1M cold x1 | chacha8 | 6.35 | 6.35 |
| fill+crc 1M cold x1 | aes-ctr | 7.70 | 7.70 |
| fill+crc 1M cold x2 | stamped | 15.62 | 7.81 |
| fill+crc 1M cold x2 | splitmix | 18.93 | 9.46 |
| fill+crc 1M cold x2 | xoshiro | 15.09 | 7.53 |
| fill+crc 1M cold x2 | chacha8 | 11.54 | 5.77 |
| fill+crc 1M cold x2 | aes-ctr | 14.95 | 7.47 |
| fill+crc 1M cold x4 | stamped | 17.60 | 4.39 |
| fill+crc 1M cold x4 | splitmix | 18.73 | 4.67 |
| fill+crc 1M cold x4 | xoshiro | 18.85 | 4.68 |
| fill+crc 1M cold x4 | chacha8 | 18.38 | 4.59 |
| fill+crc 1M cold x4 | aes-ctr | 20.26 | 5.06 |

### 2. Against a server that discards, round 3

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | pattern | 6231 | 164.4 | 1.00 | 1.00 | 6230 | 168.9 | 1.000 | 338.5 | 0 | 0 |
| put 1M plain | fill stamped | 7386 | 138.6 | 1.000 | 1.00 | 7387 | 121.1 | 0.846 | 259.2 | 0 | 0 |
| put 1M plain | fill+crc stamped | 6799 | 150.6 | 1.000 | 1.00 | 6800 | 120.4 | 0.772 | 273.7 | 0 | 0 |
| put 1M plain | fill splitmix | 8252 | 124.1 | 1.000 | 1.00 | 8253 | 119.5 | 0.936 | 243.9 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 7486 | 136.8 | 1.000 | 1.00 | 7486 | 119.3 | 0.845 | 256.0 | 0 | 0 |
| put 1M plain | fill xoshiro | 6840 | 149.7 | 1.000 | 1.00 | 6841 | 119.7 | 0.772 | 274.2 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 6309 | 162.3 | 1.000 | 1.00 | 6311 | 119.0 | 0.706 | 281.1 | 0 | 0 |
| put 1M plain | fill chacha8 | 4149 | 246.8 | 1.000 | 1.00 | 4149 | 120.7 | 0.462 | 370.6 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 3949 | 259.3 | 1.000 | 1.00 | 3950 | 120.4 | 0.437 | 390.9 | 0 | 0 |
| put 1M plain | fill aes-ctr | 5294 | 193.4 | 1.000 | 1.00 | 5295 | 120.0 | 0.593 | 319.9 | 0 | 0 |
| put 1M plain | fill+crc aes-ctr | 4979 | 205.7 | 1.000 | 1.00 | 4979 | 119.9 | 0.556 | 337.6 | 0 | 0 |
| get 1M plain | none | 5732 | 106.2 | 0.595 | 0.610 | 9640 | 183.6 | 1.000 | 269.7 | 0 | 0 |
| get 1M plain | crc | 5605 | 115.2 | 0.631 | 0.661 | 8885 | 187.6 | 1.000 | 282.4 | 0 | 0 |
| get 1M plain | regenerate stamped | 5381 | 132.1 | 0.694 | 0.710 | 7752 | 195.2 | 1.000 | 309.3 | 0 | 0 |
| get 1M plain | regenerate splitmix | 5436 | 114.7 | 0.609 | 0.601 | 8928 | 193.6 | 1.000 | 285.1 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 5610 | 129.0 | 0.707 | 0.722 | 7936 | 187.6 | 1.000 | 302.6 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 4527 | 210.7 | 0.932 | 0.947 | 4859 | 148.5 | 0.629 | 357.4 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 6245 | 154.0 | 0.939 | 0.954 | 6651 | 146.7 | 0.867 | 301.3 | 0 | 0 |
| put 1M ktls | pattern | 2665 | 366.3 | 0.954 | 0.954 | 2795 | 377.8 | 0.956 | 760.6 | 0 | 1.00 |
| put 1M ktls | fill stamped | 1766 | 281.4 | 0.485 | 0.490 | 3639 | 457.8 | 0.762 | 761.7 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 2156 | 301.0 | 0.634 | 0.637 | 3402 | 377.2 | 0.767 | 692.4 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 2670 | 290.4 | 0.757 | 0.762 | 3526 | 331.8 | 0.838 | 640.3 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 2781 | 308.0 | 0.837 | 0.840 | 3325 | 331.2 | 0.873 | 655.2 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 1761 | 291.5 | 0.501 | 0.504 | 3513 | 450.3 | 0.748 | 760.3 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 1754 | 300.8 | 0.515 | 0.517 | 3404 | 443.4 | 0.733 | 768.1 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 1761 | 387.5 | 0.666 | 0.667 | 2643 | 444.0 | 0.736 | 848.9 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 1615 | 399.5 | 0.630 | 0.631 | 2563 | 482.0 | 0.734 | 903.7 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 2792 | 366.0 | 0.998 | 0.998 | 2798 | 348.7 | 0.924 | 728.2 | 0 | 1.00 |
| put 1M ktls | fill+crc aes-ctr | 2006 | 350.9 | 0.688 | 0.693 | 2918 | 398.0 | 0.753 | 769.6 | 0 | 1.00 |
| get 1M ktls | none | 1762 | 340.7 | 0.586 | 0.600 | 3006 | 341.5 | 0.561 | 699.7 | 0 | 1.00 |
| get 1M ktls | crc | 1617 | 370.8 | 0.586 | 0.605 | 2761 | 347.1 | 0.521 | 744.4 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 1378 | 449.7 | 0.605 | 0.632 | 2277 | 351.5 | 0.446 | 838.0 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 1558 | 390.9 | 0.595 | 0.614 | 2620 | 350.2 | 0.506 | 771.5 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 1380 | 456.6 | 0.615 | 0.640 | 2243 | 343.3 | 0.436 | 838.2 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 1312 | 515.8 | 0.661 | 0.677 | 1985 | 353.4 | 0.426 | 889.6 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 1408 | 461.6 | 0.635 | 0.652 | 2218 | 354.1 | 0.460 | 837.7 | 0 | 1.00 |

### 3b. The put from several client cores, round 3

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain x1 | fill+crc splitmix | 7393 | 138.5 | 1.000 | 1.00 | 7394 | 120.6 | 0.844 | 259.0 | 0 | 0 |
| put 1M plain x1 | fill+crc xoshiro | 6271 | 163.3 | 1.000 | 1.00 | 6272 | 120.4 | 0.710 | 282.1 | 0 | 0 |
| put 1M plain x1 | fill+crc chacha8 | 3932 | 260.4 | 1.000 | 1.00 | 3933 | 122.2 | 0.442 | 389.6 | 0 | 0 |
| put 1M plain x1 | fill+crc aes-ctr | 4950 | 206.8 | 1.000 | 1.00 | 4951 | 120.5 | 0.555 | 333.4 | 0 | 0 |
| put 1M plain x2 | fill+crc splitmix | 14685 | 139.5 | 1.000 | 1.00 | 7343 | 119.3 | 0.847 | 255.6 | 0 | 0 |
| put 1M plain x2 | fill+crc xoshiro | 12399 | 165.2 | 1.000 | 1.00 | 6200 | 119.5 | 0.717 | 280.1 | 0 | 0 |
| put 1M plain x2 | fill+crc chacha8 | 7789 | 262.9 | 1.000 | 1.00 | 3895 | 119.1 | 0.445 | 380.4 | 0 | 0 |
| put 1M plain x2 | fill+crc aes-ctr | 9793 | 209.1 | 1.000 | 1.00 | 4897 | 118.9 | 0.561 | 328.7 | 0 | 0 |
| put 1M plain x4 | fill+crc splitmix | 28059 | 146.0 | 1.000 | 1.00 | 7015 | 124.1 | 0.851 | 259.4 | 0 | 0 |
| put 1M plain x4 | fill+crc xoshiro | 22509 | 182.0 | 1.000 | 1.00 | 5628 | 131.9 | 0.757 | 313.0 | 0 | 0 |
| put 1M plain x4 | fill+crc chacha8 | 15174 | 269.9 | 1.000 | 1.00 | 3794 | 120.6 | 0.450 | 389.2 | 0 | 0 |
| put 1M plain x4 | fill+crc aes-ctr | 18961 | 216.0 | 1.000 | 1.00 | 4740 | 122.0 | 0.568 | 335.5 | 0 | 0 |

### 4. A folder of real files, round 3

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | MiB/s | cpu ms/GiB | device MiB read | from device |
| --- | --- | --- | --- | --- | --- |
| 64K cold | read | 1061 | 462.2 | 2048 | 1.00 |
| 64K cold | read+crc | 1044 | 477.3 | 2048 | 1.00 |
| 64K cold | read+sha256 | 737.4 | 884.4 | 2048 | 1.00 |
| 64K hot | read | 6025 | 169.9 | 0 | 0 |
| 64K hot | read+crc | 5749 | 178.1 | 0 | 0 |
| 64K hot | read+sha256 | 1767 | 579.4 | 0 | 0 |
| 1M cold | read | 2198 | 162.5 | 2048 | 1.00 |
| 1M cold | read+crc | 2128 | 178.2 | 2048 | 1.00 |
| 1M cold | read+sha256 | 1167 | 574.2 | 2048 | 1.00 |
| 1M hot | read | 17412 | 58.80 | 0 | 0 |
| 1M hot | read+crc | 14589 | 70.17 | 0 | 0 |
| 1M hot | read+sha256 | 2212 | 462.8 | 0 | 0 |
| 64M cold | read | 2606 | 117.4 | 2048 | 1.00 |
| 64M cold | read+crc | 2603 | 131.5 | 2048 | 1.00 |
| 64M cold | read+sha256 | 1315 | 527.0 | 2048 | 1.00 |
| 64M hot | read | 21583 | 47.44 | 0 | 0 |
| 64M hot | read+crc | 17432 | 58.73 | 0 | 0 |
| 64M hot | read+sha256 | 2278 | 449.4 | 0 | 0 |

