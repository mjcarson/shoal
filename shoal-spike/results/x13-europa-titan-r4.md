### 2. Against a server that discards, round 4

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa to titan

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | fill+crc aes-ctr | 112.5 | 274.7 | 0.030 | 0.026 | 3728 | 1657 | 0.170 | 3328 | 0 | 0 |
| put 1M plain | fill aes-ctr | 112.5 | 262.1 | 0.029 | 0.040 | 3906 | 1723 | 0.177 | 3383 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 112.5 | 344.1 | 0.038 | 0.040 | 2976 | 1714 | 0.176 | 3438 | 0 | 0 |
| put 1M plain | fill chacha8 | 112.3 | 323.2 | 0.035 | 0.079 | 3168 | 1661 | 0.170 | 3517 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 112.3 | 217.5 | 0.024 | 0 | 4708 | 1657 | 0.170 | 2824 | 0 | 0 |
| put 1M plain | fill xoshiro | 112.5 | 193.9 | 0.021 | 0.004 | 5282 | 1713 | 0.176 | 5857 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 112.1 | 168.1 | 0.018 | 0 | 6091 | 1721 | 0.176 | 2902 | 0 | 0 |
| put 1M plain | fill splitmix | 112.1 | 149.1 | 0.016 | 0.010 | 6866 | 1668 | 0.170 | 3103 | 0 | 0 |
| put 1M plain | fill+crc stamped | 112.3 | 199.9 | 0.022 | 0 | 5122 | 1723 | 0.177 | 2934 | 0 | 0 |
| put 1M plain | fill stamped | 112.3 | 181.4 | 0.020 | 0.020 | 5645 | 1716 | 0.176 | 3079 | 0 | 0 |
| put 1M plain | pattern | 112.5 | 130.1 | 0.014 | 0 | 7871 | 1712 | 0.176 | 2910 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 112.4 | 407.0 | 0.045 | 0.033 | 2516 | 673.9 | 0.056 | 3699 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 112.4 | 460.3 | 0.051 | 0.037 | 2224 | 651.4 | 0.053 | 3407 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 112.2 | 363.8 | 0.040 | 0.026 | 2814 | 645.2 | 0.053 | 3377 | 0 | 0 |
| get 1M plain | regenerate splitmix | 112.4 | 339.0 | 0.037 | 0.024 | 3020 | 646.8 | 0.053 | 3407 | 0 | 0 |
| get 1M plain | regenerate stamped | 112.4 | 355.9 | 0.039 | 0.026 | 2878 | 651.0 | 0.053 | 3790 | 0 | 0 |
| get 1M plain | crc | 112.4 | 319.4 | 0.035 | 0.024 | 3206 | 644.7 | 0.053 | 3735 | 0 | 0 |
| get 1M plain | none | 112.4 | 303.7 | 0.033 | 0.022 | 3372 | 709.2 | 0.060 | 3334 | 0 | 0 |
| put 1M ktls | fill+crc aes-ctr | 112.1 | 480.8 | 0.053 | 0.058 | 2130 | 2532 | 0.265 | 4855 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 112.1 | 478.5 | 0.052 | 0.033 | 2140 | 2514 | 0.263 | 4070 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 112.1 | 576.1 | 0.063 | 0.057 | 1777 | 2568 | 0.269 | 4563 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 112.1 | 561.0 | 0.061 | 0.086 | 1825 | 2532 | 0.265 | 4855 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 112.1 | 446.2 | 0.049 | 0.056 | 2295 | 2574 | 0.270 | 4362 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 112.1 | 420.0 | 0.046 | 0.044 | 2438 | 2521 | 0.264 | 4289 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 112.1 | 408.0 | 0.045 | 0.079 | 2510 | 2576 | 0.270 | 4764 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 112.3 | 385.6 | 0.042 | 0.059 | 2655 | 2515 | 0.264 | 4573 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 112.1 | 425.9 | 0.047 | 0.059 | 2404 | 2580 | 0.270 | 4782 | 0 | 1.00 |
| put 1M ktls | fill stamped | 112.1 | 410.1 | 0.045 | 0.090 | 2497 | 2578 | 0.270 | 4801 | 0 | 1.00 |
| put 1M ktls | pattern | 111.9 | 441.0 | 0.048 | 0.067 | 2322 | 2581 | 0.270 | 4590 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 112.0 | 632.7 | 0.069 | 0.059 | 1619 | 1465 | 0.142 | 4590 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 112.0 | 696.4 | 0.076 | 0.061 | 1471 | 1502 | 0.143 | 8338 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 112.0 | 601.5 | 0.066 | 0.057 | 1702 | 1472 | 0.143 | 4297 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 112.0 | 579.6 | 0.063 | 0.051 | 1767 | 1467 | 0.142 | 4370 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 112.0 | 597.8 | 0.065 | 0.057 | 1713 | 1501 | 0.146 | 4645 | 0 | 1.00 |
| get 1M ktls | crc | 112.0 | 548.7 | 0.060 | 0.051 | 1866 | 1451 | 0.140 | 4827 | 0 | 1.00 |
| get 1M ktls | none | 111.9 | 540.5 | 0.059 | 0.049 | 1895 | 1464 | 0.141 | 4608 | 0 | 1.00 |

