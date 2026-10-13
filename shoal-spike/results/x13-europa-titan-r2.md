### 2. Against a server that discards, round 2

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa to titan

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | fill+crc aes-ctr | 112.1 | 278.5 | 0.030 | 0.008 | 3677 | 1674 | 0.171 | 3249 | 0 | 0 |
| put 1M plain | fill aes-ctr | 112.1 | 258.4 | 0.028 | 0 | 3963 | 1678 | 0.171 | 3012 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 112.3 | 339.0 | 0.037 | 0.042 | 3021 | 1677 | 0.172 | 3462 | 0 | 0 |
| put 1M plain | fill chacha8 | 112.3 | 317.0 | 0.035 | 0.024 | 3230 | 1673 | 0.171 | 3261 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 112.3 | 213.2 | 0.023 | 0 | 4803 | 1729 | 0.177 | 3043 | 0 | 0 |
| put 1M plain | fill xoshiro | 112.3 | 197.7 | 0.022 | 0.002 | 5179 | 1659 | 0.170 | 2660 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 112.3 | 169.0 | 0.019 | 0.020 | 6060 | 1656 | 0.170 | 2861 | 0 | 0 |
| put 1M plain | fill splitmix | 112.3 | 142.8 | 0.016 | 0.010 | 7172 | 1667 | 0.171 | 3043 | 0 | 0 |
| put 1M plain | fill+crc stamped | 112.1 | 199.5 | 0.022 | 0.002 | 5133 | 1734 | 0.177 | 3012 | 0 | 0 |
| put 1M plain | fill stamped | 112.5 | 183.3 | 0.020 | 0 | 5587 | 1716 | 0.176 | 2892 | 0 | 0 |
| put 1M plain | pattern | 112.3 | 132.0 | 0.014 | 0.014 | 7755 | 1667 | 0.171 | 2915 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 112.4 | 406.4 | 0.045 | 0.043 | 2520 | 644.6 | 0.052 | 3626 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 112.4 | 457.9 | 0.050 | 0.039 | 2236 | 722.8 | 0.061 | 3990 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 112.4 | 359.7 | 0.039 | 0.021 | 2847 | 723.2 | 0.061 | 3662 | 0 | 0 |
| get 1M plain | regenerate splitmix | 112.2 | 341.3 | 0.037 | 0.024 | 3000 | 644.8 | 0.053 | 3632 | 0 | 0 |
| get 1M plain | regenerate stamped | 112.1 | 358.3 | 0.039 | 0.028 | 2858 | 608.9 | 0.048 | 3377 | 0 | 0 |
| get 1M plain | crc | 112.2 | 320.0 | 0.035 | 0.022 | 3200 | 695.7 | 0.058 | 3724 | 0 | 0 |
| get 1M plain | none | 112.2 | 305.3 | 0.033 | 0.026 | 3354 | 648.1 | 0.053 | 3541 | 0 | 0 |
| put 1M ktls | fill+crc aes-ctr | 112.1 | 508.8 | 0.056 | 0.076 | 2013 | 2522 | 0.264 | 4892 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 112.5 | 464.7 | 0.051 | 0.074 | 2203 | 2542 | 0.267 | 7603 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 112.3 | 573.5 | 0.063 | 0.081 | 1785 | 2570 | 0.270 | 4592 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 112.1 | 557.6 | 0.061 | 0.056 | 1836 | 2589 | 0.271 | 4563 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 112.1 | 444.6 | 0.049 | 0.052 | 2303 | 2525 | 0.264 | 4618 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 111.9 | 424.9 | 0.046 | 0.040 | 2410 | 2569 | 0.268 | 4480 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 112.1 | 399.1 | 0.044 | 0.016 | 2565 | 2540 | 0.266 | 4107 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 112.1 | 384.1 | 0.042 | 0.010 | 2666 | 2519 | 0.263 | 4253 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 112.1 | 420.4 | 0.046 | 0.038 | 2436 | 2582 | 0.270 | 4582 | 0 | 1.00 |
| put 1M ktls | fill stamped | 112.1 | 403.3 | 0.044 | 0.061 | 2539 | 2583 | 0.270 | 4655 | 0 | 1.00 |
| put 1M ktls | pattern | 112.1 | 446.7 | 0.049 | 0.048 | 2292 | 2520 | 0.264 | 4326 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 112.0 | 644.3 | 0.070 | 0.057 | 1589 | 1447 | 0.140 | 4407 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 112.0 | 696.5 | 0.076 | 0.061 | 1470 | 1479 | 0.143 | 4955 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 112.0 | 600.9 | 0.066 | 0.057 | 1704 | 1477 | 0.143 | 4462 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 112.0 | 576.0 | 0.063 | 0.053 | 1778 | 1475 | 0.143 | 4443 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 112.0 | 592.2 | 0.065 | 0.053 | 1729 | 1476 | 0.143 | 4370 | 0 | 1.00 |
| get 1M ktls | crc | 112.0 | 559.1 | 0.061 | 0.047 | 1832 | 1490 | 0.145 | 4370 | 0 | 1.00 |
| get 1M ktls | none | 112.0 | 541.6 | 0.059 | 0.043 | 1891 | 1504 | 0.146 | 4645 | 0 | 1.00 |

