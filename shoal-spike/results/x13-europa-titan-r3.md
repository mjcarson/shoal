### 2. Against a server that discards, round 3

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa to titan

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | pattern | 112.3 | 133.0 | 0.015 | 0.012 | 7701 | 1661 | 0.170 | 2970 | 0 | 0 |
| put 1M plain | fill stamped | 112.1 | 182.6 | 0.020 | 0.014 | 5606 | 1714 | 0.175 | 2957 | 0 | 0 |
| put 1M plain | fill+crc stamped | 112.5 | 198.9 | 0.022 | 0.026 | 5149 | 1719 | 0.176 | 3183 | 0 | 0 |
| put 1M plain | fill splitmix | 112.1 | 145.7 | 0.016 | 0.002 | 7027 | 1668 | 0.170 | 3012 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 112.3 | 169.1 | 0.019 | 0 | 6056 | 1716 | 0.176 | 2824 | 0 | 0 |
| put 1M plain | fill xoshiro | 112.1 | 196.2 | 0.021 | 0.014 | 5219 | 1709 | 0.175 | 2957 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 112.3 | 209.8 | 0.023 | 0.010 | 4881 | 1689 | 0.172 | 5976 | 0 | 0 |
| put 1M plain | fill chacha8 | 112.1 | 314.9 | 0.034 | 0.006 | 3252 | 1695 | 0.173 | 3231 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 112.3 | 334.7 | 0.037 | 0.028 | 3060 | 1670 | 0.171 | 3134 | 0 | 0 |
| put 1M plain | fill aes-ctr | 112.3 | 254.0 | 0.028 | 0.002 | 4032 | 1714 | 0.176 | 2715 | 0 | 0 |
| put 1M plain | fill+crc aes-ctr | 112.3 | 274.1 | 0.030 | 0.002 | 3736 | 1716 | 0.176 | 2770 | 0 | 0 |
| get 1M plain | none | 112.4 | 306.3 | 0.034 | 0.022 | 3343 | 655.6 | 0.054 | 3371 | 0 | 0 |
| get 1M plain | crc | 112.4 | 321.2 | 0.035 | 0.026 | 3188 | 695.1 | 0.058 | 3517 | 0 | 0 |
| get 1M plain | regenerate stamped | 112.4 | 356.2 | 0.039 | 0.028 | 2875 | 645.4 | 0.053 | 3189 | 0 | 0 |
| get 1M plain | regenerate splitmix | 112.2 | 339.9 | 0.037 | 0.033 | 3013 | 633.6 | 0.051 | 3851 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 112.4 | 368.2 | 0.040 | 0.032 | 2781 | 645.8 | 0.053 | 3498 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 112.2 | 461.0 | 0.050 | 0.035 | 2221 | 653.1 | 0.053 | 3486 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 112.4 | 408.7 | 0.045 | 0.035 | 2505 | 678.6 | 0.056 | 3535 | 0 | 0 |
| put 1M ktls | pattern | 112.3 | 450.4 | 0.049 | 0.054 | 2273 | 2536 | 0.266 | 7161 | 0 | 1.00 |
| put 1M ktls | fill stamped | 112.1 | 400.1 | 0.044 | 0.068 | 2559 | 2576 | 0.270 | 4490 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 112.1 | 421.2 | 0.046 | 0.063 | 2431 | 2577 | 0.270 | 4928 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 112.1 | 384.7 | 0.042 | 0.063 | 2662 | 2522 | 0.264 | 4801 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 112.1 | 400.6 | 0.044 | 0.040 | 2556 | 2584 | 0.270 | 4509 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 112.1 | 431.9 | 0.047 | 0.046 | 2371 | 2539 | 0.266 | 4417 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 112.1 | 439.6 | 0.048 | 0.050 | 2329 | 2573 | 0.269 | 4344 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 112.1 | 555.1 | 0.061 | 0.079 | 1845 | 2520 | 0.263 | 4928 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 112.1 | 579.8 | 0.063 | 0.088 | 1766 | 2574 | 0.269 | 5220 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 112.1 | 483.5 | 0.053 | 0.044 | 2118 | 2568 | 0.269 | 4216 | 0 | 1.00 |
| put 1M ktls | fill+crc aes-ctr | 112.1 | 494.3 | 0.054 | 0.021 | 2072 | 2520 | 0.263 | 3943 | 0 | 1.00 |
| get 1M ktls | none | 112.0 | 543.4 | 0.059 | 0.047 | 1884 | 1486 | 0.143 | 4901 | 0 | 1.00 |
| get 1M ktls | crc | 112.0 | 559.1 | 0.061 | 0.049 | 1831 | 1468 | 0.142 | 4718 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 111.9 | 598.5 | 0.065 | 0.053 | 1711 | 1554 | 0.147 | 8649 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 112.0 | 571.7 | 0.063 | 0.049 | 1791 | 1470 | 0.142 | 4443 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 112.0 | 606.0 | 0.066 | 0.061 | 1690 | 1474 | 0.143 | 4480 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 112.0 | 682.1 | 0.075 | 0.059 | 1501 | 1483 | 0.144 | 4773 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 112.0 | 638.0 | 0.070 | 0.061 | 1605 | 1466 | 0.142 | 4773 | 0 | 1.00 |

