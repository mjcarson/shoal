### 2. Against a server that discards, round 1

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa to titan

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | pattern | 112.3 | 135.4 | 0.015 | 0.032 | 7563 | 1671 | 0.171 | 3936 | 0 | 0 |
| put 1M plain | fill stamped | 112.3 | 190.5 | 0.021 | 0.105 | 5376 | 1718 | 0.176 | 4027 | 0 | 0 |
| put 1M plain | fill+crc stamped | 112.1 | 196.8 | 0.022 | 0.049 | 5202 | 1669 | 0.171 | 3851 | 0 | 0 |
| put 1M plain | fill splitmix | 112.3 | 150.0 | 0.016 | 0.037 | 6826 | 1727 | 0.177 | 3644 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 112.5 | 169.5 | 0.019 | 0.047 | 6042 | 1702 | 0.175 | 3856 | 0 | 0 |
| put 1M plain | fill xoshiro | 112.1 | 203.6 | 0.022 | 0.060 | 5028 | 1730 | 0.177 | 4034 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 112.1 | 222.2 | 0.024 | 0.095 | 4608 | 1714 | 0.176 | 3870 | 0 | 0 |
| put 1M plain | fill chacha8 | 112.3 | 312.8 | 0.034 | 0.087 | 3274 | 1695 | 0.173 | 6596 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 112.3 | 334.0 | 0.037 | 0.064 | 3066 | 1729 | 0.177 | 3662 | 0 | 0 |
| put 1M plain | fill aes-ctr | 112.3 | 259.6 | 0.028 | 0.042 | 3944 | 1729 | 0.177 | 3535 | 0 | 0 |
| put 1M plain | fill+crc aes-ctr | 112.3 | 278.2 | 0.031 | 0.026 | 3681 | 1717 | 0.176 | 3444 | 0 | 0 |
| get 1M plain | none | 112.4 | 306.2 | 0.034 | 0.020 | 3344 | 641.0 | 0.052 | 3407 | 0 | 0 |
| get 1M plain | crc | 112.4 | 323.8 | 0.036 | 0.024 | 3162 | 661.4 | 0.054 | 3425 | 0 | 0 |
| get 1M plain | regenerate stamped | 112.2 | 357.4 | 0.039 | 0.030 | 2865 | 644.3 | 0.052 | 5567 | 0 | 0 |
| get 1M plain | regenerate splitmix | 112.2 | 342.5 | 0.038 | 0.026 | 2990 | 640.2 | 0.052 | 3541 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 112.4 | 365.9 | 0.040 | 0.029 | 2799 | 651.1 | 0.053 | 3334 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 112.4 | 461.5 | 0.051 | 0.037 | 2219 | 667.5 | 0.055 | 3261 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 112.4 | 407.0 | 0.045 | 0.035 | 2516 | 653.3 | 0.053 | 3681 | 0 | 0 |
| put 1M ktls | pattern | 111.9 | 445.8 | 0.049 | 0.094 | 2297 | 2587 | 0.270 | 5193 | 0 | 1.00 |
| put 1M ktls | fill stamped | 112.1 | 412.1 | 0.045 | 0.085 | 2485 | 2574 | 0.270 | 5312 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 111.9 | 409.4 | 0.045 | 0.048 | 2501 | 2536 | 0.265 | 4626 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 112.1 | 388.0 | 0.042 | 0.050 | 2639 | 2541 | 0.266 | 4545 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 112.1 | 410.3 | 0.045 | 0.061 | 2496 | 2595 | 0.272 | 7429 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 112.1 | 417.6 | 0.046 | 0.053 | 2452 | 2532 | 0.265 | 4709 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 112.1 | 441.9 | 0.048 | 0.096 | 2317 | 2590 | 0.271 | 5056 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 112.1 | 560.7 | 0.061 | 0.061 | 1826 | 2587 | 0.271 | 4490 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 112.1 | 589.8 | 0.065 | 0.095 | 1736 | 2528 | 0.265 | 4837 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 112.1 | 487.9 | 0.053 | 0.060 | 2099 | 2586 | 0.271 | 4691 | 0 | 1.00 |
| put 1M ktls | fill+crc aes-ctr | 112.1 | 509.8 | 0.056 | 0.083 | 2009 | 2560 | 0.268 | 6900 | 0 | 1.00 |
| get 1M ktls | none | 112.0 | 541.4 | 0.059 | 0.047 | 1891 | 1447 | 0.140 | 4261 | 0 | 1.00 |
| get 1M ktls | crc | 112.0 | 557.9 | 0.061 | 0.049 | 1835 | 1446 | 0.140 | 4407 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 112.0 | 599.0 | 0.065 | 0.051 | 1710 | 1472 | 0.143 | 4571 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 112.0 | 579.7 | 0.063 | 0.055 | 1766 | 1524 | 0.145 | 8302 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 112.0 | 602.1 | 0.066 | 0.059 | 1701 | 1487 | 0.143 | 4827 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 112.0 | 701.4 | 0.077 | 0.067 | 1460 | 1467 | 0.142 | 4846 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 112.0 | 628.4 | 0.069 | 0.057 | 1629 | 1468 | 0.142 | 4974 | 0 | 1.00 |

