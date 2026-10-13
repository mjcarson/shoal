### Checks, round 2

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
| describe fixed | expand | 1.00 | 7536351 |
| describe uniform | expand | 1.00 | 8708741 |
| describe doublings | expand | 1.00 | 10459829 |
| describe table | expand | 1.00 | 7871856 |

### 1. Making bytes on one core, GiB/s, round 2

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | GiB/s | lowest run | highest run | µs a call |
| --- | --- | --- | --- | --- | --- |
| fill 4K cold | stamped | 11.09 | 11.08 | 11.11 | 0.344 |
| fill 4K cold | splitmix | 23.58 | 23.53 | 23.59 | 0.162 |
| fill 4K cold | xoshiro | 14.67 | 14.65 | 14.69 | 0.260 |
| fill 4K cold | chacha8 | 6.78 | 6.66 | 6.78 | 0.563 |
| fill 4K cold | aes-ctr | 10.25 | 10.18 | 10.25 | 0.372 |
| fill+crc 4K cold | stamped | 13.73 | 13.01 | 13.74 | 0.278 |
| fill+crc 4K cold | splitmix | 23.02 | 23.00 | 23.02 | 0.166 |
| fill+crc 4K cold | xoshiro | 12.43 | 12.42 | 12.43 | 0.307 |
| fill+crc 4K cold | chacha8 | 6.25 | 6.24 | 6.25 | 0.611 |
| fill+crc 4K cold | aes-ctr | 7.79 | 7.77 | 7.88 | 0.490 |
| verify 4K cold | stamped | 16.85 | 16.73 | 16.85 | 0.226 |
| verify 4K cold | splitmix | 31.62 | 31.60 | 31.72 | 0.121 |
| verify 4K cold | xoshiro | 14.32 | 14.22 | 14.36 | 0.266 |
| verify 4K cold | chacha8 | 6.48 | 6.47 | 6.48 | 0.589 |
| verify 4K cold | aes-ctr | 9.65 | 9.61 | 9.67 | 0.395 |
| crc 4K cold | crc64nvme | 40.81 | 39.63 | 40.82 | 0.093 |
| copy 4K cold | memcpy | 6.47 | 6.46 | 6.47 | 0.590 |
| fill 4K hot | stamped | 86.60 | 86.33 | 86.77 | 0.044 |
| fill 4K hot | splitmix | 59.36 | 59.25 | 59.39 | 0.064 |
| fill 4K hot | xoshiro | 20.78 | 20.75 | 20.79 | 0.184 |
| fill 4K hot | chacha8 | 6.75 | 6.75 | 6.76 | 0.565 |
| fill 4K hot | aes-ctr | 10.84 | 10.84 | 10.87 | 0.352 |
| fill+crc 4K hot | stamped | 38.99 | 38.92 | 39.23 | 0.098 |
| fill+crc 4K hot | splitmix | 32.65 | 32.64 | 32.66 | 0.117 |
| fill+crc 4K hot | xoshiro | 15.77 | 15.76 | 15.78 | 0.242 |
| fill+crc 4K hot | chacha8 | 6.32 | 6.32 | 6.32 | 0.604 |
| fill+crc 4K hot | aes-ctr | 9.38 | 9.38 | 9.38 | 0.407 |
| verify 4K hot | stamped | 53.69 | 53.67 | 53.72 | 0.071 |
| verify 4K hot | splitmix | 43.04 | 43.03 | 43.07 | 0.089 |
| verify 4K hot | xoshiro | 18.11 | 18.10 | 18.14 | 0.211 |
| verify 4K hot | chacha8 | 6.61 | 6.61 | 6.61 | 0.577 |
| verify 4K hot | aes-ctr | 10.15 | 10.14 | 10.16 | 0.376 |
| crc 4K hot | crc64nvme | 67.93 | 67.73 | 67.99 | 0.056 |
| copy 4K hot | memcpy | 148.7 | 148.6 | 148.7 | 0.026 |
| fill 64K cold | stamped | 12.51 | 12.51 | 12.51 | 4.88 |
| fill 64K cold | splitmix | 23.61 | 23.60 | 23.63 | 2.59 |
| fill 64K cold | xoshiro | 15.83 | 15.82 | 15.85 | 3.86 |
| fill 64K cold | chacha8 | 6.92 | 6.91 | 6.92 | 8.83 |
| fill 64K cold | aes-ctr | 8.47 | 8.46 | 8.47 | 7.21 |
| fill+crc 64K cold | stamped | 12.47 | 12.46 | 12.49 | 4.89 |
| fill+crc 64K cold | splitmix | 18.89 | 18.75 | 18.93 | 3.23 |
| fill+crc 64K cold | xoshiro | 12.15 | 12.15 | 12.16 | 5.02 |
| fill+crc 64K cold | chacha8 | 6.41 | 6.41 | 6.42 | 9.52 |
| fill+crc 64K cold | aes-ctr | 7.65 | 7.62 | 7.66 | 7.97 |
| verify 64K cold | stamped | 10.57 | 10.56 | 10.58 | 5.77 |
| verify 64K cold | splitmix | 15.76 | 15.74 | 15.81 | 3.87 |
| verify 64K cold | xoshiro | 11.75 | 11.72 | 11.75 | 5.19 |
| verify 64K cold | chacha8 | 5.51 | 5.51 | 5.51 | 11.08 |
| verify 64K cold | aes-ctr | 7.76 | 7.76 | 7.76 | 7.87 |
| crc 64K cold | crc64nvme | 38.63 | 38.59 | 38.65 | 1.58 |
| copy 64K cold | memcpy | 9.61 | 9.46 | 9.64 | 6.35 |
| fill 64K hot | stamped | 64.49 | 64.44 | 64.49 | 0.946 |
| fill 64K hot | splitmix | 59.55 | 59.50 | 59.55 | 1.02 |
| fill 64K hot | xoshiro | 23.63 | 23.63 | 23.67 | 2.58 |
| fill 64K hot | chacha8 | 7.06 | 7.06 | 7.06 | 8.64 |
| fill 64K hot | aes-ctr | 11.75 | 11.75 | 11.75 | 5.19 |
| fill+crc 64K hot | stamped | 35.10 | 35.10 | 35.13 | 1.74 |
| fill+crc 64K hot | splitmix | 33.57 | 33.56 | 33.57 | 1.82 |
| fill+crc 64K hot | xoshiro | 17.93 | 17.93 | 17.93 | 3.40 |
| fill+crc 64K hot | chacha8 | 6.49 | 6.49 | 6.49 | 9.40 |
| fill+crc 64K hot | aes-ctr | 10.18 | 10.18 | 10.18 | 5.99 |
| verify 64K hot | stamped | 34.87 | 34.85 | 34.87 | 1.75 |
| verify 64K hot | splitmix | 33.44 | 33.44 | 33.45 | 1.83 |
| verify 64K hot | xoshiro | 17.91 | 17.91 | 17.91 | 3.41 |
| verify 64K hot | chacha8 | 6.49 | 6.49 | 6.49 | 9.40 |
| verify 64K hot | aes-ctr | 10.18 | 10.18 | 10.19 | 5.99 |
| crc 64K hot | crc64nvme | 76.47 | 76.45 | 76.50 | 0.798 |
| copy 64K hot | memcpy | 76.48 | 76.45 | 76.48 | 0.798 |
| fill 1M cold | stamped | 12.62 | 12.61 | 12.76 | 77.37 |
| fill 1M cold | splitmix | 23.60 | 23.26 | 23.65 | 41.37 |
| fill 1M cold | xoshiro | 15.76 | 15.74 | 15.89 | 61.97 |
| fill 1M cold | chacha8 | 6.92 | 6.92 | 6.92 | 141.2 |
| fill 1M cold | aes-ctr | 8.54 | 8.53 | 8.55 | 114.3 |
| fill+crc 1M cold | stamped | 10.86 | 10.84 | 10.89 | 89.95 |
| fill+crc 1M cold | splitmix | 17.52 | 17.50 | 17.52 | 55.75 |
| fill+crc 1M cold | xoshiro | 12.29 | 12.27 | 12.31 | 79.47 |
| fill+crc 1M cold | chacha8 | 6.28 | 6.28 | 6.28 | 155.6 |
| fill+crc 1M cold | aes-ctr | 7.72 | 7.71 | 7.73 | 126.6 |
| verify 1M cold | stamped | 17.35 | 17.31 | 17.36 | 56.29 |
| verify 1M cold | splitmix | 22.99 | 22.96 | 22.99 | 42.48 |
| verify 1M cold | xoshiro | 14.26 | 14.26 | 14.28 | 68.47 |
| verify 1M cold | chacha8 | 5.98 | 5.97 | 5.98 | 163.4 |
| verify 1M cold | aes-ctr | 8.82 | 8.81 | 8.82 | 110.8 |
| crc 1M cold | crc64nvme | 39.47 | 39.44 | 39.49 | 24.74 |
| copy 1M cold | memcpy | 10.46 | 10.36 | 10.53 | 93.35 |
| fill 1M hot | stamped | 54.96 | 54.95 | 55.02 | 17.77 |
| fill 1M hot | splitmix | 59.31 | 59.27 | 59.32 | 16.47 |
| fill 1M hot | xoshiro | 23.42 | 23.40 | 23.43 | 41.70 |
| fill 1M hot | chacha8 | 7.07 | 7.07 | 7.07 | 138.2 |
| fill 1M hot | aes-ctr | 11.47 | 11.47 | 11.47 | 85.15 |
| fill+crc 1M hot | stamped | 31.80 | 31.80 | 31.81 | 30.71 |
| fill+crc 1M hot | splitmix | 33.46 | 33.45 | 33.46 | 29.19 |
| fill+crc 1M hot | xoshiro | 17.77 | 17.76 | 17.77 | 54.96 |
| fill+crc 1M hot | chacha8 | 6.50 | 6.49 | 6.50 | 150.3 |
| fill+crc 1M hot | aes-ctr | 9.97 | 9.97 | 9.98 | 97.93 |
| verify 1M hot | stamped | 29.80 | 29.79 | 29.80 | 32.78 |
| verify 1M hot | splitmix | 30.62 | 30.62 | 30.66 | 31.89 |
| verify 1M hot | xoshiro | 16.97 | 16.97 | 17.01 | 57.55 |
| verify 1M hot | chacha8 | 6.38 | 6.38 | 6.39 | 153.0 |
| verify 1M hot | aes-ctr | 9.69 | 9.68 | 9.70 | 100.8 |
| crc 1M hot | crc64nvme | 75.96 | 75.92 | 75.97 | 12.86 |
| copy 1M hot | memcpy | 62.55 | 62.54 | 62.59 | 15.61 |

### 3a. Making bytes on several cores, no wire, round 2

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | GiB/s, all | GiB/s, least core |
| --- | --- | --- | --- |
| fill+crc 1M cold x1 | aes-ctr | 7.69 | 7.69 |
| fill+crc 1M cold x1 | chacha8 | 6.33 | 6.33 |
| fill+crc 1M cold x1 | xoshiro | 13.13 | 13.13 |
| fill+crc 1M cold x1 | splitmix | 17.71 | 17.71 |
| fill+crc 1M cold x1 | stamped | 10.94 | 10.94 |
| fill+crc 1M cold x2 | aes-ctr | 14.87 | 7.43 |
| fill+crc 1M cold x2 | chacha8 | 11.48 | 5.74 |
| fill+crc 1M cold x2 | xoshiro | 15.04 | 7.51 |
| fill+crc 1M cold x2 | splitmix | 18.93 | 9.46 |
| fill+crc 1M cold x2 | stamped | 15.56 | 7.78 |
| fill+crc 1M cold x4 | aes-ctr | 20.24 | 5.03 |
| fill+crc 1M cold x4 | chacha8 | 18.39 | 4.60 |
| fill+crc 1M cold x4 | xoshiro | 18.73 | 4.60 |
| fill+crc 1M cold x4 | splitmix | 18.83 | 4.71 |
| fill+crc 1M cold x4 | stamped | 17.67 | 4.41 |

### 2. Against a server that discards, round 2

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain | fill+crc aes-ctr | 4928 | 207.8 | 1.000 | 1.00 | 4928 | 121.5 | 0.557 | 333.2 | 0 | 0 |
| put 1M plain | fill aes-ctr | 5311 | 192.8 | 1.000 | 1.00 | 5312 | 120.2 | 0.597 | 320.4 | 0 | 0 |
| put 1M plain | fill+crc chacha8 | 3929 | 260.6 | 1.000 | 1.00 | 3929 | 121.9 | 0.441 | 391.9 | 0 | 0 |
| put 1M plain | fill chacha8 | 4140 | 247.3 | 1.000 | 1.00 | 4140 | 121.9 | 0.466 | 386.3 | 0 | 0 |
| put 1M plain | fill+crc xoshiro | 6284 | 162.9 | 1.000 | 1.00 | 6284 | 120.4 | 0.712 | 284.8 | 0 | 0 |
| put 1M plain | fill xoshiro | 5807 | 176.3 | 1.000 | 1.00 | 5809 | 149.0 | 0.818 | 397.4 | 0 | 0 |
| put 1M plain | fill+crc splitmix | 6298 | 162.6 | 1.000 | 1.00 | 6299 | 148.1 | 0.884 | 310.8 | 0 | 0 |
| put 1M plain | fill splitmix | 8282 | 123.6 | 1.000 | 1.00 | 8283 | 119.5 | 0.939 | 244.3 | 0 | 0 |
| put 1M plain | fill+crc stamped | 6764 | 151.4 | 1.000 | 1.00 | 6765 | 121.8 | 0.777 | 276.7 | 0 | 0 |
| put 1M plain | fill stamped | 7369 | 138.9 | 1.000 | 1.00 | 7370 | 121.4 | 0.848 | 260.9 | 0 | 0 |
| put 1M plain | pattern | 6471 | 158.2 | 1.000 | 1.00 | 6472 | 162.5 | 1.000 | 325.3 | 0 | 0 |
| get 1M plain | regenerate aes-ctr | 6269 | 153.9 | 0.942 | 0.959 | 6654 | 147.1 | 0.873 | 303.4 | 0 | 0 |
| get 1M plain | regenerate chacha8 | 4526 | 211.0 | 0.933 | 0.951 | 4853 | 148.4 | 0.628 | 360.5 | 0 | 0 |
| get 1M plain | regenerate xoshiro | 6663 | 122.6 | 0.797 | 0.830 | 8355 | 157.9 | 1.000 | 272.9 | 0 | 0 |
| get 1M plain | regenerate splitmix | 6817 | 108.3 | 0.721 | 0.762 | 9456 | 154.3 | 1.000 | 250.8 | 0 | 0 |
| get 1M plain | regenerate stamped | 5401 | 132.3 | 0.698 | 0.715 | 7738 | 194.8 | 1.000 | 315.4 | 0 | 0 |
| get 1M plain | crc | 5527 | 116.1 | 0.626 | 0.628 | 8824 | 190.4 | 1.000 | 282.3 | 0 | 0 |
| get 1M plain | none | 5729 | 106.4 | 0.595 | 0.599 | 9627 | 183.6 | 1.000 | 269.1 | 0 | 0 |
| put 1M ktls | fill+crc aes-ctr | 1804 | 345.7 | 0.609 | 0.608 | 2962 | 441.3 | 0.751 | 814.9 | 0 | 1.00 |
| put 1M ktls | fill aes-ctr | 1772 | 334.4 | 0.579 | 0.578 | 3062 | 441.8 | 0.738 | 820.3 | 0 | 1.00 |
| put 1M ktls | fill+crc chacha8 | 2436 | 412.7 | 0.982 | 0.982 | 2481 | 332.1 | 0.763 | 758.9 | 0 | 1.00 |
| put 1M ktls | fill chacha8 | 2056 | 390.8 | 0.785 | 0.782 | 2620 | 380.0 | 0.736 | 781.6 | 0 | 1.00 |
| put 1M ktls | fill+crc xoshiro | 1718 | 301.8 | 0.506 | 0.508 | 3393 | 457.2 | 0.740 | 778.1 | 0 | 1.00 |
| put 1M ktls | fill xoshiro | 1780 | 289.2 | 0.503 | 0.502 | 3541 | 441.8 | 0.741 | 747.5 | 0 | 1.00 |
| put 1M ktls | fill+crc splitmix | 1832 | 278.8 | 0.499 | 0.502 | 3673 | 442.4 | 0.765 | 738.9 | 0 | 1.00 |
| put 1M ktls | fill splitmix | 1872 | 266.4 | 0.487 | 0.490 | 3844 | 442.2 | 0.782 | 727.2 | 0 | 1.00 |
| put 1M ktls | fill+crc stamped | 2115 | 301.7 | 0.623 | 0.626 | 3394 | 393.9 | 0.786 | 715.5 | 0 | 1.00 |
| put 1M ktls | fill stamped | 2123 | 286.6 | 0.594 | 0.598 | 3573 | 393.7 | 0.789 | 697.3 | 0 | 1.00 |
| put 1M ktls | pattern | 3002 | 339.5 | 0.995 | 0.994 | 3016 | 339.8 | 0.968 | 691.6 | 0 | 1.00 |
| get 1M ktls | regenerate aes-ctr | 1483 | 447.2 | 0.647 | 0.664 | 2290 | 348.8 | 0.478 | 820.4 | 0 | 1.00 |
| get 1M ktls | regenerate chacha8 | 1224 | 554.9 | 0.663 | 0.687 | 1845 | 345.2 | 0.386 | 928.5 | 0 | 1.00 |
| get 1M ktls | regenerate xoshiro | 1373 | 458.6 | 0.615 | 0.642 | 2233 | 345.7 | 0.437 | 842.4 | 0 | 1.00 |
| get 1M ktls | regenerate splitmix | 1567 | 392.8 | 0.601 | 0.620 | 2607 | 343.8 | 0.499 | 753.8 | 0 | 1.00 |
| get 1M ktls | regenerate stamped | 1376 | 451.1 | 0.606 | 0.632 | 2270 | 351.7 | 0.446 | 846.5 | 0 | 1.00 |
| get 1M ktls | crc | 1599 | 372.0 | 0.581 | 0.596 | 2753 | 351.3 | 0.522 | 746.6 | 0 | 1.00 |
| get 1M ktls | none | 1498 | 397.4 | 0.582 | 0.612 | 2576 | 342.9 | 0.475 | 780.2 | 0 | 1.00 |

### 3b. The put from several client cores, round 2

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | MiB/s | client cpu ms/GiB | client busy | core busy | MiB/s at a busy core | server cpu ms/GiB | busiest executor | host cpu ms/GiB | mismatches | kTLS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| put 1M plain x1 | fill+crc aes-ctr | 4921 | 208.1 | 1.000 | 1.00 | 4921 | 123.4 | 0.565 | 336.6 | 0 | 0 |
| put 1M plain x1 | fill+crc chacha8 | 3577 | 286.2 | 1.000 | 1.00 | 3577 | 150.2 | 0.498 | 439.6 | 0 | 0 |
| put 1M plain x1 | fill+crc xoshiro | 6272 | 163.2 | 1.000 | 1.00 | 6273 | 121.0 | 0.714 | 281.4 | 0 | 0 |
| put 1M plain x1 | fill+crc splitmix | 7423 | 137.9 | 1.000 | 1.00 | 7424 | 120.8 | 0.849 | 256.2 | 0 | 0 |
| put 1M plain x2 | fill+crc aes-ctr | 9793 | 209.1 | 1.000 | 1.00 | 4897 | 119.6 | 0.563 | 327.2 | 0 | 0 |
| put 1M plain x2 | fill+crc chacha8 | 7785 | 263.1 | 1.000 | 1.00 | 3893 | 119.6 | 0.446 | 383.8 | 0 | 0 |
| put 1M plain x2 | fill+crc xoshiro | 12355 | 165.8 | 1.000 | 1.00 | 6178 | 120.0 | 0.717 | 281.6 | 0 | 0 |
| put 1M plain x2 | fill+crc splitmix | 13372 | 153.1 | 1.000 | 1.00 | 6687 | 133.8 | 0.880 | 285.1 | 0 | 0 |
| put 1M plain x4 | fill+crc aes-ctr | 19130 | 214.1 | 1.000 | 1.00 | 4783 | 120.9 | 0.567 | 332.7 | 0 | 0 |
| put 1M plain x4 | fill+crc chacha8 | 15205 | 269.4 | 1.000 | 1.00 | 3801 | 120.3 | 0.450 | 386.9 | 0 | 0 |
| put 1M plain x4 | fill+crc xoshiro | 23961 | 170.9 | 1.000 | 1.00 | 5991 | 122.8 | 0.719 | 276.2 | 0 | 0 |
| put 1M plain x4 | fill+crc splitmix | 26258 | 156.0 | 1.000 | 1.00 | 6565 | 133.9 | 0.876 | 287.9 | 0 | 0 |

### 4. A folder of real files, round 2

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-31-generic · build native (avx2 avx512f vaes aes sha) · crc x86_64-avx512-vpclmulqdq · leg europa native

| cell | side | MiB/s | cpu ms/GiB | device MiB read | from device |
| --- | --- | --- | --- | --- | --- |
| 64K cold | read+sha256 | 740.5 | 879.1 | 2048 | 1.00 |
| 64K cold | read+crc | 1050 | 469.8 | 2048 | 1.00 |
| 64K cold | read | 1063 | 460.6 | 2048 | 1.00 |
| 64K hot | read+sha256 | 1762 | 581.2 | 0 | 0 |
| 64K hot | read+crc | 5798 | 176.6 | 0 | 0 |
| 64K hot | read | 6257 | 163.6 | 0 | 0 |
| 1M cold | read+sha256 | 1167 | 574.0 | 2048 | 1.00 |
| 1M cold | read+crc | 2131 | 175.5 | 2048 | 1.00 |
| 1M cold | read | 2193 | 164.9 | 2048 | 1.00 |
| 1M hot | read+sha256 | 2202 | 464.9 | 0 | 0 |
| 1M hot | read+crc | 14472 | 70.75 | 0 | 0 |
| 1M hot | read | 17420 | 58.78 | 0 | 0 |
| 64M cold | read+sha256 | 1315 | 525.3 | 2048 | 1.00 |
| 64M cold | read+crc | 2606 | 131.1 | 2048 | 1.00 |
| 64M cold | read | 2606 | 119.4 | 2048 | 1.00 |
| 64M hot | read+sha256 | 2270 | 451.0 | 0 | 0 |
| 64M hot | read+crc | 16964 | 60.35 | 0 | 0 |
| 64M hot | read | 21272 | 48.13 | 0 | 0 |

