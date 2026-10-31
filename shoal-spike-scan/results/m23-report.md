
### Big alone

| Figure | Rounds |
| --- | --- |
| MiB a second | 30.2 [27.9, 33.9] (n 8) |
| Zen1 hosts' device KiB a read | 11 [11, 11] (n 8) |
| Zen1 hosts' device busy ms a second | 842 [837, 848] (n 8) |
| Zen1 hosts' node cpu a second | 0.686 [0.68, 0.694] (n 8) |
| Zen1 hosts' node cpu us a partition | 49 [47.8, 52.2] (n 8) |
| Zen1 hosts' partitions a device read | 9.81 [9.76, 9.86] (n 8) |
| Zen1 hosts' partitions a walk read | 10.2 [10.1, 10.2] (n 8) |
| Zen1 hosts' scan MiB a second | 14.3 [13.7, 14.6] (n 8) |
| Zen1 hosts' scans' busy ms a MiB | 5.16 [5.13, 5.24] (n 8) |
| Zen1 hosts' scans' busy ms a second | 74.2 [71.3, 75.1] (n 8) |
| europa's device KiB a read | 12.9 [12.9, 13] (n 8) |
| europa's device busy ms a second | 21.1 [20.3, 25.8] (n 8) |
| europa's node cpu a second | 0.247 [0.242, 0.262] (n 8) |
| europa's node cpu us a partition | 23.8 [16.3, 28.3] (n 8) |
| europa's partitions a device read | 11.7 [11.7, 11.8] (n 8) |
| europa's partitions a walk read | 12.1 [12.1, 12.2] (n 8) |
| europa's scan MiB a second | 9.21 [7.69, 14.7] (n 8) |
| europa's scans' busy ms a MiB | 0.906 [0.88, 0.922] (n 8) |
| europa's scans' busy ms a second | 8.37 [6.96, 13.2] (n 8) |
| page held p99 ms | 0.0245 [0.021, 0.036] (n 8) |
| page paced p50 ms | 60.7 [50.4, 63.2] (n 8) |
| page read p99 ms | 1.99e+03 [1.65e+03, 2.79e+03] (n 8) |
| pages shed a page | 9.35 [8.56, 10] (n 8) |
| partitions a second | 3.05e+04 [2.81e+04, 3.42e+04] (n 8) |

### Keyword alone

| Figure | Rounds |
| --- | --- |
| MiB a second | 5.86 [5.29, 7.06] (n 8) |
| Zen1 hosts' device KiB a read | 4.7 [4.02, 6.48] (n 8) |
| Zen1 hosts' device busy ms a second | 853 [825, 860] (n 8) |
| Zen1 hosts' node cpu a second | 0.663 [0.645, 0.669] (n 8) |
| Zen1 hosts' node cpu us a partition | 753 [694, 889] (n 8) |
| Zen1 hosts' partitions a device read | 1.56 [1.25, 2.18] (n 8) |
| Zen1 hosts' partitions a walk read | 1.63 [1.28, 2.45] (n 8) |
| Zen1 hosts' scan MiB a second | 2.04 [1.88, 2.31] (n 8) |
| Zen1 hosts' scans' busy ms a MiB | 36.2 [30.1, 39.6] (n 8) |
| Zen1 hosts' scans' busy ms a second | 73.8 [69.4, 74.9] (n 8) |
| europa's device KiB a read | 3.65 [3.06, 5.18] (n 8) |
| europa's device busy ms a second | 15.8 [13.9, 16.9] (n 8) |
| europa's node cpu a second | 0.24 [0.231, 0.243] (n 8) |
| europa's node cpu us a partition | 379 [279, 625] (n 8) |
| europa's partitions a device read | 2.29 [1.88, 3.32] (n 8) |
| europa's partitions a walk read | 2.55 [1.99, 4.02] (n 8) |
| europa's scan MiB a second | 0.678 [0.433, 0.95] (n 8) |
| europa's scans' busy ms a MiB | 4.27 [3.15, 5.05] (n 8) |
| europa's scans' busy ms a second | 3.09 [1.37, 3.42] (n 8) |
| page held p99 ms | 0.013 [0.012, 0.016] (n 8) |
| page paced p50 ms | 0.342 [0.001, 1.91] (n 8) |
| page read p99 ms | 1.76e+03 [1.2e+03, 2.57e+03] (n 8) |
| pages shed a page | 3.48 [3.11, 3.57] (n 8) |
| partitions a second | 2.15e+03 [1.93e+03, 2.58e+03] (n 8) |

### cell

| Figure | Rounds |
| --- | --- |
| insert p99 ms, alone | 24.8 [24.3, 26.1] (n 8) |
| read p99 ms, alone | 7.49 [7.21, 7.66] (n 8) |
| reads a second, alone | 2.37e+03 [2.33e+03, 2.43e+03] (n 8) |

### cell alone, the control

| Figure | Rounds |
| --- | --- |
| insert p99 a second, after it and before the wrap | 30.3 [27.2, 33.4] (n 2) |
| insert p99 a second, after it and before the wrap / before | 1.16 [0.975, 1.34] (n 2) |
| insert p99 a second, before the scans' start | 26.5 [25, 27.9] (n 2) |
| read p99 a second, after it and before the wrap | 8 [7.52, 8.48] (n 2) |
| read p99 a second, after it and before the wrap / before | 0.986 [0.895, 1.08] (n 2) |
| read p99 a second, before the scans' start | 8.14 [7.88, 8.41] (n 2) |
| seconds before the wrap past the scans' start | 67 [66, 68] (n 2) |

### cell beside Big

| Figure | Rounds |
| --- | --- |
| Resident evictions in the capture | 0 [0, 0] (n 8) |
| Zen1 hosts' partitions a walk read | 10.2 [10.1, 10.2] (n 8) |
| Zen1 hosts' scan MiB a second | 5.21 [4.51, 5.48] (n 8) |
| Zen1 hosts' scans' busy ms a MiB | 11.1 [10.7, 11.4] (n 8) |
| Zen1 hosts' scans' busy ms a second | 57.6 [50.4, 60.1] (n 8) |
| europa's partitions a walk read | 12.1 [12.1, 12.1] (n 8) |
| europa's scan MiB a second | 3.48 [2.61, 5.65] (n 8) |
| europa's scans' busy ms a MiB | 1.03 [0.959, 1.04] (n 8) |
| europa's scans' busy ms a second | 3.59 [2.69, 5.81] (n 8) |
| event p99 ratio permille | 1.62e+03 [1.56e+03, 1.72e+03] (n 8) |
| insert p99 a second, before | 23.7 [23.3, 24.1] (n 8) |
| insert p99 a second, during before the wrap | 25.4 [24.4, 26.2] (n 8) |
| insert p99 a second, during before the wrap / before | 1.06 [1.03, 1.12] (n 8) |
| insert p99 during / before | 1.62 [1.56, 1.72] (n 8) |
| insert p99 during / mean of before and after | 1.19 [1.12, 1.27] (n 8) |
| insert p99 ms after | 42.9 [40.4, 46.6] (n 8) |
| insert p99 ms before | 24.8 [24.1, 25.6] (n 8) |
| insert p99 ms during | 40.6 [39.3, 42.1] (n 8) |
| inserts a second before | 2.38e+03 [2.33e+03, 2.41e+03] (n 8) |
| inserts a second during | 1.63e+03 [1.58e+03, 1.71e+03] (n 8) |
| page paced p99 ms | 5.55e+03 [4.23e+03, 6.49e+03] (n 8) |
| read p99 a second, before | 5.81 [5.57, 6.37] (n 8) |
| read p99 a second, during before the wrap | 6.3 [6, 6.89] (n 8) |
| read p99 a second, during before the wrap / before | 1.08 [1.02, 1.11] (n 8) |
| read p99 during / before | 1.07 [1.03, 1.08] (n 8) |
| read p99 during / mean of before and after | 1.05 [0.946, 1.13] (n 8) |
| read p99 ms after | 6.2 [5.57, 7.89] (n 8) |
| read p99 ms before | 6.22 [5.98, 6.41] (n 8) |
| read p99 ms during | 6.55 [6.47, 6.73] (n 8) |
| reads a second before | 2.36e+03 [2.32e+03, 2.39e+03] (n 8) |
| reads a second during | 1.62e+03 [1.58e+03, 1.7e+03] (n 8) |
| scan longest step us in the capture | 424 [326, 539] (n 8) |
| scan partitions a second | 1.67e+04 [1.35e+04, 2.04e+04] (n 8) |
| scan steps in the capture | 9.23e+04 [7.85e+04, 1.09e+05] (n 8) |
| scan steps past 100 us in the capture | 201 [158, 268] (n 8) |
| seconds of the scan before the wrap | 61.5 [60, 64] (n 8) |
| whole scan s | 180 [146, 223] (n 8) |

### cell beside Keyword

| Figure | Rounds |
| --- | --- |
| Resident evictions in the capture | 0 [0, 0] (n 8) |
| Zen1 hosts' partitions a walk read | 1.11 [0.948, 1.31] (n 8) |
| Zen1 hosts' scan MiB a second | 0.495 [0.461, 0.568] (n 8) |
| Zen1 hosts' scans' busy ms a MiB | 134 [124, 145] (n 8) |
| Zen1 hosts' scans' busy ms a second | 68.3 [58.7, 75.2] (n 8) |
| europa's partitions a walk read | 1.19 [1.08, 1.45] (n 8) |
| europa's scan MiB a second | 0.162 [0.0933, 0.334] (n 8) |
| europa's scans' busy ms a MiB | 8.1 [7.24, 9.53] (n 8) |
| europa's scans' busy ms a second | 1.54 [0.675, 2.84] (n 8) |
| event p99 ratio permille | 1.48e+03 [1.3e+03, 1.55e+03] (n 8) |
| insert p99 a second, before | 25.9 [24.6, 30] (n 8) |
| insert p99 a second, during before the wrap | 32.7 [28.1, 37.4] (n 8) |
| insert p99 a second, during before the wrap / before | 1.23 [1.05, 1.36] (n 8) |
| insert p99 during / before | 1.48 [1.3, 1.55] (n 8) |
| insert p99 during / mean of before and after | 1.03 [0.982, 1.18] (n 8) |
| insert p99 ms after | 49.7 [43.6, 55.4] (n 8) |
| insert p99 ms before | 27.3 [26, 33] (n 8) |
| insert p99 ms during | 40.6 [38.6, 42.7] (n 8) |
| inserts a second before | 2.29e+03 [2.12e+03, 2.35e+03] (n 8) |
| inserts a second during | 1.76e+03 [1.67e+03, 1.84e+03] (n 8) |
| page paced p99 ms | 6.57e+03 [5.64e+03, 7.83e+03] (n 8) |
| read p99 a second, before | 8.47 [7.83, 9.18] (n 8) |
| read p99 a second, during before the wrap | 7.39 [7.26, 8.35] (n 8) |
| read p99 a second, during before the wrap / before | 0.87 [0.803, 1.03] (n 8) |
| read p99 during / before | 0.872 [0.817, 0.973] (n 8) |
| read p99 during / mean of before and after | 0.95 [0.924, 1.02] (n 8) |
| read p99 ms after | 7.5 [6.63, 8.89] (n 8) |
| read p99 ms before | 9.36 [8.41, 10.1] (n 8) |
| read p99 ms during | 8.11 [7.85, 8.58] (n 8) |
| reads a second before | 2.28e+03 [2.11e+03, 2.34e+03] (n 8) |
| reads a second during | 1.75e+03 [1.67e+03, 1.83e+03] (n 8) |
| scan longest step us in the capture | 368 [224, 489] (n 8) |
| scan partitions a second | 599 [450, 636] (n 8) |
| scan steps in the capture | 1.04e+04 [9.7e+03, 1.47e+04] (n 8) |
| scan steps past 100 us in the capture | 18.5 [6, 24] (n 8) |
| seconds of the scan before the wrap | 68 [67, 75] (n 8) |
| whole scan s | 94.1 [88.4, 125] (n 8) |

### Big alone before and after an hour's churn

| Figure | 10mib afterchurn | 10mib scan | 40mib afterchurn | 40mib scan |
| --- | --- | --- | --- | --- |
| MiB a second | 53.4 [53.4, 53.4] (n 1) | 50.2 [50.2, 50.2] (n 1) | 60.1 [60.1, 60.1] (n 1) | 98.5 [98.5, 98.5] (n 1) |
| Zen1 hosts' device KiB a read | 13 [13, 13] (n 1) | 11.7 [11.7, 11.7] (n 1) | 28.4 [28.4, 28.4] (n 1) | 40.9 [40.9, 40.9] (n 1) |
| Zen1 hosts' device busy ms a second | 87.2 [87.2, 87.2] (n 1) | 79.2 [79.2, 79.2] (n 1) | 185 [185, 185] (n 1) | 84.4 [84.4, 84.4] (n 1) |
| Zen1 hosts' idle node cpu a second | 0.248 [0.248, 0.248] (n 1) | 0.241 [0.241, 0.241] (n 1) | 0.26 [0.26, 0.26] (n 1) | 0.254 [0.254, 0.254] (n 1) |
| Zen1 hosts' node cpu a second | 0.418 [0.418, 0.418] (n 1) | 0.435 [0.435, 0.435] (n 1) | 0.402 [0.402, 0.402] (n 1) | 0.44 [0.44, 0.44] (n 1) |
| Zen1 hosts' node cpu us a partition | 22 [22, 22] (n 1) | 22.6 [22.6, 22.6] (n 1) | 16.8 [16.8, 16.8] (n 1) | 12.2 [12.2, 12.2] (n 1) |
| Zen1 hosts' node cpu us a partition above idle | 8.94 [8.94, 8.94] (n 1) | 10.1 [10.1, 10.1] (n 1) | 5.94 [5.94, 5.94] (n 1) | 5.16 [5.16, 5.16] (n 1) |
| Zen1 hosts' partitions a device read | 10.6 [10.6, 10.6] (n 1) | 10.5 [10.5, 10.5] (n 1) | 23.6 [23.6, 23.6] (n 1) | 40 [40, 40] (n 1) |
| Zen1 hosts' partitions a walk read | 11.1 [11.1, 11.1] (n 1) | 10.8 [10.8, 10.8] (n 1) | 24.5 [24.5, 24.5] (n 1) | 43.1 [43.1, 43.1] (n 1) |
| Zen1 hosts' scan MiB a second | 21.6 [21.6, 21.6] (n 1) | 19.8 [19.8, 19.8] (n 1) | 26.8 [26.8, 26.8] (n 1) | 35 [35, 35] (n 1) |
| Zen1 hosts' scans' busy ms a MiB | 3.47 [3.47, 3.47] (n 1) | 3.81 [3.81, 3.81] (n 1) | 2.38 [2.38, 2.38] (n 1) | 1.95 [1.95, 1.95] (n 1) |
| Zen1 hosts' scans' busy ms a second | 75 [75, 75] (n 1) | 75.4 [75.4, 75.4] (n 1) | 63.8 [63.8, 63.8] (n 1) | 68.4 [68.4, 68.4] (n 1) |
| europa's device KiB a read | 13.8 [13.8, 13.8] (n 1) | 13.8 [13.8, 13.8] (n 1) | 38.2 [38.2, 38.2] (n 1) | 48.7 [48.7, 48.7] (n 1) |
| europa's device busy ms a second | 16.3 [16.3, 16.3] (n 1) | 13.6 [13.6, 13.6] (n 1) | 11.5 [11.5, 11.5] (n 1) | 16.8 [16.8, 16.8] (n 1) |
| europa's idle node cpu a second | 0.134 [0.134, 0.134] (n 1) | 0.141 [0.141, 0.141] (n 1) | 0.151 [0.151, 0.151] (n 1) | 0.148 [0.148, 0.148] (n 1) |
| europa's node cpu a second | 0.179 [0.179, 0.179] (n 1) | 0.176 [0.176, 0.176] (n 1) | 0.174 [0.174, 0.174] (n 1) | 0.187 [0.187, 0.187] (n 1) |
| europa's node cpu us a partition | 8.9 [8.9, 8.9] (n 1) | 9.62 [9.62, 9.62] (n 1) | 8.51 [8.51, 8.51] (n 1) | 5.76 [5.76, 5.76] (n 1) |
| europa's node cpu us a partition above idle | 2.25 [2.25, 2.25] (n 1) | 1.94 [1.94, 1.94] (n 1) | 1.12 [1.12, 1.12] (n 1) | 1.19 [1.19, 1.19] (n 1) |
| europa's partitions a device read | 11.3 [11.3, 11.3] (n 1) | 12.6 [12.6, 12.6] (n 1) | 34 [34, 34] (n 1) | 45.7 [45.7, 45.7] (n 1) |
| europa's partitions a walk read | 11.7 [11.7, 11.7] (n 1) | 12.9 [12.9, 12.9] (n 1) | 35.6 [35.6, 35.6] (n 1) | 51.7 [51.7, 51.7] (n 1) |
| europa's scan MiB a second | 22.3 [22.3, 22.3] (n 1) | 19 [19, 19] (n 1) | 18.5 [18.5, 18.5] (n 1) | 31.4 [31.4, 31.4] (n 1) |
| europa's scans' busy ms a MiB | 0.864 [0.864, 0.864] (n 1) | 0.816 [0.816, 0.816] (n 1) | 0.557 [0.557, 0.557] (n 1) | 0.512 [0.512, 0.512] (n 1) |
| europa's scans' busy ms a second | 19.2 [19.2, 19.2] (n 1) | 15.5 [15.5, 15.5] (n 1) | 10.3 [10.3, 10.3] (n 1) | 16.1 [16.1, 16.1] (n 1) |
| page held p99 ms | 0.015 [0.015, 0.015] (n 1) | 0.02 [0.02, 0.02] (n 1) | 0.202 [0.202, 0.202] (n 1) | 0.197 [0.197, 0.197] (n 1) |
| page paced p50 ms | 37.6 [37.6, 37.6] (n 1) | 35.8 [35.8, 35.8] (n 1) | 23.7 [23.7, 23.7] (n 1) | 13 [13, 13] (n 1) |
| page read p99 ms | 2.92e+03 [2.92e+03, 2.92e+03] (n 1) | 1.17e+03 [1.17e+03, 1.17e+03] (n 1) | 1.34e+03 [1.34e+03, 1.34e+03] (n 1) | 2.13e+03 [2.13e+03, 2.13e+03] (n 1) |
| pages shed a page | 6.69 [6.69, 6.69] (n 1) | 6.65 [6.65, 6.65] (n 1) | 6.23 [6.23, 6.23] (n 1) | 3.99 [3.99, 3.99] (n 1) |
| partitions a second | 5.39e+04 [5.39e+04, 5.39e+04] (n 1) | 5.06e+04 [5.06e+04, 5.06e+04] (n 1) | 6.06e+04 [6.06e+04, 6.06e+04] (n 1) | 9.93e+04 [9.93e+04, 9.93e+04] (n 1) |

### Scan steps past 100 us, by arm and member

Each cell is a member's KiB a resident partition handed out, then its steps past 100 us of its steps.

| Round | Arm | europa | hyperion | titan |
| --- | --- | --- | --- | --- |
| 1 | big rw50/b1/scan | 1.0 KiB, 0 of 10555 | 1.0 KiB, 5 of 7780 | 1.1 KiB, 5 of 9454 |
| 1 | big scan/Big/none | 1.0 KiB, 3 of 10468 | 1.0 KiB, 2 of 7767 | 1.0 KiB, 7 of 8908 |
| 1 | big scan/Keyword/none | 20.4 KiB, 106 of 8679 | 6.8 KiB, 19 of 11704 | 6.2 KiB, 26 of 13064 |
| 1 | keyword rw50/b1/scan | 11.7 KiB, 0 of 2522 | 0.7 KiB, 1 of 3163 | 5.8 KiB, 17 of 9032 |
| 2 | big rw50/b1/scan | 1.0 KiB, 1 of 9896 | 1.0 KiB, 2 of 6669 | 1.0 KiB, 5 of 12443 |
| 2 | big scan/Big/none | 1.0 KiB, 0 of 9865 | 1.0 KiB, 4 of 5555 | 1.0 KiB, 7 of 12294 |
| 2 | big scan/Keyword/none | 22.9 KiB, 162 of 9101 | 1.0 KiB, 5 of 8922 | 0.9 KiB, 1 of 8221 |
| 2 | keyword rw50/b1/scan | 15.2 KiB, 4 of 2633 | 2.5 KiB, 3 of 3759 | 3.5 KiB, 14 of 3748 |
| 3 | big rw50/b1/scan | 1.0 KiB, 0 of 8197 | 1.2 KiB, 2 of 6312 | 1.0 KiB, 1 of 11082 |
| 3 | big scan/Big/none | 1.0 KiB, 3 of 8160 | 1.0 KiB, 3 of 5812 | 1.0 KiB, 5 of 8341 |
| 3 | big scan/Keyword/none | 22.0 KiB, 139 of 8656 | 6.6 KiB, 25 of 11693 | 5.5 KiB, 37 of 12831 |
| 3 | keyword rw50/b1/scan | 15.2 KiB, 11 of 2586 | 4.6 KiB, 10 of 3750 | 1.2 KiB, 3 of 3723 |
| 4 | big rw50/b1/scan | 1.0 KiB, 1 of 10716 | 1.0 KiB, 3 of 8436 | 1.0 KiB, 12 of 14828 |
| 4 | big scan/Big/none | 1.0 KiB, 2 of 11425 | 1.0 KiB, 4 of 8378 | 1.0 KiB, 9 of 14613 |
| 4 | big scan/Keyword/none | 18.7 KiB, 130 of 6134 | 9.1 KiB, 44 of 21417 | 5.4 KiB, 48 of 13371 |
| 4 | keyword rw50/b1/scan | 12.1 KiB, 1 of 1644 | 3.8 KiB, 12 of 4783 | 2.6 KiB, 6 of 4154 |
| 5 | big rw50/b1/scan | 1.0 KiB, 0 of 6839 | 1.1 KiB, 4 of 12861 | 1.0 KiB, 5 of 10591 |
| 5 | big scan/Big/none | 1.0 KiB, 1 of 6803 | 1.0 KiB, 5 of 12098 | 1.0 KiB, 5 of 8335 |
| 5 | big scan/Keyword/none | 23.0 KiB, 192 of 10682 | 7.5 KiB, 31 of 13987 | 5.6 KiB, 25 of 14077 |
| 5 | keyword rw50/b1/scan | 15.0 KiB, 3 of 2388 | 2.6 KiB, 6 of 3709 | 0.6 KiB, 1 of 3605 |
| 6 | big rw50/b1/scan | 1.0 KiB, 0 of 8405 | 1.0 KiB, 10 of 12568 | 1.0 KiB, 3 of 12501 |
| 6 | big scan/Big/none | 1.0 KiB, 0 of 8332 | 1.0 KiB, 8 of 11161 | 1.0 KiB, 3 of 11976 |
| 6 | big scan/Keyword/none | 20.3 KiB, 208 of 11983 | 6.2 KiB, 26 of 12884 | 1.6 KiB, 5 of 11419 |
| 6 | keyword rw50/b1/scan | 11.2 KiB, 4 of 2726 | 5.8 KiB, 17 of 4237 | 0.7 KiB, 1 of 3685 |
| 7 | big rw50/b1/scan | 1.0 KiB, 0 of 6907 | 1.0 KiB, 8 of 16082 | 1.0 KiB, 5 of 16465 |
| 7 | big scan/Big/none | 1.0 KiB, 0 of 6807 | 1.0 KiB, 4 of 13419 | 1.0 KiB, 8 of 14522 |
| 7 | big scan/Keyword/none | 19.0 KiB, 90 of 5313 | 9.8 KiB, 40 of 15205 | 0.7 KiB, 3 of 11392 |
| 7 | keyword rw50/b1/scan | 8.8 KiB, 1 of 1553 | 0.8 KiB, 1 of 3746 | 2.3 KiB, 4 of 4430 |
| 8 | big rw50/b1/scan | 1.0 KiB, 1 of 7959 | 1.0 KiB, 3 of 7646 | 1.0 KiB, 9 of 12406 |
| 8 | big scan/Big/none | 1.0 KiB, 1 of 7908 | 1.0 KiB, 2 of 7550 | 1.0 KiB, 5 of 8650 |
| 8 | big scan/Keyword/none | 21.2 KiB, 140 of 7386 | 3.2 KiB, 6 of 10651 | 0.6 KiB, 0 of 8388 |
| 8 | keyword rw50/b1/scan | 13.2 KiB, 1 of 2093 | 2.4 KiB, 6 of 4787 | 2.7 KiB, 10 of 3681 |

### The drivers before and after

| Event | Figure | Before | After | Disjoint |
| --- | --- | --- | --- | --- |
| deep_scrub | during window s | 601 [601, 601] (n 2) | 601 [601, 601] (n 2) | no |
| deep_scrub | get p99 ms before | 30.1 [28.4, 31.7] (n 2) | 27.6 [26.8, 28.5] (n 2) | no |
| deep_scrub | get p99 ms during | 28.2 [27.1, 29.3] (n 2) | 28 [27.9, 28.2] (n 2) | no |
| deep_scrub | gets a second during | 21.8 [21.4, 22.1] (n 2) | 21.9 [21.9, 21.9] (n 2) | no |
| deep_scrub | put p99 ms before | 90.7 [90.6, 90.8] (n 2) | 90.2 [90, 90.3] (n 2) | yes |
| deep_scrub | put p99 ms during | 161 [153, 169] (n 2) | 163 [162, 165] (n 2) | no |
| deep_scrub | puts a second during | 21.8 [21.4, 22.2] (n 2) | 21.9 [21.9, 22] (n 2) | no |
| device_drain | event done after s | 396 [393, 400] (n 2) | 385 [383, 387] (n 2) | yes |
| device_drain | get p99 ms before | 25.1 [23.2, 26.9] (n 2) | 26 [24.8, 27.1] (n 2) | no |
| device_drain | get p99 ms during | 27.7 [27.4, 28.1] (n 2) | 28.2 [28, 28.3] (n 2) | no |
| device_drain | gets a second during | 21.7 [21.3, 22.1] (n 2) | 22.1 [22.1, 22.1] (n 2) | no |
| device_drain | put p99 ms before | 89.2 [87.8, 90.6] (n 2) | 89.4 [88.3, 90.5] (n 2) | no |
| device_drain | put p99 ms during | 166 [161, 171] (n 2) | 152 [151, 153] (n 2) | yes |
| device_drain | puts a second during | 21.6 [21.2, 22] (n 2) | 22.1 [22.1, 22.1] (n 2) | yes |
| device_fail | event done after s | 166 [26, 306] (n 2) | 306 [305, 306] (n 2) | no |
| device_fail | get p99 ms before | 31.4 [31.4, 31.4] (n 2) | 30.1 [28.6, 31.6] (n 2) | no |
| device_fail | get p99 ms during | 42.3 [35.2, 49.3] (n 2) | 32.5 [31.1, 34] (n 2) | yes |
| device_fail | gets a second during | 22.2 [22.1, 22.4] (n 2) | 22.5 [22.4, 22.6] (n 2) | yes |
| device_fail | put p99 ms before | 90 [89.9, 90] (n 2) | 94.9 [93.8, 96.1] (n 2) | yes |
| device_fail | put p99 ms during | 144 [127, 161] (n 2) | 129 [126, 131] (n 2) | no |
| device_fail | puts a second during | 21.3 [20.7, 21.9] (n 2) | 22.1 [22, 22.1] (n 2) | yes |
| none | get p99 ms measured | 29.9 [29.1, 30.7] (n 2) | 29 [28.5, 29.4] (n 2) | no |
| none | gets a second measured | 28.8 [27.9, 29.7] (n 2) | 29.2 [29.1, 29.3] (n 2) | no |
| none | put p99 ms measured | 109 [108, 111] (n 2) | 107 [104, 110] (n 2) | no |
| none | puts a second measured | 26.8 [25.9, 27.6] (n 2) | 27 [26.9, 27.2] (n 2) | no |

### The exit's clauses, on the medians

| Clause | Measured | Held |
| --- | --- | --- |
| the cell's read p99 beside a scan of Big | 6.22 -> 6.55 ms | yes |
| (beside its drift) the cell's read p99 beside a scan of Big, against before and after | 6.21 -> 6.55 ms | yes |
| (before the cell's wrap) the cell's read p99 a second beside a scan of Big | 5.81 -> 6.30 ms | yes |
| the cell's insert p99 beside a scan of Big | 24.76 -> 40.61 ms | no |
| (beside its drift) the cell's insert p99 beside a scan of Big, against before and after | 33.84 -> 40.61 ms | no |
| (before the cell's wrap) the cell's insert p99 a second beside a scan of Big | 23.74 -> 25.38 ms | yes |
| no eviction pass by the process's memory, the Big captures | 0 at most | yes |
| no scan step over 100 us, the Big captures | 268 at most | no |
| the cell's read p99 beside a scan of Keyword | 9.36 -> 8.11 ms | yes |
| (beside its drift) the cell's read p99 beside a scan of Keyword, against before and after | 8.43 -> 8.11 ms | yes |
| (before the cell's wrap) the cell's read p99 a second beside a scan of Keyword | 8.47 -> 7.39 ms | yes |
| the cell's insert p99 beside a scan of Keyword | 27.32 -> 40.59 ms | no |
| (beside its drift) the cell's insert p99 beside a scan of Keyword, against before and after | 38.50 -> 40.59 ms | yes |
| (before the cell's wrap) the cell's insert p99 a second beside a scan of Keyword | 25.94 -> 32.72 ms | no |
| no eviction pass by the process's memory, the Keyword captures | 0 at most | yes |
| no scan step over 100 us, the Keyword captures | 24 at most | no |
| at least ten partitions a device read, Zen1 hosts, Big loaded once and scanned alone, 10 MiB segments | 10.5 | yes |
| at most 15 us of a Zen1 host's cpu a partition, Big loaded once and scanned alone, 10 MiB segments | 22.6 | no |
| the same above the idle node's cpu, Big loaded once and scanned alone, 10 MiB segments | 10.1 | yes |
| at least ten partitions a device read, Zen1 hosts, Big loaded once and scanned alone, 40 MiB segments | 40 | yes |
| at most 15 us of a Zen1 host's cpu a partition, Big loaded once and scanned alone, 40 MiB segments | 12.2 | yes |
| the same above the idle node's cpu, Big loaded once and scanned alone, 40 MiB segments | 5.16 | yes |
| at least ten partitions a device read, Zen1 hosts, the rounds' Big alone, the paced stream beside it | 9.81 | no |
| at most 15 us of a Zen1 host's cpu a partition, the rounds' Big alone, the paced stream beside it | 49 | no |
| at least nine partitions a device read after an hour's churn, 10mib segments | 10.6 | yes |
| at least nine partitions a device read after an hour's churn, 40mib segments | 23.6 | yes |
