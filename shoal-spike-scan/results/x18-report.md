
### c32t2: a ceiling of 32 MiB a second, a trickle of 2

| Figure | SSD | Disk |
| --- | --- | --- |
| cell read p99 ms before | 5.79 [5.69, 5.92] (n 4) | 44.8 [43.8, 54.9] (n 4) |
| cell read p99 ms during | 6.6 [6.35, 6.75] (n 4) | 196 [146, 204] (n 4) |
| cell read p99 during / before | 1.14 [1.07, 1.19] (n 4) | 3.96 [3.28, 4.63] (n 4) |
| cell insert p99 ms before | 24.7 [24.6, 25.4] (n 4) | 65.4 [55.6, 92.7] (n 4) |
| cell insert p99 ms during | 26.5 [26.1, 27.3] (n 4) | 216 [163, 292] (n 4) |
| cell insert p99 during / before | 1.07 [1.06, 1.09] (n 4) | 3.06 [2.23, 5.25] (n 4) |
| scan MiB a second during | 78.5 [70.8, 85] (n 4) | 4.73 [4.13, 5.13] (n 4) |
| scan partitions a second during | 7.92e+04 [7.14e+04, 8.57e+04] (n 4) | 4.77e+03 [4.17e+03, 5.17e+03] (n 4) |
| page paced p99 ms during | 629 [614, 700] (n 4) | 349 [279, 387] (n 4) |
| page read p99 ms during | 645 [631, 713] (n 4) | 811 [795, 844] (n 4) |
| europa's scan MiB a second during | 19.7 [15, 29.3] (n 4) | 1.1 [1.04, 1.25] (n 4) |
| titan's scan MiB a second during | 27.4 [25.2, 30.6] (n 4) | 1.85 [1.51, 2.18] (n 4) |
| hyperion's scan MiB a second during | 30.3 [27.3, 30.7] (n 4) | 1.73 [1.56, 1.82] (n 4) |
| europa's share at its ceiling | 0.591 [0.433, 0.909] (n 4) | 0 [0, 0] (n 4) |
| titan's share at its ceiling | 0.846 [0.772, 0.953] (n 4) | 0 [0, 0.00589] (n 4) |
| hyperion's share at its ceiling | 0.944 [0.843, 0.957] (n 4) | 0 [0, 0] (n 4) |

### c64t2: a ceiling of 64 MiB a second, a trickle of 2

| Figure | SSD | Disk |
| --- | --- | --- |
| cell read p99 ms before | 5.92 [5.77, 6.06] (n 4) | 1.08e+03 [155, 2.22e+03] (n 4) |
| cell read p99 ms during | 7 [6.75, 7.5] (n 4) | 231 [189, 251] (n 4) |
| cell read p99 during / before | 1.19 [1.11, 1.27] (n 4) | 0.216 [0.0851, 1.61] (n 4) |
| cell insert p99 ms before | 25.2 [24.9, 25.6] (n 4) | 2.46e+03 [217, 4.1e+03] (n 4) |
| cell insert p99 ms during | 28.1 [27.9, 28.2] (n 4) | 279 [171, 445] (n 4) |
| cell insert p99 during / before | 1.12 [1.1, 1.12] (n 4) | 0.161 [0.0417, 0.989] (n 4) |
| scan MiB a second during | 153 [135, 165] (n 4) | 3.15 [2.61, 4.13] (n 4) |
| scan partitions a second during | 1.55e+05 [1.36e+05, 1.67e+05] (n 4) | 3.17e+03 [2.63e+03, 4.17e+03] (n 4) |
| page paced p99 ms during | 343 [330, 371] (n 4) | 381 [295, 467] (n 4) |
| page read p99 ms during | 354 [344, 379] (n 4) | 915 [836, 1.07e+03] (n 4) |
| europa's scan MiB a second during | 38.7 [28.6, 56.9] (n 4) | 0.762 [0.604, 1.01] (n 4) |
| titan's scan MiB a second during | 53.9 [48, 58.7] (n 4) | 1.22 [0.991, 1.75] (n 4) |
| hyperion's scan MiB a second during | 58.8 [52, 60.7] (n 4) | 1.17 [1.01, 1.37] (n 4) |
| europa's share at its ceiling | 0.592 [0.429, 0.886] (n 4) | 0 [0, 0] (n 4) |
| titan's share at its ceiling | 0.836 [0.743, 0.915] (n 4) | 0 [0, 0] (n 4) |
| hyperion's share at its ceiling | 0.916 [0.806, 0.947] (n 4) | 0 [0, 0] (n 4) |

### c128t2: a ceiling of 128 MiB a second, a trickle of 2

| Figure | SSD | Disk |
| --- | --- | --- |
| cell read p99 ms before | 6.03 [5.3, 6.3] (n 4) | 258 [58.3, 378] (n 4) |
| cell read p99 ms during | 10 [6.65, 10.9] (n 4) | 293 [182, 389] (n 4) |
| cell read p99 during / before | 1.65 [1.26, 1.75] (n 4) | 1.32 [0.541, 6.67] (n 4) |
| cell insert p99 ms before | 25 [23.3, 25.6] (n 4) | 324 [30.3, 505] (n 4) |
| cell insert p99 ms during | 33.9 [26.2, 41.6] (n 4) | 286 [88, 466] (n 4) |
| cell insert p99 during / before | 1.35 [1.13, 1.63] (n 4) | 1.4 [0.174, 15.4] (n 4) |
| scan MiB a second during | 284 [248, 298] (n 4) | 3.44 [2.36, 3.92] (n 4) |
| scan partitions a second during | 2.86e+05 [2.5e+05, 3e+05] (n 4) | 3.47e+03 [2.38e+03, 3.95e+03] (n 4) |
| page paced p99 ms during | 130 [127, 133] (n 4) | 414 [356, 500] (n 4) |
| page read p99 ms during | 143 [139, 148] (n 4) | 1.12e+03 [795, 1.79e+03] (n 4) |
| europa's scan MiB a second during | 69.9 [55, 102] (n 4) | 0.836 [0.547, 0.957] (n 4) |
| titan's scan MiB a second during | 99 [89, 106] (n 4) | 1.33 [0.897, 1.66] (n 4) |
| hyperion's scan MiB a second during | 108 [91, 117] (n 4) | 1.25 [0.918, 1.36] (n 4) |
| europa's share at its ceiling | 0.539 [0.421, 0.797] (n 4) | 0 [0, 0] (n 4) |
| titan's share at its ceiling | 0.77 [0.691, 0.823] (n 4) | 0 [0, 0] (n 4) |
| hyperion's share at its ceiling | 0.839 [0.706, 0.914] (n 4) | 0 [0, 0] (n 4) |

### c256t2: a ceiling of 256 MiB a second, a trickle of 2

| Figure | SSD | Disk |
| --- | --- | --- |
| cell read p99 ms before | 5.79 [5.21, 6.19] (n 4) | 101 [87.3, 336] (n 4) |
| cell read p99 ms during | 8.58 [8.36, 13.1] (n 4) | 296 [289, 338] (n 4) |
| cell read p99 during / before | 1.6 [1.38, 2.12] (n 4) | 2.9 [1.01, 3.39] (n 4) |
| cell insert p99 ms before | 24.1 [22.8, 26.3] (n 4) | 82.8 [57.4, 630] (n 4) |
| cell insert p99 ms during | 33 [30.2, 42.5] (n 4) | 303 [284, 798] (n 4) |
| cell insert p99 during / before | 1.38 [1.31, 1.62] (n 4) | 3.46 [1.27, 5.58] (n 4) |
| scan MiB a second during | 425 [372, 496] (n 4) | 3.86 [2.16, 4.38] (n 4) |
| scan partitions a second during | 4.29e+05 [3.75e+05, 5e+05] (n 4) | 3.89e+03 [2.17e+03, 4.41e+03] (n 4) |
| page paced p99 ms during | 47 [44, 49.2] (n 4) | 327 [238, 401] (n 4) |
| page read p99 ms during | 62.2 [60.9, 65.5] (n 4) | 946 [647, 1.84e+03] (n 4) |
| europa's scan MiB a second during | 114 [82.6, 146] (n 4) | 0.892 [0.552, 1.07] (n 4) |
| titan's scan MiB a second during | 152 [127, 176] (n 4) | 1.51 [0.79, 1.86] (n 4) |
| hyperion's scan MiB a second during | 167 [136, 184] (n 4) | 1.4 [0.814, 1.56] (n 4) |
| europa's share at its ceiling | 0.44 [0.317, 0.568] (n 4) | 0 [0, 0] (n 4) |
| titan's share at its ceiling | 0.59 [0.493, 0.685] (n 4) | 0 [0, 0] (n 4) |
| hyperion's share at its ceiling | 0.649 [0.529, 0.717] (n 4) | 0 [0, 0] (n 4) |

### c64t64: a ceiling of 64 MiB a second, a trickle of 64

| Figure | SSD | Disk |
| --- | --- | --- |
| cell read p99 ms before | 5.73 [5.43, 6.09] (n 4) | 250 [140, 380] (n 4) |
| cell read p99 ms during | 6.92 [6.69, 7.74] (n 4) | 322 [252, 559] (n 4) |
| cell read p99 during / before | 1.23 [1.19, 1.27] (n 4) | 1.3 [0.664, 3.98] (n 4) |
| cell insert p99 ms before | 24.4 [22.4, 25.7] (n 4) | 239 [85.4, 577] (n 4) |
| cell insert p99 ms during | 26.7 [24.9, 28.4] (n 4) | 162 [37.5, 1.21e+03] (n 4) |
| cell insert p99 during / before | 1.11 [1.07, 1.12] (n 4) | 0.575 [0.302, 4.52] (n 4) |
| scan MiB a second during | 149 [135, 165] (n 4) | 4.34 [3.46, 8.75] (n 4) |
| scan partitions a second during | 1.5e+05 [1.36e+05, 1.67e+05] (n 4) | 4.38e+03 [3.49e+03, 8.82e+03] (n 4) |
| page paced p99 ms during | 344 [307, 354] (n 4) | 0.001 [0.001, 0.001] (n 4) |
| page read p99 ms during | 355 [317, 367] (n 4) | 670 [553, 1.87e+03] (n 4) |
| europa's scan MiB a second during | 37.5 [28.6, 56.9] (n 4) | 1.02 [0.86, 2.14] (n 4) |
| titan's scan MiB a second during | 52.5 [48, 58.3] (n 4) | 1.69 [1.27, 3.71] (n 4) |
| hyperion's scan MiB a second during | 58 [52, 58.9] (n 4) | 1.65 [1.31, 2.9] (n 4) |

### c256t256: a ceiling of 256 MiB a second, a trickle of 256

| Figure | SSD | Disk |
| --- | --- | --- |
| cell read p99 ms before | 5.96 [5.4, 6.27] (n 4) | 68.8 [58, 255] (n 4) |
| cell read p99 ms during | 8.11 [7.55, 11.8] (n 4) | 597 [238, 702] (n 4) |
| cell read p99 during / before | 1.41 [1.31, 1.88] (n 4) | 5.88 [2.76, 11.7] (n 4) |
| cell insert p99 ms before | 24.2 [22.8, 25.5] (n 4) | 50 [26.7, 346] (n 4) |
| cell insert p99 ms during | 30.3 [28.2, 36.4] (n 4) | 226 [177, 726] (n 4) |
| cell insert p99 during / before | 1.26 [1.23, 1.43] (n 4) | 3.81 [2.09, 10.1] (n 4) |
| scan MiB a second during | 425 [372, 595] (n 4) | 5.8 [3.46, 7.83] (n 4) |
| scan partitions a second during | 4.29e+05 [3.75e+05, 6e+05] (n 4) | 5.85e+03 [3.49e+03, 7.89e+03] (n 4) |
| page paced p99 ms during | 46.1 [40.2, 52.5] (n 4) | 0.001 [0.001, 0.001] (n 4) |
| page read p99 ms during | 58.6 [55.6, 59.9] (n 4) | 653 [545, 1.77e+03] (n 4) |
| europa's scan MiB a second during | 118 [82.6, 165] (n 4) | 1.34 [0.886, 1.91] (n 4) |
| titan's scan MiB a second during | 152 [127, 211] (n 4) | 2.26 [1.27, 3.32] (n 4) |
| hyperion's scan MiB a second during | 168 [136, 218] (n 4) | 2.2 [1.31, 2.59] (n 4) |

### c128t1: a ceiling of 128 MiB a second, a trickle of 1

| Figure | SSD | Disk |
| --- | --- | --- |
| cell read p99 ms before | 5.81 [5.38, 6.2] (n 4) | 132 [106, 275] (n 4) |
| cell read p99 ms during | 7.81 [7.75, 9.78] (n 4) | 238 [182, 257] (n 4) |
| cell read p99 during / before | 1.42 [1.29, 1.58] (n 4) | 1.92 [0.66, 2.2] (n 4) |
| cell insert p99 ms before | 24.3 [22.9, 26] (n 4) | 164 [135, 307] (n 4) |
| cell insert p99 ms during | 28.2 [26.7, 35.8] (n 4) | 275 [64.4, 293] (n 4) |
| cell insert p99 during / before | 1.19 [1.11, 1.38] (n 4) | 1.73 [0.209, 2.08] (n 4) |
| scan MiB a second during | 284 [248, 298] (n 4) | 2.32 [1.86, 2.98] (n 4) |
| scan partitions a second during | 2.86e+05 [2.5e+05, 3e+05] (n 4) | 2.34e+03 [1.88e+03, 3e+03] (n 4) |
| page paced p99 ms during | 132 [123, 137] (n 4) | 780 [721, 1.07e+03] (n 4) |
| page read p99 ms during | 143 [134, 153] (n 4) | 1.51e+03 [1.11e+03, 2.64e+03] (n 4) |
| europa's scan MiB a second during | 71.3 [52.4, 102] (n 4) | 0.57 [0.43, 0.728] (n 4) |
| titan's scan MiB a second during | 97.3 [88.1, 111] (n 4) | 0.865 [0.749, 1.26] (n 4) |
| hyperion's scan MiB a second during | 107 [99.2, 109] (n 4) | 0.889 [0.681, 0.985] (n 4) |
| europa's share at its ceiling | 0.554 [0.405, 0.799] (n 4) | 0 [0, 0] (n 4) |
| titan's share at its ceiling | 0.759 [0.686, 0.868] (n 4) | 0 [0, 0.00207] (n 4) |
| hyperion's share at its ceiling | 0.833 [0.773, 0.852] (n 4) | 0 [0, 0] (n 4) |

### c128t4: a ceiling of 128 MiB a second, a trickle of 4

| Figure | SSD | Disk |
| --- | --- | --- |
| cell read p99 ms before | 5.89 [5.28, 6.16] (n 4) | 152 [92.1, 183] (n 4) |
| cell read p99 ms during | 8.09 [7.46, 9.18] (n 4) | 338 [219, 470] (n 4) |
| cell read p99 during / before | 1.36 [1.24, 1.74] (n 4) | 2.54 [1.2, 4.01] (n 4) |
| cell insert p99 ms before | 24 [22.7, 25.5] (n 4) | 97.6 [71.9, 239] (n 4) |
| cell insert p99 ms during | 28.5 [26.2, 30.7] (n 4) | 252 [119, 932] (n 4) |
| cell insert p99 during / before | 1.14 [1.12, 1.36] (n 4) | 2.61 [0.768, 8.85] (n 4) |
| scan MiB a second during | 270 [248, 298] (n 4) | 3.81 [3.81, 5.13] (n 4) |
| scan partitions a second during | 2.73e+05 [2.5e+05, 3e+05] (n 4) | 3.85e+03 [3.85e+03, 5.17e+03] (n 4) |
| page paced p99 ms during | 129 [125, 132] (n 4) | 191 [81.4, 250] (n 4) |
| page read p99 ms during | 142 [139, 143] (n 4) | 796 [444, 1.55e+03] (n 4) |
| europa's scan MiB a second during | 71.3 [52.4, 93.1] (n 4) | 0.955 [0.883, 1.19] (n 4) |
| titan's scan MiB a second during | 96.9 [80.9, 111] (n 4) | 1.53 [1.4, 2.06] (n 4) |
| hyperion's scan MiB a second during | 103 [96.4, 109] (n 4) | 1.46 [1.26, 1.88] (n 4) |
| europa's share at its ceiling | 0.543 [0.391, 0.719] (n 4) | 0 [0, 0] (n 4) |
| titan's share at its ceiling | 0.749 [0.62, 0.865] (n 4) | 0 [0, 0] (n 4) |
| hyperion's share at its ceiling | 0.801 [0.746, 0.849] (n 4) | 0 [0, 0] (n 4) |

### The depth supplement: a page's runs in flight at two paces, on the SSDs

| Figure | c32t2, 2 in flight | c32t2, 16 in flight | c64t2, 2 in flight | c64t2, 16 in flight |
| --- | --- | --- | --- | --- |
| cell read p99 ms before | 5.75 [5.24, 6.2] (n 4) | 5.75 [5.57, 5.9] (n 4) | 5.54 [5.31, 5.66] (n 4) | 6.03 [5.96, 6.23] (n 4) |
| cell read p99 ms during | 6.05 [5.4, 7.25] (n 4) | 6.5 [5.89, 6.99] (n 4) | 6.16 [5.88, 6.88] (n 4) | 7.2 [6.99, 7.44] (n 4) |
| cell read p99 during / before | 1.05 [1.03, 1.17] (n 4) | 1.12 [1.06, 1.2] (n 4) | 1.11 [1.1, 1.22] (n 4) | 1.19 [1.17, 1.2] (n 4) |
| cell insert p99 ms before | 24.4 [23, 25.7] (n 4) | 23.8 [22.9, 25.5] (n 4) | 23.3 [22.6, 24.9] (n 4) | 24.4 [23.6, 25.1] (n 4) |
| cell insert p99 ms during | 25.4 [24.4, 27.1] (n 4) | 25.9 [23.6, 28.1] (n 4) | 25.8 [25, 29.2] (n 4) | 27.8 [26.1, 28.6] (n 4) |
| cell insert p99 during / before | 1.05 [1.02, 1.08] (n 4) | 1.07 [1.03, 1.15] (n 4) | 1.12 [1.09, 1.17] (n 4) | 1.13 [1.09, 1.18] (n 4) |
| scan MiB a second during | 76.5 [69.2, 80.4] (n 4) | 77.4 [72.6, 80.4] (n 4) | 145 [135, 149] (n 4) | 145 [142, 157] (n 4) |

### A scan alone at the widest pace

| Figure | SSD | Disk |
| --- | --- | --- |
| scan MiB a second alone | 634 [575, 692] (n 4) | 23.1 [17.9, 26.1] (n 4) |
| device ms a MiB scanned | 1.85 [1.8, 1.98] (n 4) | 123 [111, 151] (n 4) |
| device reads a MiB scanned | 93.1 [91.8, 94] (n 4) | 210 [209, 211] (n 4) |

### W7's budget on the medians, and each ceiling in device time

| Pace | Layout | Read | Insert | Ceiling in device ms a second |
| --- | --- | --- | --- | --- |
| c32t2 | ssd | 5.79 -> 6.60 ms, outside | 24.74 -> 26.54 ms, inside | 59 |
| c32t2 | disk | 44.75 -> 196.35 ms, outside | 65.42 -> 215.94 ms, outside | 3939 |
| c64t2 | ssd | 5.92 -> 7.00 ms, outside | 25.18 -> 28.05 ms, outside | 118 |
| c64t2 | disk | 1082.37 -> 230.72 ms, inside | 2463.74 -> 279.49 ms, inside | 7879 |
| c128t2 | ssd | 6.03 -> 10.00 ms, outside | 24.99 -> 33.92 ms, outside | 236 |
| c128t2 | disk | 257.53 -> 293.44 ms, outside | 324.48 -> 286.21 ms, inside | 15758 |
| c256t2 | ssd | 5.79 -> 8.58 ms, outside | 24.13 -> 33.03 ms, outside | 473 |
| c256t2 | disk | 101.38 -> 295.55 ms, outside | 82.85 -> 302.98 ms, outside | 31515 |
| c64t64 | ssd | 5.73 -> 6.92 ms, outside | 24.44 -> 26.73 ms, inside | 118 |
| c64t64 | disk | 250.24 -> 321.79 ms, outside | 238.97 -> 162.18 ms, inside | 7879 |
| c256t256 | ssd | 5.96 -> 8.11 ms, outside | 24.18 -> 30.27 ms, outside | 473 |
| c256t256 | disk | 68.77 -> 597.25 ms, outside | 50.02 -> 225.53 ms, outside | 31515 |
| c128t1 | ssd | 5.81 -> 7.81 ms, outside | 24.30 -> 28.21 ms, outside | 236 |
| c128t1 | disk | 132.19 -> 237.89 ms, outside | 163.58 -> 275.33 ms, outside | 15758 |
| c128t4 | ssd | 5.89 -> 8.09 ms, outside | 23.97 -> 28.51 ms, outside | 236 |
| c128t4 | disk | 152.25 -> 338.30 ms, outside | 97.63 -> 252.48 ms, outside | 15758 |
