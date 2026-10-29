### 1. Rate and cpu by frame, round 3

client hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · server hyperion · leg hyperion loopback

| cell | side | MiB/s | server cpu ms/GiB | client cpu ms/GiB | host cpu ms/GiB | held KiB | sock KiB (server/client) | small p50/p99/p999 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write 64K file | plain | 790.4 | 1014 | 358.3 | 1321 | 256.0 | 668.5/4153 | — |
| write 64K file | ktls | 671.4 | 1446 | 875.5 | 2428 | 256.0 | 324.4/4112 | — |
| write 64K file | ktls-nopad | 708.1 | 1367 | 873.6 | 2333 | 256.0 | 129.8/4112 | — |
| write 64K memory | plain | 2197 | 469.6 | 466.0 | 943.9 | 64.00 | 65.91/1430 | — |
| write 64K memory | ktls | 1011 | 987.1 | 895.1 | 1950 | 64.00 | 182.8/4112 | — |
| write 64K memory | ktls-nopad | 788.9 | 1216 | 1298 | 2608 | 64.00 | 35.85/53.74 | — |
| write 256K file | plain | 791.2 | 498.3 | 318.2 | 820.4 | 1024 | 790.6/4160 | — |
| write 256K file | ktls | 781.9 | 1205 | 815.6 | 2132 | 1024 | 262.0/4112 | — |
| write 256K file | ktls-nopad | 791.3 | 1037 | 823.0 | 1907 | 1024 | 324.4/2581 | — |
| write 256K memory | plain | 3232 | 319.6 | 316.8 | 637.2 | 256.0 | 129.8/195.8 | — |
| write 256K memory | ktls | 1184 | 850.8 | 813.2 | 1681 | 256.0 | 324.4/2581 | — |
| write 256K memory | ktls-nopad | 1247 | 781.2 | 821.7 | 1597 | 256.0 | 324.4/2468 | — |
| write 1M file | plain | 791.3 | 375.7 | 379.4 | 910.9 | 4096 | 1576/4146 | — |
| write 1M file | ktls | 553.9 | 1293 | 861.3 | 2392 | 2048 | 169.3/2581 | — |
| write 1M file | ktls-nopad | 664.3 | 1049 | 807.9 | 2007 | 2048 | 194.7/2581 | — |
| write 1M memory | plain | 3506 | 294.6 | 292.0 | 587.3 | 1024 | 64.89/131.6 | — |
| write 1M memory | ktls | 643.5 | 1174 | 809.5 | 2052 | 1024 | 182.8/2581 | — |
| write 1M memory | ktls-nopad | 1246 | 782.7 | 822.4 | 1636 | 1024 | 324.4/2573 | — |
| write 4M file | plain | 791.0 | 311.9 | 457.1 | 921.5 | 16384 | 6412/4159 | — |
| write 4M file | ktls | 667.9 | 1133 | 811.5 | 2076 | 8192 | 194.7/2581 | — |
| write 4M file | ktls-nopad | 791.9 | 863.4 | 816.4 | 1810 | 16384 | 324.4/2581 | — |
| write 4M memory | plain | 3073 | 336.3 | 331.6 | 680.1 | 4096 | 129.8/198.9 | — |
| write 4M memory | ktls | 718.3 | 1069 | 815.9 | 1947 | 4096 | 259.5/4112 | — |
| write 4M memory | ktls-nopad | 724.6 | 977.4 | 804.6 | 1811 | 4096 | 194.7/2581 | — |
| write 8M file | plain | 791.9 | 305.1 | 454.1 | 899.9 | 32768 | 11290/4160 | — |
| write 8M file | ktls | 563.0 | 1235 | 864.7 | 2262 | 8192 | 194.7/2581 | — |
| write 8M file | ktls-nopad | 787.1 | 842.7 | 864.8 | 1871 | 32768 | 324.4/2581 | — |
| write 8M memory | plain | 2955 | 349.8 | 343.5 | 705.5 | 8192 | 129.8/137.4 | — |
| write 8M memory | ktls | 1027 | 881.7 | 822.0 | 1701 | 8192 | 324.4/2581 | — |
| write 8M memory | ktls-nopad | 683.1 | 982.4 | 809.5 | 1817 | 8192 | 194.7/4112 | — |
| read 64K file | plain | 766.2 | 1252 | 435.6 | 1625 | 256.0 | 267.4/3.88 | — |
| read 64K file | ktls | 457.0 | 2131 | 1594 | 3804 | 256.0 | 34.86/34.86 | — |
| read 64K file | ktls-nopad | 438.4 | 2125 | 1457 | 3741 | 256.0 | 34.86/35.85 | — |
| read 64K memory | plain | 1529 | 675.6 | 413.3 | 1050 | 0 | 0/1.96 | — |
| read 64K memory | ktls | 621.0 | 1629 | 1476 | 3255 | 0 | 17.90/66.01 | — |
| read 64K memory | ktls-nopad | 631.7 | 1596 | 1338 | 3131 | 0 | 17.90/52.80 | — |
| read 256K file | plain | 862.5 | 812.2 | 381.9 | 1161 | 1024 | 0.969/66.03 | — |
| read 256K file | ktls | 736.1 | 1388 | 1185 | 2646 | 768.0 | 52.75/70.70 | — |
| read 256K file | ktls-nopad | 750.7 | 1371 | 1064 | 2531 | 1024 | 49.94/51.62 | — |
| read 256K memory | plain | 2671 | 386.8 | 242.4 | 604.2 | 0 | 0/64.89 | — |
| read 256K memory | ktls | 963.6 | 1058 | 924.2 | 2036 | 0 | 425.5/201.8 | — |
| read 256K memory | ktls-nopad | 876.6 | 976.4 | 805.2 | 1880 | 0 | 775.6/194.7 | — |
| read 1M file | plain | 863.1 | 546.1 | 274.7 | 775.8 | 1024 | 0.969/64.89 | — |
| read 1M file | ktls | 623.2 | 975.7 | 989.6 | 1994 | 1024 | 3137/194.7 | — |
| read 1M file | ktls-nopad | 655.7 | 982.6 | 872.2 | 1933 | 1024 | 3404/160.3 | — |
| read 1M memory | plain | 3186 | 324.3 | 194.5 | 483.2 | 0 | 1886/0.969 | — |
| read 1M memory | ktls | 651.7 | 885.0 | 947.7 | 1866 | 0 | 3640/194.7 | — |
| read 1M memory | ktls-nopad | 715.0 | 885.0 | 796.7 | 1701 | 0 | 3520/194.7 | — |
| read 4M file | plain | 863.1 | 573.3 | 345.8 | 889.7 | 4096 | 0.969/64.89 | — |
| read 4M file | ktls | 651.7 | 1017 | 995.2 | 2076 | 12288 | 4106/164.8 | — |
| read 4M file | ktls-nopad | 862.8 | 992.8 | 728.0 | 1758 | 4096 | 3811/389.3 | — |
| read 4M memory | plain | 2854 | 362.3 | 270.6 | 602.0 | 0 | 0/65.85 | — |
| read 4M memory | ktls | 700.7 | 880.3 | 953.4 | 1862 | 0 | 4106/194.7 | — |
| read 4M memory | ktls-nopad | 1210 | 852.4 | 720.9 | 1605 | 0 | 3542/324.4 | — |
| read 8M file | plain | 862.3 | 569.5 | 348.3 | 938.0 | 8192 | 0.969/64.89 | — |
| read 8M file | ktls | 779.0 | 1008 | 931.0 | 1966 | 24576 | 4111/194.7 | — |
| read 8M file | ktls-nopad | 855.1 | 1108 | 850.3 | 2017 | 16384 | 3853/136.3 | — |
| read 8M memory | plain | 2748 | 376.3 | 293.0 | 638.5 | 0 | 0/65.85 | — |
| read 8M memory | ktls | 686.3 | 877.2 | 962.4 | 1886 | 0 | 4047/194.7 | — |
| read 8M memory | ktls-nopad | 1185 | 868.1 | 721.8 | 1608 | 0 | 3488/259.5 | — |

x11: handoff check (plain): handed 1, ktls after 0, ok 1
x11: handoff check (ktls): handed 1, ktls after 1, ok 1
### 4. Routes to the other executor, round 3

client hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · server hyperion · leg hyperion loopback

| cell | side | MiB/s | server cpu ms/GiB | client cpu ms/GiB | host cpu ms/GiB | held KiB | sock KiB (server/client) | small p50/p99/p999 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write 64K plain | direct | 789.9 | 1039 | 371.1 | 1348 | 256.0 | 503.4/4160 | — |
| write 64K plain | hop | 480.0 | 2076 | 357.3 | 2572 | 256.0 | 444.4/4160 | — |
| write 64K plain | hop-copy | 475.0 | 2077 | 354.8 | 2535 | 256.0 | 461.7/4159 | — |
| write 64K plain | handoff | 790.3 | 1021 | 365.2 | 1381 | 256.0 | 685.9/4153 | — |
| write 64K ktls | direct | 667.7 | 1453 | 878.4 | 2429 | 256.0 | 185.8/4112 | — |
| write 64K ktls | hop | 489.3 | 2918 | 1046 | 3934 | 256.0 | 194.7/4112 | — |
| write 64K ktls | hop-copy | 484.1 | 2958 | 1047 | 3998 | 256.0 | 259.5/4112 | — |
| write 64K ktls | handoff | 636.8 | 1506 | 904.1 | 2540 | 256.0 | 160.3/4112 | — |
| write 1M plain | direct | 791.1 | 375.1 | 378.9 | 880.1 | 4096 | 1656/4158 | — |
| write 1M plain | hop | 691.7 | 486.3 | 283.1 | 950.3 | 4096 | 1896/4152 | — |
| write 1M plain | hop-copy | 606.9 | 773.1 | 268.2 | 1178 | 4096 | 2043/4158 | — |
| write 1M plain | handoff | 791.3 | 360.8 | 375.4 | 854.0 | 4096 | 1591/4160 | — |
| write 1M ktls | direct | 584.2 | 1246 | 840.7 | 2295 | 4096 | 188.8/2580 | — |
| write 1M ktls | hop | 533.9 | 1560 | 900.3 | 2711 | 4096 | 106.9/2565 | — |
| write 1M ktls | hop-copy | 527.5 | 1738 | 908.7 | 2838 | 4096 | 106.9/2565 | — |
| write 1M ktls | handoff | 603.6 | 1229 | 830.1 | 2211 | 2048 | 194.7/2581 | — |

