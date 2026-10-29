### 1. Rate and cpu by frame, round 4

client hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · server hyperion · leg hyperion loopback

| cell | side | MiB/s | server cpu ms/GiB | client cpu ms/GiB | host cpu ms/GiB | held KiB | sock KiB (server/client) | small p50/p99/p999 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write 64K file | ktls-nopad | 654.2 | 1463 | 908.8 | 2470 | 256.0 | 105.9/4112 | — |
| write 64K file | ktls | 643.0 | 1495 | 918.7 | 2525 | 256.0 | 163.3/4112 | — |
| write 64K file | plain | 790.0 | 1036 | 369.9 | 1376 | 256.0 | 414.9/4155 | — |
| write 64K memory | ktls-nopad | 778.8 | 1231 | 1315 | 2675 | 64.00 | 83.96/716.6 | — |
| write 64K memory | ktls | 871.8 | 1118 | 1149 | 2337 | 64.00 | 194.7/4109 | — |
| write 64K memory | plain | 2203 | 468.4 | 464.8 | 930.4 | 64.00 | 65.91/65.91 | — |
| write 256K file | ktls-nopad | 791.4 | 1027 | 832.3 | 1896 | 1024 | 324.4/4112 | — |
| write 256K file | ktls | 776.4 | 1200 | 831.8 | 2131 | 1024 | 324.4/4112 | — |
| write 256K file | plain | 791.2 | 503.4 | 318.2 | 812.7 | 1024 | 794.3/4160 | — |
| write 256K memory | ktls-nopad | 880.6 | 1105 | 1162 | 2343 | 256.0 | 69.71/70.70 | — |
| write 256K memory | ktls | 784.2 | 1079 | 816.8 | 1919 | 256.0 | 194.7/2581 | — |
| write 256K memory | plain | 3251 | 317.7 | 315.1 | 641.1 | 256.0 | 129.8/195.8 | — |
| write 1M file | ktls-nopad | 565.5 | 1296 | 896.8 | 2412 | 3072 | 106.9/2565 | — |
| write 1M file | ktls | 789.5 | 1002 | 823.7 | 1934 | 4096 | 324.4/3607 | — |
| write 1M file | plain | 791.3 | 373.5 | 375.3 | 843.6 | 4096 | 1785/4161 | — |
| write 1M memory | ktls-nopad | 1228 | 782.9 | 831.8 | 1652 | 1024 | 259.5/2569 | — |
| write 1M memory | ktls | 1026 | 882.4 | 826.4 | 1742 | 1024 | 259.5/2581 | — |
| write 1M memory | plain | 3546 | 291.3 | 288.7 | 583.7 | 1024 | 194.7/131.6 | — |
| write 4M file | ktls-nopad | 657.5 | 1039 | 817.2 | 1971 | 8192 | 181.3/2581 | — |
| write 4M file | ktls | 790.1 | 949.1 | 827.0 | 1910 | 16384 | 324.4/2581 | — |
| write 4M file | plain | 791.1 | 304.1 | 461.1 | 918.9 | 16384 | 8666/4149 | — |
| write 4M memory | ktls-nopad | 712.7 | 989.2 | 821.5 | 1891 | 4096 | 194.7/2581 | — |
| write 4M memory | ktls | 622.3 | 1179 | 823.4 | 2043 | 4096 | 194.7/2581 | — |
| write 4M memory | plain | 3121 | 331.0 | 327.3 | 659.6 | 4096 | 129.8/134.1 | — |
| write 8M file | ktls-nopad | 790.3 | 931.6 | 825.4 | 1923 | 32768 | 259.5/2581 | — |
| write 8M file | ktls | 790.2 | 946.4 | 845.1 | 1943 | 32768 | 324.4/4112 | — |
| write 8M file | plain | 791.6 | 302.7 | 454.1 | 907.6 | 32768 | 11882/4160 | — |
| write 8M memory | ktls-nopad | 1166 | 838.0 | 835.7 | 1680 | 8192 | 259.5/2581 | — |
| write 8M memory | ktls | 866.7 | 913.4 | 816.3 | 1745 | 8192 | 324.4/4112 | — |
| write 8M memory | plain | 2974 | 347.6 | 345.1 | 703.9 | 8192 | 129.8/72.48 | — |
| read 64K file | ktls-nopad | 435.8 | 2093 | 1438 | 3712 | 256.0 | 34.86/52.75 | — |
| read 64K file | ktls | 455.5 | 2102 | 1597 | 3803 | 256.0 | 70.70/35.85 | — |
| read 64K file | plain | 765.1 | 1241 | 431.1 | 1598 | 256.0 | 0/1.96 | — |
| read 64K memory | ktls-nopad | 636.4 | 1584 | 1327 | 3070 | 0 | 1.91/49.47 | — |
| read 64K memory | ktls | 620.8 | 1628 | 1476 | 3292 | 0 | 69.71/36.84 | — |
| read 64K memory | plain | 1544 | 668.9 | 415.1 | 1037 | 0 | 0/1.96 | — |
| read 256K file | ktls-nopad | 763.5 | 1350 | 1058 | 2478 | 512.0 | 130.2/129.8 | — |
| read 256K file | ktls | 747.3 | 1370 | 1178 | 2639 | 512.0 | 69.71/68.22 | — |
| read 256K file | plain | 863.1 | 799.9 | 381.7 | 1153 | 256.0 | 523.3/66.03 | — |
| read 256K memory | ktls-nopad | 829.0 | 1237 | 1078 | 2460 | 0 | 66.52/82.92 | — |
| read 256K memory | ktls | 959.2 | 1063 | 935.2 | 2054 | 0 | 637.6/226.6 | — |
| read 256K memory | plain | 2685 | 384.7 | 243.5 | 588.0 | 0 | 0/65.85 | — |
| read 1M file | ktls-nopad | 676.3 | 942.9 | 851.5 | 1820 | 1024 | 2994/172.3 | — |
| read 1M file | ktls | 609.3 | 947.7 | 1024 | 2047 | 1024 | 3159/160.3 | — |
| read 1M file | plain | 863.1 | 546.8 | 278.2 | 809.0 | 1024 | 0.969/64.89 | — |
| read 1M memory | ktls-nopad | 702.7 | 876.7 | 800.0 | 1705 | 0 | 3633/194.7 | — |
| read 1M memory | ktls | 665.9 | 889.0 | 939.5 | 1848 | 0 | 3547/194.7 | — |
| read 1M memory | plain | 3207 | 322.1 | 191.8 | 473.1 | 0 | 0/64.89 | — |
| read 4M file | ktls-nopad | 860.5 | 931.1 | 733.5 | 1701 | 12288 | 4101/389.3 | — |
| read 4M file | ktls | 788.7 | 961.1 | 920.0 | 1945 | 12288 | 4113/259.5 | — |
| read 4M file | plain | 863.0 | 547.3 | 332.5 | 863.6 | 4096 | 0.969/64.89 | — |
| read 4M memory | ktls-nopad | 1132 | 889.7 | 747.9 | 1658 | 0 | 3758/194.7 | — |
| read 4M memory | ktls | 696.6 | 877.9 | 956.2 | 1905 | 0 | 4109/194.7 | — |
| read 4M memory | plain | 2844 | 363.6 | 272.3 | 617.7 | 0 | 0/65.85 | — |
| read 8M file | ktls-nopad | 807.9 | 1205 | 945.2 | 2234 | 16384 | 4032/2569 | — |
| read 8M file | ktls | 713.4 | 981.6 | 960.4 | 1972 | 24576 | 4109/194.7 | — |
| read 8M file | plain | 863.7 | 557.2 | 347.7 | 884.1 | 8192 | 0.969/64.89 | — |
| read 8M memory | ktls-nopad | 1144 | 889.4 | 743.7 | 1675 | 0 | 3792/194.7 | — |
| read 8M memory | ktls | 1132 | 858.1 | 849.1 | 1728 | 0 | 3089/324.4 | — |
| read 8M memory | plain | 2749 | 376.1 | 292.4 | 633.3 | 0 | 0/65.85 | — |

x11: handoff check (plain): handed 1, ktls after 0, ok 1
x11: handoff check (ktls): handed 1, ktls after 1, ok 1
### 4. Routes to the other executor, round 4

client hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · server hyperion · leg hyperion loopback

| cell | side | MiB/s | server cpu ms/GiB | client cpu ms/GiB | host cpu ms/GiB | held KiB | sock KiB (server/client) | small p50/p99/p999 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write 64K plain | handoff | 789.7 | 1035 | 361.6 | 1390 | 256.0 | 473.0/3651 | — |
| write 64K plain | hop-copy | 474.6 | 2027 | 357.3 | 2451 | 256.0 | 539.9/4157 | — |
| write 64K plain | hop | 479.8 | 2062 | 355.2 | 2544 | 256.0 | 459.8/4152 | — |
| write 64K plain | direct | 790.4 | 1035 | 368.7 | 1365 | 256.0 | 468.9/4153 | — |
| write 64K ktls | handoff | 674.5 | 1441 | 898.3 | 2435 | 256.0 | 194.7/4112 | — |
| write 64K ktls | hop-copy | 483.0 | 2963 | 1061 | 4027 | 256.0 | 194.7/4112 | — |
| write 64K ktls | hop | 488.5 | 2928 | 1058 | 4041 | 256.0 | 259.5/4112 | — |
| write 64K ktls | direct | 634.0 | 1507 | 918.1 | 2510 | 256.0 | 160.3/4112 | — |
| write 1M plain | handoff | 790.9 | 377.6 | 372.8 | 844.1 | 4096 | 1765/4148 | — |
| write 1M plain | hop-copy | 605.1 | 787.5 | 270.8 | 1188 | 4096 | 1088/4152 | — |
| write 1M plain | hop | 692.5 | 498.2 | 284.5 | 993.5 | 4096 | 1927/4158 | — |
| write 1M plain | direct | 791.5 | 372.7 | 377.2 | 869.3 | 4096 | 1574/4146 | — |
| write 1M ktls | handoff | 565.5 | 1318 | 878.7 | 2332 | 2048 | 160.3/2565 | — |
| write 1M ktls | hop-copy | 610.6 | 1508 | 847.2 | 2511 | 4096 | 188.8/2580 | — |
| write 1M ktls | hop | 618.7 | 1302 | 851.1 | 2409 | 4096 | 194.7/4112 | — |
| write 1M ktls | direct | 497.7 | 1384 | 918.7 | 2497 | 2048 | 129.8/2581 | — |

