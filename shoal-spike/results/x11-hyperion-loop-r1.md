### 1. Rate and cpu by frame, round 1

client hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · server hyperion · leg hyperion loopback

| cell | side | MiB/s | server cpu ms/GiB | client cpu ms/GiB | host cpu ms/GiB | held KiB | sock KiB (server/client) | small p50/p99/p999 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write 64K file | plain | 790.6 | 1039 | 366.1 | 1399 | 256.0 | 509.8/4159 | — |
| write 64K file | ktls | 669.6 | 1449 | 877.5 | 2461 | 256.0 | 324.4/4112 | — |
| write 64K file | ktls-nopad | 716.0 | 1349 | 868.5 | 2348 | 256.0 | 194.7/4112 | — |
| write 64K memory | plain | 2209 | 467.3 | 463.6 | 929.9 | 64.00 | 65.91/1.02 | — |
| write 64K memory | ktls | 1059 | 945.1 | 890.2 | 1881 | 64.00 | 259.5/4110 | — |
| write 64K memory | ktls-nopad | 795.1 | 1206 | 1288 | 2618 | 64.00 | 36.78/53.74 | — |
| write 256K file | plain | 788.6 | 505.3 | 328.4 | 869.9 | 1024 | 769.1/4159 | — |
| write 256K file | ktls | 791.2 | 1187 | 816.8 | 2034 | 1024 | 324.4/2581 | — |
| write 256K file | ktls-nopad | 791.2 | 1021 | 816.2 | 1850 | 1024 | 389.3/4112 | — |
| write 256K memory | plain | 3238 | 319.0 | 316.2 | 636.6 | 256.0 | 129.8/195.8 | — |
| write 256K memory | ktls | 1208 | 842.7 | 795.9 | 1676 | 256.0 | 324.4/2581 | — |
| write 256K memory | ktls-nopad | 1154 | 835.7 | 887.6 | 1770 | 256.0 | 259.5/2464 | — |
| write 1M file | plain | 791.3 | 377.2 | 380.2 | 869.5 | 4096 | 2075/4152 | — |
| write 1M file | ktls | 544.7 | 1349 | 885.8 | 2398 | 2048 | 160.3/2580 | — |
| write 1M file | ktls-nopad | 791.3 | 831.1 | 817.4 | 1775 | 4096 | 324.4/2581 | — |
| write 1M memory | plain | 3500 | 295.1 | 292.5 | 596.4 | 1024 | 64.89/131.6 | — |
| write 1M memory | ktls | 543.9 | 1412 | 900.7 | 2394 | 1024 | 106.9/2565 | — |
| write 1M memory | ktls-nopad | 773.5 | 949.6 | 805.2 | 1768 | 1024 | 194.7/2581 | — |
| write 4M file | plain | 791.1 | 314.2 | 459.1 | 926.7 | 16384 | 6375/4160 | — |
| write 4M file | ktls | 677.5 | 1129 | 813.1 | 2131 | 12288 | 194.7/2581 | — |
| write 4M file | ktls-nopad | 647.1 | 1045 | 820.0 | 2047 | 8192 | 194.7/4112 | — |
| write 4M memory | plain | 3079 | 335.7 | 332.1 | 669.7 | 4096 | 129.8/134.1 | — |
| write 4M memory | ktls | 631.1 | 1170 | 815.1 | 2018 | 4096 | 193.3/2578 | — |
| write 4M memory | ktls-nopad | 1256 | 784.1 | 815.1 | 1624 | 4096 | 324.4/2572 | — |
| write 8M file | plain | 791.9 | 306.5 | 455.2 | 936.1 | 32768 | 12202/4160 | — |
| write 8M file | ktls | 489.5 | 1386 | 938.9 | 2543 | 8192 | 128.9/2578 | — |
| write 8M file | ktls-nopad | 791.4 | 840.0 | 864.3 | 1828 | 32768 | 324.4/2581 | — |
| write 8M memory | plain | 2957 | 349.6 | 348.1 | 695.7 | 8192 | 129.8/72.48 | — |
| write 8M memory | ktls | 986.5 | 882.3 | 818.7 | 1734 | 8192 | 324.4/4112 | — |
| write 8M memory | ktls-nopad | 1257 | 786.2 | 814.1 | 1630 | 8192 | 324.4/2579 | — |
| read 64K file | plain | 759.6 | 1248 | 435.2 | 1601 | 256.0 | 0/1.96 | — |
| read 64K file | ktls | 455.2 | 2119 | 1594 | 3792 | 256.0 | 34.86/35.85 | — |
| read 64K file | ktls-nopad | 440.3 | 2120 | 1458 | 3706 | 256.0 | 1.49/34.86 | — |
| read 64K memory | plain | 1542 | 670.0 | 415.3 | 1053 | 0 | 0/65.85 | — |
| read 64K memory | ktls | 618.2 | 1634 | 1478 | 3293 | 0 | 69.71/66.01 | — |
| read 64K memory | ktls-nopad | 626.0 | 1610 | 1362 | 3117 | 0 | 52.75/35.85 | — |
| read 256K file | plain | 863.0 | 804.0 | 381.1 | 1129 | 512.0 | 0.969/66.03 | — |
| read 256K file | ktls | 735.1 | 1380 | 1171 | 2646 | 512.0 | 257.8/147.9 | — |
| read 256K file | ktls-nopad | 752.1 | 1367 | 1061 | 2535 | 512.0 | 32.98/52.80 | — |
| read 256K memory | plain | 2677 | 385.6 | 242.7 | 601.3 | 0 | 1046/65.85 | — |
| read 256K memory | ktls | 956.7 | 1067 | 924.8 | 2018 | 0 | 458.1/202.5 | — |
| read 256K memory | ktls-nopad | 711.9 | 1017 | 890.9 | 1942 | 0 | 820.8/125.9 | — |
| read 1M file | plain | 863.3 | 546.2 | 266.7 | 797.0 | 1024 | 1.77/64.89 | — |
| read 1M file | ktls | 606.9 | 969.1 | 1021 | 2058 | 1024 | 3159/160.3 | — |
| read 1M file | ktls-nopad | 660.7 | 966.1 | 868.0 | 1847 | 1024 | 3102/160.3 | — |
| read 1M memory | plain | 3194 | 323.5 | 191.8 | 465.5 | 0 | 0/0.969 | — |
| read 1M memory | ktls | 649.9 | 891.4 | 946.0 | 1916 | 0 | 3514/194.7 | — |
| read 1M memory | ktls-nopad | 712.3 | 892.5 | 797.0 | 1754 | 0 | 3557/194.7 | — |
| read 4M file | plain | 863.1 | 572.2 | 347.0 | 922.9 | 4096 | 0.969/64.89 | — |
| read 4M file | ktls | 839.0 | 1018 | 911.7 | 1950 | 8192 | 4109/194.7 | — |
| read 4M file | ktls-nopad | 862.8 | 1075 | 843.4 | 1957 | 8192 | 4055/160.3 | — |
| read 4M memory | plain | 2836 | 364.7 | 271.7 | 624.1 | 0 | 0/65.85 | — |
| read 4M memory | ktls | 839.1 | 915.5 | 943.2 | 1955 | 0 | 3592/185.8 | — |
| read 4M memory | ktls-nopad | 1214 | 850.4 | 724.7 | 1595 | 0 | 3435/259.5 | — |
| read 8M file | plain | 862.3 | 569.5 | 348.2 | 892.9 | 8192 | 0.969/64.89 | — |
| read 8M file | ktls | 649.4 | 1013 | 994.9 | 2043 | 24576 | 4104/173.8 | — |
| read 8M file | ktls-nopad | 842.7 | 1090 | 882.8 | 2023 | 24576 | 3084/136.3 | — |
| read 8M memory | plain | 2724 | 379.7 | 293.7 | 656.5 | 0 | 1.06/389.3 | — |
| read 8M memory | ktls | 847.6 | 876.5 | 909.0 | 1814 | 0 | 3863/194.7 | — |
| read 8M memory | ktls-nopad | 1214 | 850.7 | 721.9 | 1597 | 0 | 3470/259.5 | — |

x11: handoff check (plain): handed 1, ktls after 0, ok 1
x11: handoff check (ktls): handed 1, ktls after 1, ok 1
### 4. Routes to the other executor, round 1

client hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · server hyperion · leg hyperion loopback

| cell | side | MiB/s | server cpu ms/GiB | client cpu ms/GiB | host cpu ms/GiB | held KiB | sock KiB (server/client) | small p50/p99/p999 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write 64K plain | direct | 787.0 | 1039 | 368.9 | 1415 | 256.0 | 634.6/4160 | — |
| write 64K plain | hop | 479.9 | 2285 | 367.4 | 2735 | 256.0 | 112.9/2582 | — |
| write 64K plain | hop-copy | 474.4 | 2021 | 325.6 | 2374 | 256.0 | 449.7/2630 | — |
| write 64K plain | handoff | 790.4 | 1035 | 363.6 | 1358 | 256.0 | 526.0/4161 | — |
| write 64K ktls | direct | 643.0 | 1496 | 903.4 | 2548 | 256.0 | 167.8/4112 | — |
| write 64K ktls | hop | 487.4 | 2932 | 1050 | 4016 | 256.0 | 259.5/4112 | — |
| write 64K ktls | hop-copy | 484.4 | 2957 | 1052 | 3970 | 256.0 | 324.4/4112 | — |
| write 64K ktls | handoff | 668.2 | 1447 | 880.4 | 2424 | 256.0 | 194.7/4112 | — |
| write 1M plain | direct | 791.1 | 379.0 | 381.1 | 900.8 | 4096 | 1790/4159 | — |
| write 1M plain | hop | 687.7 | 490.5 | 284.7 | 979.6 | 4096 | 1792/4161 | — |
| write 1M plain | hop-copy | 604.5 | 775.6 | 269.9 | 1230 | 4096 | 2102/4152 | — |
| write 1M plain | handoff | 790.9 | 374.5 | 376.3 | 838.9 | 4096 | 1591/4160 | — |
| write 1M ktls | direct | 791.0 | 1009 | 814.6 | 1939 | 4096 | 324.4/2581 | — |
| write 1M ktls | hop | 530.7 | 1567 | 898.9 | 2782 | 4096 | 106.9/2565 | — |
| write 1M ktls | hop-copy | 584.5 | 1566 | 853.7 | 2729 | 4096 | 160.3/2565 | — |
| write 1M ktls | handoff | 491.9 | 1386 | 924.3 | 2439 | 2048 | 129.8/2581 | — |

