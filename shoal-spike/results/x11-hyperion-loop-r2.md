### 1. Rate and cpu by frame, round 2

client hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · server hyperion · leg hyperion loopback

| cell | side | MiB/s | server cpu ms/GiB | client cpu ms/GiB | host cpu ms/GiB | held KiB | sock KiB (server/client) | small p50/p99/p999 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write 64K file | ktls-nopad | 643.6 | 1482 | 899.3 | 2517 | 256.0 | 105.9/4112 | — |
| write 64K file | ktls | 663.6 | 1458 | 884.2 | 2450 | 256.0 | 194.7/4112 | — |
| write 64K file | plain | 790.4 | 1026 | 361.3 | 1345 | 256.0 | 603.6/4153 | — |
| write 64K memory | ktls-nopad | 789.9 | 1215 | 1296 | 2605 | 64.00 | 83.90/53.74 | — |
| write 64K memory | ktls | 966.6 | 1022 | 1010 | 2105 | 64.00 | 259.5/4110 | — |
| write 64K memory | plain | 2205 | 468.0 | 464.4 | 937.1 | 64.00 | 65.91/308.0 | — |
| write 256K file | ktls-nopad | 791.2 | 1060 | 822.4 | 1905 | 1024 | 324.4/2581 | — |
| write 256K file | ktls | 766.4 | 1211 | 816.0 | 2115 | 1024 | 259.5/3864 | — |
| write 256K file | plain | 791.4 | 500.0 | 334.1 | 874.6 | 1024 | 685.0/4152 | — |
| write 256K memory | ktls-nopad | 1243 | 783.0 | 822.5 | 1641 | 256.0 | 324.4/2468 | — |
| write 256K memory | ktls | 1179 | 853.5 | 813.6 | 1703 | 256.0 | 259.5/2581 | — |
| write 256K memory | plain | 3265 | 316.3 | 313.7 | 630.8 | 256.0 | 129.8/195.8 | — |
| write 1M file | ktls-nopad | 646.3 | 1113 | 832.5 | 2120 | 2048 | 194.7/2581 | — |
| write 1M file | ktls | 594.4 | 1232 | 836.7 | 2259 | 2048 | 194.7/2581 | — |
| write 1M file | plain | 791.3 | 379.7 | 376.5 | 913.5 | 4096 | 1735/4152 | — |
| write 1M memory | ktls-nopad | 721.4 | 1028 | 826.9 | 1958 | 1024 | 167.8/2573 | — |
| write 1M memory | ktls | 632.3 | 1135 | 850.0 | 2011 | 1024 | 194.7/4112 | — |
| write 1M memory | plain | 3519 | 293.5 | 291.1 | 593.0 | 1024 | 129.8/131.6 | — |
| write 4M file | ktls-nopad | 656.6 | 1049 | 815.4 | 2061 | 8192 | 194.7/2581 | — |
| write 4M file | ktls | 580.7 | 1230 | 844.2 | 2253 | 4096 | 190.3/2581 | — |
| write 4M file | plain | 791.6 | 318.0 | 462.1 | 933.5 | 16384 | 6206/4151 | — |
| write 4M memory | ktls-nopad | 1260 | 786.5 | 819.4 | 1606 | 4096 | 324.4/2432 | — |
| write 4M memory | ktls | 627.7 | 1175 | 818.5 | 2071 | 4096 | 194.7/2581 | — |
| write 4M memory | plain | 3111 | 332.1 | 329.5 | 670.5 | 4096 | 129.8/134.1 | — |
| write 8M file | ktls-nopad | 790.1 | 842.5 | 858.5 | 1829 | 32768 | 324.4/3351 | — |
| write 8M file | ktls | 571.1 | 1217 | 857.6 | 2244 | 8192 | 194.7/2581 | — |
| write 8M file | plain | 788.7 | 304.2 | 455.5 | 906.1 | 32768 | 12659/4160 | — |
| write 8M memory | ktls-nopad | 713.4 | 987.1 | 804.5 | 1857 | 8192 | 194.7/2581 | — |
| write 8M memory | ktls | 627.1 | 1161 | 816.9 | 2054 | 8192 | 194.7/2581 | — |
| write 8M memory | plain | 2972 | 347.8 | 340.7 | 694.2 | 8192 | 129.8/137.4 | — |
| read 64K file | ktls-nopad | 426.3 | 2100 | 1421 | 3627 | 256.0 | 34.86/35.85 | — |
| read 64K file | ktls | 449.9 | 2148 | 1597 | 3842 | 256.0 | 34.86/35.85 | — |
| read 64K file | plain | 763.1 | 1241 | 447.6 | 1632 | 256.0 | 267.4/66.82 | — |
| read 64K memory | ktls-nopad | 627.2 | 1608 | 1354 | 3138 | 0 | 17.90/35.85 | — |
| read 64K memory | ktls | 620.9 | 1629 | 1475 | 3245 | 0 | 17.90/66.01 | — |
| read 64K memory | plain | 1550 | 666.6 | 408.8 | 1014 | 0 | 0/1.96 | — |
| read 256K file | ktls-nopad | 728.4 | 1412 | 1083 | 2617 | 512.0 | 197.4/65.88 | — |
| read 256K file | ktls | 717.3 | 1414 | 1189 | 2723 | 768.0 | 81.98/98.99 | — |
| read 256K file | plain | 863.0 | 823.2 | 378.3 | 1139 | 512.0 | 0.969/66.03 | — |
| read 256K memory | ktls-nopad | 713.6 | 1013 | 886.8 | 1914 | 0 | 847.5/125.9 | — |
| read 256K memory | ktls | 851.8 | 1200 | 1085 | 2363 | 0 | 114.0/184.7 | — |
| read 256K memory | plain | 2690 | 384.0 | 241.2 | 587.8 | 0 | 0/65.85 | — |
| read 1M file | ktls-nopad | 628.7 | 1004 | 912.1 | 1993 | 1024 | 3426/136.3 | — |
| read 1M file | ktls | 602.5 | 1001 | 1017 | 2063 | 1024 | 3160/194.7 | — |
| read 1M file | plain | 862.9 | 547.4 | 261.8 | 768.9 | 1024 | 0.969/64.89 | — |
| read 1M memory | ktls-nopad | 857.5 | 1202 | 1039 | 2369 | 0 | 0/51.81 | — |
| read 1M memory | ktls | 650.1 | 895.5 | 944.5 | 1906 | 0 | 3638/194.7 | — |
| read 1M memory | plain | 3205 | 322.3 | 190.8 | 465.0 | 0 | 0/0.969 | — |
| read 4M file | ktls-nopad | 862.6 | 1037 | 760.4 | 1806 | 4096 | 3965/259.5 | — |
| read 4M file | ktls | 679.0 | 1016 | 968.8 | 2029 | 12288 | 4051/182.8 | — |
| read 4M file | plain | 862.3 | 576.3 | 343.7 | 940.4 | 4096 | 0.969/64.89 | — |
| read 4M memory | ktls-nopad | 888.8 | 957.6 | 890.5 | 1881 | 0 | 4105/95.88 | — |
| read 4M memory | ktls | 703.9 | 890.5 | 960.3 | 1885 | 0 | 3592/191.8 | — |
| read 4M memory | plain | 2850 | 362.8 | 271.5 | 607.0 | 0 | 0/65.85 | — |
| read 8M file | ktls-nopad | 755.9 | 1370 | 1048 | 2557 | 16384 | 0/104.6 | — |
| read 8M file | ktls | 654.3 | 1013 | 994.7 | 2078 | 32768 | 3602/170.8 | — |
| read 8M file | plain | 863.5 | 570.7 | 349.1 | 936.3 | 8192 | 0.969/64.89 | — |
| read 8M memory | ktls-nopad | 1011 | 947.3 | 812.7 | 1784 | 0 | 3802/136.3 | — |
| read 8M memory | ktls | 1229 | 840.0 | 823.1 | 1674 | 0 | 3870/778.6 | — |
| read 8M memory | plain | 2729 | 378.9 | 293.2 | 643.7 | 0 | 1.06/65.85 | — |

x11: handoff check (plain): handed 1, ktls after 0, ok 1
x11: handoff check (ktls): handed 1, ktls after 1, ok 1
### 4. Routes to the other executor, round 2

client hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · server hyperion · leg hyperion loopback

| cell | side | MiB/s | server cpu ms/GiB | client cpu ms/GiB | host cpu ms/GiB | held KiB | sock KiB (server/client) | small p50/p99/p999 µs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| write 64K plain | handoff | 790.4 | 1004 | 357.6 | 1321 | 256.0 | 657.9/4153 | — |
| write 64K plain | hop-copy | 475.8 | 1988 | 335.5 | 2354 | 256.0 | 656.9/4161 | — |
| write 64K plain | hop | 481.2 | 2047 | 338.8 | 2456 | 256.0 | 497.0/4152 | — |
| write 64K plain | direct | 787.7 | 1057 | 365.6 | 1417 | 256.0 | 627.6/3648 | — |
| write 64K ktls | handoff | 669.8 | 1448 | 874.4 | 2455 | 256.0 | 188.8/4112 | — |
| write 64K ktls | hop-copy | 485.7 | 2943 | 1050 | 3992 | 256.0 | 194.7/4112 | — |
| write 64K ktls | hop | 489.3 | 2912 | 1050 | 3942 | 256.0 | 259.5/4112 | — |
| write 64K ktls | direct | 600.6 | 1591 | 946.9 | 2680 | 256.0 | 106.9/4112 | — |
| write 1M plain | handoff | 791.3 | 375.5 | 377.6 | 877.3 | 4096 | 1789/4156 | — |
| write 1M plain | hop-copy | 604.1 | 786.7 | 270.3 | 1186 | 4096 | 1196/4159 | — |
| write 1M plain | hop | 690.1 | 488.0 | 282.6 | 940.6 | 4096 | 1396/4152 | — |
| write 1M plain | direct | 787.1 | 372.7 | 378.9 | 882.0 | 4096 | 1436/4146 | — |
| write 1M ktls | handoff | 589.8 | 1245 | 838.1 | 2256 | 2048 | 194.7/2581 | — |
| write 1M ktls | hop-copy | 595.9 | 1520 | 839.7 | 2550 | 4096 | 194.7/2581 | — |
| write 1M ktls | hop | 540.3 | 1545 | 901.0 | 2713 | 4096 | 106.9/2565 | — |
| write 1M ktls | direct | 554.6 | 1324 | 879.0 | 2370 | 3072 | 160.3/2581 | — |

