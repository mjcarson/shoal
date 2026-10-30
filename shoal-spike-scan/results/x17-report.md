
### partitions a second

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 1.49e+05 [1.11e+05, 1.59e+05] (n 4) | 1.5e+05 [1.49e+05, 1.65e+05] (n 4) | no | 1.01 |  |  |
| r4: four reads in flight | 3.63e+05 [3.32e+05, 3.98e+05] (n 4) | 3.83e+05 [3.46e+05, 4.02e+05] (n 4) | no | 1.06 | 2.55 | yes |
| r16: sixteen reads in flight | 4.8e+05 [4.5e+05, 5.2e+05] (n 4) | 4.84e+05 [4.32e+05, 5.3e+05] (n 4) | no | 1.01 | 3.22 | yes |
| g4k2: runs across gaps of 4 KiB, two in flight | 2.35e+05 [1.87e+05, 2.6e+05] (n 4) | 1.25e+06 [1.17e+06, 1.31e+06] (n 4) | yes | 5.32 | 8.35 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 5e+05 [4.52e+05, 5.47e+05] (n 4) | 1.72e+06 [1.49e+06, 1.86e+06] (n 4) | yes | 3.45 | 11.50 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 1.51e+05 [1.37e+05, 1.6e+05] (n 4) | 1.25e+06 [1.13e+06, 1.33e+06] (n 4) | yes | 8.29 | 8.36 | yes |
| t1: three operations a record, one read at a time | 1.19e+05 [1.02e+05, 1.47e+05] (n 4) | 1.37e+05 [1.12e+05, 1.53e+05] (n 4) | no | 1.16 | 0.92 | no |
| t16: three operations a record, sixteen in flight | 4.64e+05 [3.82e+05, 5.03e+05] (n 4) | 4.63e+05 [4.02e+05, 5e+05] (n 4) | no | 1.00 | 3.09 | yes |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 4.87e+05 [4.47e+05, 5.63e+05] (n 4) | 1.92e+06 [1.66e+06, 2.08e+06] (n 4) | yes | 3.95 | 12.81 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 1.53e+05 [1.44e+05, 1.64e+05] (n 4) |  | - | 1.02 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 1.52e+05 [1.52e+05, 1.52e+05] (n 1) | 1.41e+05 [1.33e+05, 1.51e+05] (n 3) | yes | 0.92 | 0.94 | no |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 1.44e+06 [1.41e+06, 1.47e+06] (n 2) |  | - | 9.62 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 1.02e+06 [1.02e+06, 1.02e+06] (n 2) |  | - | 6.81 | yes |

### MiB a second

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 148 [110, 157] (n 4) | 149 [148, 163] (n 4) | no | 1.01 |  |  |
| r4: four reads in flight | 360 [329, 394] (n 4) | 380 [343, 399] (n 4) | no | 1.06 | 2.55 | yes |
| r16: sixteen reads in flight | 476 [447, 515] (n 4) | 480 [428, 526] (n 4) | no | 1.01 | 3.22 | yes |
| g4k2: runs across gaps of 4 KiB, two in flight | 233 [185, 258] (n 4) | 1.24e+03 [1.16e+03, 1.3e+03] (n 4) | yes | 5.32 | 8.35 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 496 [448, 543] (n 4) | 1.71e+03 [1.47e+03, 1.85e+03] (n 4) | yes | 3.45 | 11.50 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 150 [136, 159] (n 4) | 1.24e+03 [1.13e+03, 1.32e+03] (n 4) | yes | 8.29 | 8.36 | yes |
| t1: three operations a record, one read at a time | 118 [102, 146] (n 4) | 136 [111, 152] (n 4) | no | 1.16 | 0.92 | no |
| t16: three operations a record, sixteen in flight | 460 [379, 499] (n 4) | 459 [399, 496] (n 4) | no | 1.00 | 3.09 | yes |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 483 [444, 559] (n 4) | 1.91e+03 [1.65e+03, 2.07e+03] (n 4) | yes | 3.95 | 12.81 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 152 [143, 163] (n 4) |  | - | 1.02 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 151 [151, 151] (n 1) | 140 [132, 150] (n 3) | yes | 0.92 | 0.94 | no |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 1.43e+03 [1.4e+03, 1.46e+03] (n 2) |  | - | 9.62 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 1.01e+03 [1.01e+03, 1.02e+03] (n 2) |  | - | 6.81 | yes |

### walk reads a page

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 724 [723, 724] (n 4) | 723 [723, 724] (n 4) | no | 1.00 |  |  |
| r4: four reads in flight | 724 [723, 724] (n 4) | 724 [723, 724] (n 4) | no | 1.00 | 1.00 | no |
| r16: sixteen reads in flight | 724 [724, 724] (n 4) | 724 [723, 724] (n 4) | no | 1.00 | 1.00 | no |
| g4k2: runs across gaps of 4 KiB, two in flight | 720 [720, 720] (n 4) | 63.8 [63.1, 64.2] (n 4) | yes | 0.09 | 0.09 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 720 [720, 720] (n 4) | 63.8 [63, 64.3] (n 4) | yes | 0.09 | 0.09 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 617 [617, 618] (n 4) | 63.8 [63, 64.2] (n 4) | yes | 0.10 | 0.09 | yes |
| t1: three operations a record, one read at a time | 724 [723, 724] (n 4) | 724 [723, 724] (n 4) | no | 1.00 | 1.00 | no |
| t16: three operations a record, sixteen in flight | 724 [724, 724] (n 4) | 724 [723, 724] (n 4) | no | 1.00 | 1.00 | no |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 720 [720, 720] (n 4) | 63.8 [63.1, 64.2] (n 4) | yes | 0.09 | 0.09 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 724 [724, 724] (n 4) |  | - | 1.00 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 801 [801, 801] (n 1) | 801 [800, 801] (n 3) | no | 1.00 | 1.11 | yes |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 78.6 [78.5, 78.7] (n 2) |  | - | 0.11 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 78.3 [78.2, 78.5] (n 2) |  | - | 0.11 | yes |

### partitions a page

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 732 [732, 733] (n 4) | 732 [732, 733] (n 4) | no | 1.00 |  |  |
| r4: four reads in flight | 732 [732, 732] (n 4) | 732 [732, 732] (n 4) | no | 1.00 | 1.00 | no |
| r16: sixteen reads in flight | 732 [732, 732] (n 4) | 732 [732, 732] (n 4) | no | 1.00 | 1.00 | no |
| g4k2: runs across gaps of 4 KiB, two in flight | 732 [732, 732] (n 4) | 732 [732, 732] (n 4) | no | 1.00 | 1.00 | no |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 732 [732, 732] (n 4) | 732 [732, 732] (n 4) | no | 1.00 | 1.00 | no |
| g128k2: runs across gaps of 128 KiB, two in flight | 732 [732, 733] (n 4) | 732 [732, 732] (n 4) | no | 1.00 | 1.00 | no |
| t1: three operations a record, one read at a time | 732 [732, 732] (n 4) | 732 [732, 733] (n 4) | no | 1.00 | 1.00 | no |
| t16: three operations a record, sixteen in flight | 732 [732, 732] (n 4) | 732 [732, 732] (n 4) | no | 1.00 | 1.00 | no |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 732 [732, 732] (n 4) | 732 [732, 732] (n 4) | no | 1.00 | 1.00 | no |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 733 [732, 733] (n 4) |  | - | 1.00 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 806 [806, 806] (n 1) | 806 [806, 806] (n 3) | yes | 1.00 | 1.10 | yes |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 806 [806, 806] (n 2) |  | - | 1.10 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 806 [806, 806] (n 2) |  | - | 1.10 | yes |

### partitions a walk read

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 1 [1, 1] (n 4) | 1 [1, 1] (n 4) | no | 1.00 |  |  |
| r4: four reads in flight | 1 [1, 1] (n 4) | 1 [1, 1] (n 4) | no | 1.00 | 1.00 | no |
| r16: sixteen reads in flight | 1 [1, 1] (n 4) | 1 [1, 1] (n 4) | no | 1.00 | 1.00 | no |
| g4k2: runs across gaps of 4 KiB, two in flight | 1.01 [1.01, 1.01] (n 4) | 11.3 [11.3, 11.5] (n 4) | yes | 11.29 | 11.34 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 1.01 [1.01, 1.01] (n 4) | 11.3 [11.3, 11.5] (n 4) | yes | 11.29 | 11.35 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 1.17 [1.17, 1.17] (n 4) | 11.3 [11.3, 11.5] (n 4) | yes | 9.67 | 11.34 | yes |
| t1: three operations a record, one read at a time | 1 [1, 1] (n 4) | 1 [1, 1] (n 4) | no | 1.00 | 1.00 | no |
| t16: three operations a record, sixteen in flight | 1 [1, 1] (n 4) | 1 [1, 1] (n 4) | no | 1.00 | 1.00 | no |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 1.01 [1.01, 1.01] (n 4) | 11.3 [11.3, 11.5] (n 4) | yes | 11.29 | 11.34 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 1 [1, 1] (n 4) |  | - | 1.00 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 1 [1, 1] (n 1) | 1 [1, 1] (n 3) | no | 1.00 | 1.00 | no |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 10.2 [10.2, 10.2] (n 2) |  | - | 10.18 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 10.2 [10.2, 10.2] (n 2) |  | - | 10.22 | yes |

### partitions a stretch

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 1 [1, 1] (n 4) | 11.3 [11.2, 11.4] (n 4) | yes | 11.24 |  |  |
| r4: four reads in flight | 1 [1, 1] (n 4) | 11.3 [11.2, 11.5] (n 4) | yes | 11.32 | 1.01 | no |
| r16: sixteen reads in flight | 1 [1, 1] (n 4) | 11.3 [11.3, 11.5] (n 4) | yes | 11.33 | 1.01 | no |
| g4k2: runs across gaps of 4 KiB, two in flight | 1 [1, 1] (n 4) | 11.3 [11.3, 11.5] (n 4) | yes | 11.32 | 1.01 | no |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 1 [1, 1] (n 4) | 11.3 [11.2, 11.5] (n 4) | yes | 11.32 | 1.01 | no |
| g128k2: runs across gaps of 128 KiB, two in flight | 1 [1, 1] (n 4) | 11.3 [11.2, 11.5] (n 4) | yes | 11.32 | 1.01 | no |
| t1: three operations a record, one read at a time | 1 [1, 1] (n 4) | 11.3 [11.2, 11.5] (n 4) | yes | 11.30 | 1.00 | no |
| t16: three operations a record, sixteen in flight | 1 [1, 1] (n 4) | 11.3 [11.2, 11.5] (n 4) | yes | 11.33 | 1.01 | no |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 1 [1, 1] (n 4) | 11.3 [11.2, 11.5] (n 4) | yes | 11.32 | 1.01 | no |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 11.3 [11.2, 11.5] (n 4) |  | - | 1.01 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 1 [1, 1] (n 1) | 5.93 [5.54, 5.96] (n 3) | yes | 5.92 | 0.53 | yes |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 5.95 [5.94, 5.96] (n 2) |  | - | 0.53 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 5.95 [5.94, 5.96] (n 2) |  | - | 0.53 | yes |

### device reads a partition

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 0.992 [0.991, 0.992] (n 4) | 0.991 [0.99, 0.992] (n 4) | no | 1.00 |  |  |
| r4: four reads in flight | 0.991 [0.991, 0.992] (n 4) | 0.991 [0.99, 0.992] (n 4) | no | 1.00 | 1.00 | no |
| r16: sixteen reads in flight | 0.991 [0.991, 0.992] (n 4) | 0.991 [0.991, 0.992] (n 4) | no | 1.00 | 1.00 | no |
| g4k2: runs across gaps of 4 KiB, two in flight | 0.986 [0.986, 0.987] (n 4) | 0.0903 [0.0891, 0.0907] (n 4) | yes | 0.09 | 0.09 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 0.986 [0.986, 0.987] (n 4) | 0.0903 [0.0891, 0.0908] (n 4) | yes | 0.09 | 0.09 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 0.851 [0.851, 0.851] (n 4) | 0.0904 [0.089, 0.0908] (n 4) | yes | 0.11 | 0.09 | yes |
| t1: three operations a record, one read at a time | 0.992 [0.991, 0.992] (n 4) | 0.991 [0.991, 0.992] (n 4) | no | 1.00 | 1.00 | no |
| t16: three operations a record, sixteen in flight | 0.991 [0.991, 0.992] (n 4) | 0.991 [0.991, 0.992] (n 4) | no | 1.00 | 1.00 | no |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 0.986 [0.986, 0.987] (n 4) | 0.0903 [0.0891, 0.0907] (n 4) | yes | 0.09 | 0.09 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 0.991 [0.991, 0.992] (n 4) |  | - | 1.00 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 0.997 [0.997, 0.997] (n 1) | 0.997 [0.997, 0.998] (n 3) | no | 1.00 | 1.01 | yes |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 0.101 [0.101, 0.101] (n 2) |  | - | 0.10 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 0.101 [0.101, 0.101] (n 2) |  | - | 0.10 | yes |

### partitions a device read

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 |  |  |
| r4: four reads in flight | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 | 1.00 | no |
| r16: sixteen reads in flight | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 | 1.00 | no |
| g4k2: runs across gaps of 4 KiB, two in flight | 1.01 [1.01, 1.01] (n 4) | 11.1 [11, 11.2] (n 4) | yes | 10.92 | 10.97 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 1.01 [1.01, 1.01] (n 4) | 11.1 [11, 11.2] (n 4) | yes | 10.92 | 10.97 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 1.18 [1.17, 1.18] (n 4) | 11.1 [11, 11.2] (n 4) | yes | 9.42 | 10.97 | yes |
| t1: three operations a record, one read at a time | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 | 1.00 | no |
| t16: three operations a record, sixteen in flight | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 | 1.00 | no |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 1.01 [1.01, 1.01] (n 4) | 11.1 [11, 11.2] (n 4) | yes | 10.92 | 10.97 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 1.01 [1.01, 1.01] (n 4) |  | - | 1.00 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 1 [1, 1] (n 1) | 1 [1, 1] (n 3) | no | 1.00 | 0.99 | yes |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 9.88 [9.87, 9.89] (n 2) |  | - | 9.79 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 9.92 [9.91, 9.93] (n 2) |  | - | 9.83 | yes |

### device KiB a partition

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 1.55 [1.55, 1.55] (n 4) | 1.55 [1.55, 1.55] (n 4) | no | 1.00 |  |  |
| r4: four reads in flight | 1.55 [1.55, 1.55] (n 4) | 1.55 [1.55, 1.55] (n 4) | no | 1.00 | 1.00 | no |
| r16: sixteen reads in flight | 1.55 [1.55, 1.55] (n 4) | 1.55 [1.55, 1.55] (n 4) | no | 1.00 | 1.00 | no |
| g4k2: runs across gaps of 4 KiB, two in flight | 1.56 [1.56, 1.56] (n 4) | 1.1 [1.1, 1.1] (n 4) | yes | 0.71 | 0.71 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 1.56 [1.56, 1.56] (n 4) | 1.1 [1.1, 1.1] (n 4) | yes | 0.71 | 0.71 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 10.6 [10.6, 10.7] (n 4) | 1.1 [1.1, 1.1] (n 4) | yes | 0.10 | 0.71 | yes |
| t1: three operations a record, one read at a time | 1.55 [1.55, 1.55] (n 4) | 1.55 [1.55, 1.55] (n 4) | no | 1.00 | 1.00 | no |
| t16: three operations a record, sixteen in flight | 1.55 [1.55, 1.55] (n 4) | 1.55 [1.55, 1.55] (n 4) | no | 1.00 | 1.00 | no |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 1.56 [1.56, 1.56] (n 4) | 1.1 [1.1, 1.1] (n 4) | yes | 0.71 | 0.71 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 1.55 [1.55, 1.55] (n 4) |  | - | 1.00 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 1.57 [1.57, 1.57] (n 1) | 1.57 [1.57, 1.57] (n 3) | no | 1.00 | 1.01 | yes |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 1.2 [1.2, 1.2] (n 2) |  | - | 0.77 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 1.2 [1.2, 1.2] (n 2) |  | - | 0.77 | yes |

### node cpu us a partition

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 38.6 [37.8, 39.8] (n 4) | 37.4 [35.3, 38.7] (n 4) | no | 0.97 |  |  |
| r4: four reads in flight | 21.9 [21.4, 22.1] (n 4) | 21.5 [19.8, 22.4] (n 4) | no | 0.98 | 0.57 | yes |
| r16: sixteen reads in flight | 17.7 [17.5, 18] (n 4) | 17.9 [16.8, 18.6] (n 4) | no | 1.01 | 0.48 | yes |
| g4k2: runs across gaps of 4 KiB, two in flight | 30 [29.1, 30.9] (n 4) | 5.6 [5.2, 5.85] (n 4) | yes | 0.19 | 0.15 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 17.7 [17.5, 17.9] (n 4) | 4.45 [4.14, 4.73] (n 4) | yes | 0.25 | 0.12 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 34.7 [34.1, 35.5] (n 4) | 5.59 [5.2, 5.91] (n 4) | yes | 0.16 | 0.15 | yes |
| t1: three operations a record, one read at a time | 49.2 [46.8, 50.4] (n 4) | 46.4 [43.4, 49.8] (n 4) | no | 0.94 | 1.24 | yes |
| t16: three operations a record, sixteen in flight | 19.3 [19, 19.4] (n 4) | 19.4 [18.1, 20.4] (n 4) | no | 1.00 | 0.52 | yes |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 18 [17.7, 18.3] (n 4) | 4.54 [4.29, 4.73] (n 4) | yes | 0.25 | 0.12 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 39.9 [38.5, 40.7] (n 4) |  | - | 1.07 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 38.5 [38.5, 38.5] (n 1) | 38.4 [38, 38.9] (n 3) | no | 1.00 | 1.03 | no |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 4.97 [4.92, 5.01] (n 2) |  | - | 0.13 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 6.32 [6.23, 6.41] (n 2) |  | - | 0.17 | yes |

### Zen1 hosts' node cpu us a partition

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 46 [45.3, 48.5] (n 4) | 46.4 [45.4, 46.8] (n 4) | no | 1.01 |  |  |
| r4: four reads in flight | 26.7 [26.4, 27] (n 4) | 27.1 [26.7, 27.2] (n 4) | no | 1.01 | 0.58 | yes |
| r16: sixteen reads in flight | 21.4 [21.1, 21.6] (n 4) | 22.3 [21.8, 22.5] (n 4) | yes | 1.04 | 0.48 | yes |
| g4k2: runs across gaps of 4 KiB, two in flight | 36.3 [36, 38] (n 4) | 7.01 [6.97, 7.08] (n 4) | yes | 0.19 | 0.15 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 21.3 [21.1, 21.5] (n 4) | 5.56 [5.49, 5.7] (n 4) | yes | 0.26 | 0.12 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 42.9 [42, 43.5] (n 4) | 7.04 [6.9, 7.11] (n 4) | yes | 0.16 | 0.15 | yes |
| t1: three operations a record, one read at a time | 58.4 [56.8, 60.1] (n 4) | 58.5 [56.8, 59] (n 4) | no | 1.00 | 1.26 | yes |
| t16: three operations a record, sixteen in flight | 23.2 [22.8, 23.6] (n 4) | 24.2 [23.7, 24.4] (n 4) | yes | 1.04 | 0.52 | yes |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 21.5 [21.3, 21.9] (n 4) | 5.64 [5.58, 5.69] (n 4) | yes | 0.26 | 0.12 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 47.6 [46.5, 48.8] (n 4) |  | - | 1.03 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 45.9 [45.9, 45.9] (n 1) | 46.9 [46.2, 47] (n 3) | yes | 1.02 | 1.01 | no |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 5.96 [5.96, 5.96] (n 2) |  | - | 0.13 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 7.6 [7.56, 7.64] (n 2) |  | - | 0.16 | yes |

### europa's node cpu us a partition

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 18.2 [17.6, 18.6] (n 4) | 17.3 [16.5, 18.1] (n 4) | no | 0.95 |  |  |
| r4: four reads in flight | 7.97 [7.71, 8.07] (n 4) | 7.77 [7.55, 8.14] (n 4) | no | 0.97 | 0.45 | yes |
| r16: sixteen reads in flight | 7.23 [7.09, 7.63] (n 4) | 7.47 [7.18, 7.83] (n 4) | no | 1.03 | 0.43 | yes |
| g4k2: runs across gaps of 4 KiB, two in flight | 12.2 [11.6, 12.7] (n 4) | 2.09 [2, 2.17] (n 4) | yes | 0.17 | 0.12 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 7.45 [7.2, 7.77] (n 4) | 1.71 [1.66, 1.75] (n 4) | yes | 0.23 | 0.10 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 13.9 [13.6, 14.2] (n 4) | 2.09 [2, 2.21] (n 4) | yes | 0.15 | 0.12 | yes |
| t1: three operations a record, one read at a time | 24 [22.4, 24.4] (n 4) | 22.3 [21.3, 24.1] (n 4) | no | 0.93 | 1.29 | yes |
| t16: three operations a record, sixteen in flight | 8 [7.81, 8.23] (n 4) | 8.17 [7.84, 8.33] (n 4) | no | 1.02 | 0.47 | yes |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 7.82 [7.67, 8.12] (n 4) | 1.78 [1.78, 1.96] (n 4) | yes | 0.23 | 0.10 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 20.9 [20, 21.7] (n 4) |  | - | 1.21 | yes |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 18 [18, 18] (n 1) | 18.5 [17.8, 18.6] (n 3) | no | 1.02 | 1.07 | no |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 1.97 [1.96, 1.98] (n 2) |  | - | 0.11 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 2.46 [2.44, 2.48] (n 2) |  | - | 0.14 | yes |

### Zen1 hosts' partitions a device read

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 |  |  |
| r4: four reads in flight | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 | 1.00 | no |
| r16: sixteen reads in flight | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 | 1.00 | no |
| g4k2: runs across gaps of 4 KiB, two in flight | 1.01 [1.01, 1.01] (n 4) | 10.6 [10.6, 10.6] (n 4) | yes | 10.43 | 10.48 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 1.01 [1.01, 1.01] (n 4) | 10.6 [10.6, 10.6] (n 4) | yes | 10.43 | 10.48 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 1.17 [1.17, 1.17] (n 4) | 10.6 [10.6, 10.6] (n 4) | yes | 9.03 | 10.48 | yes |
| t1: three operations a record, one read at a time | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 | 1.00 | no |
| t16: three operations a record, sixteen in flight | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 | 1.00 | no |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 1.01 [1.01, 1.01] (n 4) | 10.6 [10.6, 10.6] (n 4) | yes | 10.43 | 10.48 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 1.01 [1.01, 1.01] (n 4) |  | - | 1.00 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 1 [1, 1] (n 1) | 1 [1, 1] (n 3) | no | 1.00 | 0.99 | yes |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 9.51 [9.51, 9.51] (n 2) |  | - | 9.42 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 9.54 [9.54, 9.54] (n 2) |  | - | 9.45 | yes |

### europa's partitions a device read

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 |  |  |
| r4: four reads in flight | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 | 1.00 | no |
| r16: sixteen reads in flight | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 | 1.00 | no |
| g4k2: runs across gaps of 4 KiB, two in flight | 1.01 [1.01, 1.02] (n 4) | 12.6 [12.6, 12.6] (n 4) | yes | 12.42 | 12.48 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 1.01 [1.01, 1.01] (n 4) | 12.6 [12.6, 12.6] (n 4) | yes | 12.42 | 12.48 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 1.19 [1.18, 1.19] (n 4) | 12.6 [12.6, 12.6] (n 4) | yes | 10.63 | 12.48 | yes |
| t1: three operations a record, one read at a time | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 | 1.00 | no |
| t16: three operations a record, sixteen in flight | 1.01 [1.01, 1.01] (n 4) | 1.01 [1.01, 1.01] (n 4) | no | 1.00 | 1.00 | no |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 1.01 [1.01, 1.02] (n 4) | 12.6 [12.6, 12.6] (n 4) | yes | 12.42 | 12.48 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 1.01 [1.01, 1.01] (n 4) |  | - | 1.00 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 1 [1, 1] (n 1) | 1 [1, 1] (n 3) | no | 1.00 | 1.00 | yes |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 11.2 [11.2, 11.3] (n 2) |  | - | 11.11 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 11.3 [11.2, 11.3] (n 2) |  | - | 11.18 | yes |

### host cpu us a partition

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 38.2 [36.9, 39.5] (n 4) | 36.8 [35.6, 38.7] (n 4) | no | 0.96 |  |  |
| r4: four reads in flight | 21.9 [21.5, 22.1] (n 4) | 21.7 [19.6, 22.6] (n 4) | no | 0.99 | 0.59 | yes |
| r16: sixteen reads in flight | 18.1 [17.8, 18.7] (n 4) | 18.2 [16.9, 18.9] (n 4) | no | 1.01 | 0.50 | yes |
| g4k2: runs across gaps of 4 KiB, two in flight | 29.6 [28.9, 30.7] (n 4) | 5.75 [5.39, 6.12] (n 4) | yes | 0.19 | 0.16 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 18 [17.8, 18.2] (n 4) | 4.61 [4.33, 5.46] (n 4) | yes | 0.26 | 0.13 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 34.4 [33.5, 34.8] (n 4) | 5.77 [5.28, 5.97] (n 4) | yes | 0.17 | 0.16 | yes |
| t1: three operations a record, one read at a time | 49.3 [46.4, 51] (n 4) | 45.6 [42.9, 48.2] (n 4) | no | 0.92 | 1.24 | yes |
| t16: three operations a record, sixteen in flight | 19.7 [19.5, 20.1] (n 4) | 19.7 [18.3, 20.6] (n 4) | no | 1.00 | 0.54 | yes |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 18.4 [17.9, 18.5] (n 4) | 4.65 [4.34, 4.94] (n 4) | yes | 0.25 | 0.13 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 39.5 [37.9, 40.4] (n 4) |  | - | 1.07 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 37.6 [37.6, 37.6] (n 1) | 37.3 [37, 37.4] (n 3) | yes | 0.99 | 1.01 | no |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 5.05 [4.98, 5.12] (n 2) |  | - | 0.14 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 6.37 [6.17, 6.57] (n 2) |  | - | 0.17 | yes |

### pages shed a page

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 18.6 [16.8, 31.9] (n 4) | 19.2 [15.7, 21.1] (n 4) | no | 1.03 |  |  |
| r4: four reads in flight | 8.36 [7.18, 10] (n 4) | 8.14 [6.83, 8.86] (n 4) | no | 0.97 | 0.43 | yes |
| r16: sixteen reads in flight | 6.2 [5.48, 7.2] (n 4) | 6.15 [5.38, 7.05] (n 4) | no | 0.99 | 0.32 | yes |
| g4k2: runs across gaps of 4 KiB, two in flight | 12.6 [10.8, 19.1] (n 4) | 2.96 [2.65, 3.08] (n 4) | yes | 0.23 | 0.15 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 5.83 [4.98, 7.17] (n 4) | 2.13 [1.96, 2.52] (n 4) | yes | 0.36 | 0.11 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 17.4 [15.7, 22.6] (n 4) | 2.94 [2.6, 3.2] (n 4) | yes | 0.17 | 0.15 | yes |
| t1: three operations a record, one read at a time | 25.6 [17.4, 32.2] (n 4) | 21.7 [16.1, 28] (n 4) | no | 0.85 | 1.13 | no |
| t16: three operations a record, sixteen in flight | 6.31 [5.56, 9.02] (n 4) | 6.51 [5.68, 7.76] (n 4) | no | 1.03 | 0.34 | yes |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 7.11 [5.04, 9.72] (n 4) | 2.48 [1.96, 2.54] (n 4) | yes | 0.35 | 0.13 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 23.7 [20.4, 25.8] (n 4) |  | - | 1.24 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 19.8 [19.8, 19.8] (n 1) | 23.3 [20.6, 26.2] (n 3) | yes | 1.18 | 1.22 | no |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 2.73 [2.63, 2.84] (n 2) |  | - | 0.14 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 3.87 [3.84, 3.9] (n 2) |  | - | 0.20 | yes |

### page read p50 ms

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 94.7 [94.7, 96.3] (n 4) | 94.5 [92.2, 95.2] (n 4) | no | 1.00 |  |  |
| r4: four reads in flight | 38.1 [37.9, 38.4] (n 4) | 38.8 [36.1, 39.7] (n 4) | no | 1.02 | 0.41 | yes |
| r16: sixteen reads in flight | 29.2 [28.4, 29.4] (n 4) | 31.1 [27.5, 32.1] (n 4) | no | 1.06 | 0.33 | yes |
| g4k2: runs across gaps of 4 KiB, two in flight | 57.6 [57.3, 58.1] (n 4) | 10.7 [9.92, 10.9] (n 4) | yes | 0.18 | 0.11 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 28 [26.8, 28] (n 4) | 6.78 [5.98, 6.94] (n 4) | yes | 0.24 | 0.07 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 99.6 [96.3, 100] (n 4) | 10.6 [9.92, 10.8] (n 4) | yes | 0.11 | 0.11 | yes |
| t1: three operations a record, one read at a time | 107 [106, 109] (n 4) | 106 [103, 108] (n 4) | no | 0.99 | 1.12 | yes |
| t16: three operations a record, sixteen in flight | 30 [28.9, 30.2] (n 4) | 31.4 [28.2, 32.5] (n 4) | no | 1.05 | 0.33 | yes |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 56.1 [54, 56.6] (n 4) | 13.5 [11.8, 14.5] (n 4) | yes | 0.24 | 0.14 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 91.6 [90.1, 92.2] (n 4) |  | - | 0.97 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 104 [104, 104] (n 1) | 104 [104, 105] (n 3) | no | 1.00 | 1.11 | yes |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 8.41 [8.26, 8.57] (n 2) |  | - | 0.09 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 12.9 [12.7, 13] (n 2) |  | - | 0.14 | yes |

### page read p99 ms

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 114 [112, 123] (n 4) | 114 [111, 115] (n 4) | no | 1.00 |  |  |
| r4: four reads in flight | 47.7 [47.4, 49.2] (n 4) | 49.7 [46.6, 49.9] (n 4) | no | 1.04 | 0.44 | yes |
| r16: sixteen reads in flight | 37.6 [37.4, 38.7] (n 4) | 40.7 [39.4, 42] (n 4) | yes | 1.08 | 0.36 | yes |
| g4k2: runs across gaps of 4 KiB, two in flight | 71.2 [70.1, 74.2] (n 4) | 14.5 [14.3, 15.4] (n 4) | yes | 0.20 | 0.13 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 37.1 [36.9, 37.9] (n 4) | 11.4 [10.8, 11.7] (n 4) | yes | 0.31 | 0.10 | yes |
| g128k2: runs across gaps of 128 KiB, two in flight | 120 [119, 122] (n 4) | 14.8 [14.5, 15.1] (n 4) | yes | 0.12 | 0.13 | yes |
| t1: three operations a record, one read at a time | 130 [128, 135] (n 4) | 130 [126, 133] (n 4) | no | 1.00 | 1.14 | yes |
| t16: three operations a record, sixteen in flight | 39.3 [38.9, 39.4] (n 4) | 41.7 [40.7, 42.2] (n 4) | yes | 1.06 | 0.37 | yes |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 74.8 [73.7, 77.3] (n 4) | 22.7 [22.4, 23.7] (n 4) | yes | 0.30 | 0.20 | yes |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 111 [109, 112] (n 4) |  | - | 0.98 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 124 [124, 124] (n 1) | 124 [124, 126] (n 3) | no | 1.00 | 1.09 | yes |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 13.6 [13.4, 13.8] (n 2) |  | - | 0.12 | yes |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 18.2 [17.9, 18.4] (n 2) |  | - | 0.16 | yes |

### page held p99 ms

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 0.116 [0.105, 0.123] (n 4) | 0.11 [0.106, 0.118] (n 4) | no | 0.94 |  |  |
| r4: four reads in flight | 0.122 [0.107, 0.135] (n 4) | 0.116 [0.107, 0.127] (n 4) | no | 0.95 | 1.06 | no |
| r16: sixteen reads in flight | 0.121 [0.101, 0.139] (n 4) | 0.11 [0.107, 0.138] (n 4) | no | 0.90 | 1.00 | no |
| g4k2: runs across gaps of 4 KiB, two in flight | 0.0965 [0.069, 0.107] (n 4) | 0.102 [0.092, 0.104] (n 4) | no | 1.06 | 0.93 | yes |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 0.113 [0.078, 0.119] (n 4) | 0.103 [0.086, 0.107] (n 4) | no | 0.92 | 0.95 | no |
| g128k2: runs across gaps of 128 KiB, two in flight | 0.094 [0.078, 0.103] (n 4) | 0.0975 [0.073, 0.103] (n 4) | no | 1.04 | 0.89 | yes |
| t1: three operations a record, one read at a time | 0.11 [0.092, 0.118] (n 4) | 0.111 [0.098, 0.126] (n 4) | no | 1.01 | 1.01 | no |
| t16: three operations a record, sixteen in flight | 0.12 [0.101, 0.131] (n 4) | 0.116 [0.109, 0.138] (n 4) | no | 0.97 | 1.06 | no |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 0.123 [0.097, 0.128] (n 4) | 0.109 [0.107, 0.112] (n 4) | no | 0.89 | 1.00 | no |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 0.103 [0.101, 0.119] (n 4) |  | - | 0.95 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 0.1 [0.1, 0.1] (n 1) | 0.102 [0.1, 0.109] (n 3) | no | 1.02 | 0.93 | no |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 0.102 [0.096, 0.107] (n 2) |  | - | 0.93 | no |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 0.0925 [0.089, 0.096] (n 2) |  | - | 0.84 | yes |

### page paced p50 ms

| Arm | Hash order | Key order | Disjoint | Key / hash | Key's against r1 | Disjoint from r1 |
| --- | --- | --- | --- | --- | --- | --- |
| r1: one read at a time, today's walk | 0.001 [0.001, 0.001] (n 4) | 0.001 [0.001, 0.001] (n 4) | no | 1.00 |  |  |
| r4: four reads in flight | 0.001 [0.001, 0.001] (n 4) | 0.001 [0.001, 0.001] (n 4) | no | 1.00 | 1.00 | no |
| r16: sixteen reads in flight | 0.001 [0.001, 0.001] (n 4) | 0.001 [0.001, 0.001] (n 4) | no | 1.00 | 1.00 | no |
| g4k2: runs across gaps of 4 KiB, two in flight | 0.001 [0.001, 0.001] (n 4) | 0.001 [0.001, 0.001] (n 4) | no | 1.00 | 1.00 | no |
| g4k16: runs across gaps of 4 KiB, sixteen in flight | 0.001 [0.001, 0.001] (n 4) | 0.001 [0.001, 0.001] (n 4) | no | 1.00 | 1.00 | no |
| g128k2: runs across gaps of 128 KiB, two in flight | 0.001 [0.001, 0.001] (n 4) | 0.001 [0.001, 0.001] (n 4) | no | 1.00 | 1.00 | no |
| t1: three operations a record, one read at a time | 0.001 [0.001, 0.001] (n 4) | 0.001 [0.001, 0.001] (n 4) | no | 1.00 | 1.00 | no |
| t16: three operations a record, sixteen in flight | 0.001 [0.001, 0.001] (n 4) | 0.001 [0.001, 0.001] (n 4) | no | 1.00 | 1.00 | no |
| g4k16p4: g4k16's reads at four pages a shard and 64 streams | 0.001 [0.001, 0.001] (n 4) | 0.001 [0.001, 0.001] (n 4) | no | 1.00 | 1.00 | no |
| shedstay: today's walk, the driver staying on a shed tablet as before O139 | - | 0.001 [0.001, 0.001] (n 4) |  | - | 1.00 | no |
| afterchurn: today's walk after twenty minutes of inserts and overwrites into Big | 0.001 [0.001, 0.001] (n 1) | 0.001 [0.001, 0.001] (n 3) | no | 1.00 | 1.00 | no |
| afterchurn-g4k16: g4k16 after the same churn, the churn's supplement | - | 0.001 [0.001, 0.001] (n 2) |  | - | 1.00 | no |
| afterchurn-g128k2: g128k2 after the same churn, the churn's supplement | - | 0.001 [0.001, 0.001] (n 2) |  | - | 1.00 | no |

### cell r1

| Figure | Hash order | Key order | Disjoint |
| --- | --- | --- | --- |
| insert p99 during / before | 1.46 [1.32, 1.57] (n 4) | 1.3 [1.13, 1.48] (n 4) | no |
| insert p99 ms before | 23.2 [22.3, 24.6] (n 4) | 24.6 [24.2, 25.2] (n 4) | no |
| insert p99 ms during | 34 [29.6, 38.6] (n 4) | 32.1 [27.7, 36.6] (n 4) | no |
| read p99 during / before | 1.08 [1.06, 1.11] (n 4) | 1.11 [1.09, 1.12] (n 4) | no |
| read p99 ms before | 5.48 [5.15, 5.85] (n 4) | 5.81 [5.7, 5.98] (n 4) | no |
| read p99 ms during | 5.87 [5.56, 6.47] (n 4) | 6.44 [6.34, 6.66] (n 4) | no |
| scan partitions a second during | 3.69e+04 [2.86e+04, 4.69e+04] (n 4) | 4.65e+04 [3.33e+04, 5.45e+04] (n 4) | no |

### cell g4k16

| Figure | Hash order | Key order | Disjoint |
| --- | --- | --- | --- |
| insert p99 during / before | 1.64 [1.56, 1.68] (n 4) | 1.2 [1.03, 1.3] (n 4) | yes |
| insert p99 ms before | 24.5 [23.2, 24.8] (n 4) | 24.3 [23.6, 25.4] (n 4) | no |
| insert p99 ms during | 39.8 [37.8, 40.9] (n 4) | 28.6 [26.2, 32] (n 4) | yes |
| read p99 during / before | 1.57 [1.51, 1.64] (n 4) | 1.28 [1.18, 1.38] (n 4) | yes |
| read p99 ms before | 5.5 [5.38, 6.16] (n 4) | 6.09 [5.75, 6.28] (n 4) | no |
| read p99 ms during | 8.54 [8.26, 10.1] (n 4) | 7.73 [7.02, 8.52] (n 4) | no |
| scan partitions a second during | 2e+05 [1.76e+05, 2.14e+05] (n 4) | 2.4e+05 [2.14e+05, 2.5e+05] (n 4) | no |

### gets

| Figure | Hash order | Key order | Disjoint |
| --- | --- | --- | --- |
| get p50 ms | 0.183 [0.181, 0.188] (n 4) | 0.182 [0.179, 0.184] (n 4) | no |
| get p99 ms | 0.938 [0.867, 1.02] (n 4) | 0.921 [0.862, 0.973] (n 4) | no |
| gets a second | 1.06e+05 [1.06e+05, 1.07e+05] (n 4) | 1.07e+05 [1.07e+05, 1.08e+05] (n 4) | no |

### load

| Figure | Hash order | Key order | Disjoint |
| --- | --- | --- | --- |
| preload secs | 109 [105, 116] (n 4) | 116 [106, 124] (n 6) | no |

### merges while loading Big

| Figure | Hash order | Key order | Disjoint |
| --- | --- | --- | --- |
| entries a merge, median | 9.15e+03 [9.15e+03, 9.15e+03] (n 4) | 9.15e+03 [9.15e+03, 9.15e+03] (n 6) | no |
| merges | 1.06e+03 [1.06e+03, 1.06e+03] (n 4) | 1.06e+03 [1.06e+03, 1.06e+03] (n 6) | no |
| order KiB a merge, most | 215 [215, 215] (n 4) | 215 [215, 215] (n 6) | no |
| sort ns an entry | 0.000544 [0, 0.00467] (n 4) | 34.9 [34.7, 35.3] (n 6) | yes |
| sort us a merge, median | 0 [0, 0] (n 4) | 301 [300, 303] (n 6) | yes |
| sort us a merge, most | 2.5 [0, 42] (n 4) | 1.24e+03 [1.04e+03, 1.55e+03] (n 6) | yes |
| sort us a merge, p99 | 0 [0, 0] (n 4) | 840 [764, 873] (n 6) | yes |
| sort's share of a merge's time, percent | 7e-06 [0, 5.75e-05] (n 4) | 0.444 [0.432, 0.451] (n 6) | yes |
| write ms a merge, median | 76.9 [73.6, 81.4] (n 4) | 75.4 [74.5, 79.8] (n 6) | no |

### merges while churning Big

| Figure | Hash order | Key order | Disjoint |
| --- | --- | --- | --- |
| entries a merge, median | 8.81e+03 [8.81e+03, 8.81e+03] (n 1) | 8.81e+03 [8.81e+03, 8.81e+03] (n 3) | no |
| merges | 631 [631, 631] (n 1) | 640 [626, 645] (n 3) | no |
| order KiB a merge, most | 207 [207, 207] (n 1) | 214 [207, 214] (n 3) | yes |
| sort ns an entry | 0 [0, 0] (n 1) | 30.2 [29.8, 30.2] (n 3) | yes |
| sort us a merge, median | 0 [0, 0] (n 1) | 287 [286, 288] (n 3) | yes |
| sort us a merge, most | 0 [0, 0] (n 1) | 636 [597, 639] (n 3) | yes |
| sort us a merge, p99 | 0 [0, 0] (n 1) | 480 [480, 501] (n 3) | yes |
| sort's share of a merge's time, percent | 0 [0, 0] (n 1) | 0.772 [0.73, 0.776] (n 3) | yes |
| write ms a merge, median | 41.1 [41.1, 41.1] (n 1) | 41.7 [41.3, 42.4] (n 3) | yes |

### busy threads with the cluster idle, a minute a round

| Host | Busy threads |
| --- | --- |
| europa | 0.178 [0.118, 0.259] (n 4) |
| hyperion | 0.258 [0.241, 0.273] (n 4) |
| titan | 0.257 [0.239, 0.265] (n 4) |

### titan's node under a scan, its self time by kind of work

| Kind | key-g4k16 | key-r1 |
| --- | --- | --- |
| user: shoal | 17.8% | 3.0% |
| kernel: io_uring | 5.5% | 12.5% |
| kernel: other | 6.4% | 4.8% |
| kernel: syscall entry, exit and SRSO | 3.2% | 7.4% |
| user: allocator | 7.7% | 2.9% |
| user: glommio and liburing | 3.9% | 6.5% |
| kernel: block, nvme, iomap, xfs | 4.7% | 4.5% |
| user: clocks | 5.5% | 3.1% |
| kernel: memory | 2.8% | 3.8% |
| kernel: IPIs and membarrier | 1.2% | 4.5% |
| user: other (x16-node) | 3.1% | 2.0% |
| kernel: scheduling and futex | 1.1% | 3.7% |
| user: rkyv validation | 4.6% | 0.2% |
| kernel: locks and rcu | 1.5% | 2.8% |
| user: kanal and futures | 3.7% | 0.1% |
| kernel: timers and clocks | 0.8% | 1.8% |
| kernel: files and descriptors | 0.7% | 1.5% |
| user: openraft | 0.5% | 0.2% |
| kernel: copies | 0.3% | 0.4% |
| kernel: network | 0.2% | 0.0% |
| every symbol the report's top holds | 75.0% | 65.6% |
