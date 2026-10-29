### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.53 KiB a sync · sync p50 11.14 µs |
| fdatasync of a clean file | p50 10.71 µs, p99 16.93 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 72.53 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 74.14 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.18 µs, p99 16.15 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 4 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 600.0 | 43.29 | 0.040 | 370.6 | 21.20 | 29.30 | 429.9 | 1021 | 5001 | 501.8 | 4187 | 65.43 | 0 | 194.0 | 15.28 | 1.00 |
| 64K | 1 | class-pool | 600.0 | 42.24 | 0.041 | 70.05 | 20.25 | 31.24 | 121.5 | 294.6 | 615.9 | 3137 | 146.8 | 2.29 | 0 | 13.00 | 2.03 | 6.25 |
| 64K | 1 | class-fresh | 600.0 | 42.27 | 28.41 | 68.38 | 46.59 | 26.85 | 59.23 | 273.2 | 329.3 | 3524 | 76.91 | 1.20 | 0 | 0 | 0.000 | 7.02 |
| 64K | 8 | today | 600.0 | 14967 | 0.031 | 329.9 | 50.04 | 44.06 | 912.4 | 16200 | 21101 | 540.0 | 4184 | 65.38 | 0 | 505.0 | 13.34 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 864.3 | 0.040 | 131.4 | 87.46 | 60.77 | 212.0 | 1374 | 1855 | 6093 | 144.0 | 2.25 | 0 | 466.0 | 3.08 | 11.28 |
| 64K | 8 | class-fresh | 600.0 | 137.8 | 102.4 | 186.4 | 116.1 | 53.84 | 191.4 | 870.0 | 1489 | 8570 | 73.06 | 1.14 | 0 | 0 | 0.000 | 15.87 |
| 256K | 1 | today | 512.0 | 43.70 | 0.040 | 441.4 | 19.76 | 29.89 | 428.8 | 1064 | 5268 | 491.4 | 4381 | 17.11 | 0 | 163.0 | 15.28 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 32.60 | 0.050 | 183.9 | 17.07 | 40.56 | 149.4 | 439.1 | 684.6 | 2236 | 532.6 | 2.08 | 0 | 1.00 | 3.05 | 4.55 |
| 256K | 1 | class-fresh | 512.0 | 42.27 | 28.25 | 143.7 | 50.34 | 27.01 | 53.94 | 345.0 | 395.6 | 2775 | 269.8 | 1.05 | 0 | 0 | 0.000 | 5.65 |
| 256K | 8 | today | 512.0 | 14609 | 0.040 | 600.4 | 34.25 | 31.42 | 1712 | 17106 | 25161 | 502.9 | 4388 | 17.14 | 0 | 425.0 | 14.37 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 1429 | 0.040 | 341.5 | 28.06 | 29.18 | 601.8 | 2461 | 3342 | 3464 | 530.3 | 2.07 | 0 | 404.0 | 3.07 | 6.89 |
| 256K | 8 | class-fresh | 512.0 | 126.5 | 97.96 | 468.6 | 375.5 | 44.58 | 199.2 | 1355 | 2038 | 5722 | 266.2 | 1.04 | 0 | 0 | 0.000 | 11.38 |
| 1M | 1 | today | 128.0 | 43.63 | 0.040 | 775.3 | 19.71 | 30.20 | 438.7 | 1428 | 6035 | 417.3 | 5143 | 5.02 | 0 | 37.00 | 19.32 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 94.37 | 0.050 | 791.8 | 26.08 | 32.56 | 89.08 | 1036 | 1817 | 960.2 | 2065 | 2.02 | 0 | 2.00 | 5.08 | 2.30 |
| 1M | 1 | class-fresh | 128.0 | 42.37 | 24.96 | 439.2 | 45.32 | 27.53 | 53.81 | 634.3 | 681.5 | 1535 | 1034 | 1.01 | 0 | 0 | 0.000 | 3.68 |
| 1M | 8 | today | 128.0 | 14566 | 0.031 | 1481 | 21.98 | 28.83 | 2268 | 18463 | 22936 | 495.8 | 5143 | 5.02 | 0 | 102.0 | 15.33 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 4317 | 0.040 | 1163 | 15.90 | 27.85 | 1722 | 7517 | 9814 | 1161 | 2065 | 2.02 | 0 | 100.0 | 5.08 | 2.34 |
| 1M | 8 | class-fresh | 128.0 | 122.7 | 96.38 | 1796 | 1524 | 58.19 | 107.4 | 3791 | 6987 | 2047 | 1033 | 1.01 | 0 | 0 | 0.000 | 4.13 |
| 4M | 1 | today | 32.00 | 43.09 | 0.050 | 1957 | 15.16 | 31.32 | 445.7 | 2555 | 6279 | 316.7 | 8215 | 2.01 | 0 | 6.00 | 13.31 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 40.10 | 0.050 | 1946 | 19.60 | 31.36 | 449.3 | 2595 | 6318 | 317.6 | 8217 | 2.01 | 0 | 6.00 | 14.25 | 1.00 |
| 4M | 1 | class-fresh | 32.00 | 46.18 | 25.67 | 1867 | 58.65 | 30.04 | 66.13 | 2102 | 2446 | 469.4 | 4105 | 1.00 | 0 | 0 | 0.000 | 1.48 |
| 4M | 8 | today | 32.00 | 15944 | 0.030 | 5866 | 21.68 | 29.55 | 5881 | 29174 | 34172 | 313.3 | 8211 | 2.00 | 0 | 22.00 | 13.33 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 15196 | 0.030 | 6215 | 21.45 | 28.93 | 5308 | 23662 | 33297 | 329.2 | 8210 | 2.00 | 0 | 22.00 | 12.21 | 1.05 |
| 4M | 8 | class-fresh | 32.00 | 136.1 | 266.8 | 7783 | 6525 | 59.52 | 100.3 | 13515 | 14243 | 583.8 | 4106 | 1.00 | 0 | 0 | 0.000 | 1.86 |

