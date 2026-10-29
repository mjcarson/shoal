### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.53 KiB a sync · sync p50 11.18 µs |
| fdatasync of a clean file | p50 10.73 µs, p99 16.28 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 72.36 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 73.81 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.26 µs, p99 16.11 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 2 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 600.0 | 42.68 | 0.040 | 369.7 | 21.71 | 29.52 | 426.8 | 984.3 | 4988 | 506.6 | 4188 | 65.43 | 0 | 193.0 | 17.35 | 1.00 |
| 64K | 1 | class-pool | 600.0 | 42.30 | 0.050 | 69.75 | 20.33 | 31.26 | 124.8 | 296.0 | 606.0 | 3153 | 147.0 | 2.30 | 0 | 11.00 | 2.04 | 6.22 |
| 64K | 1 | class-fresh | 600.0 | 42.62 | 28.43 | 67.50 | 46.28 | 26.94 | 59.44 | 273.4 | 311.5 | 3535 | 76.66 | 1.20 | 0 | 0 | 0.000 | 6.98 |
| 64K | 8 | today | 600.0 | 14590 | 0.040 | 402.1 | 55.46 | 45.02 | 1127 | 16147 | 21769 | 541.2 | 4185 | 65.39 | 0 | 501.0 | 14.28 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 883.5 | 0.031 | 133.1 | 95.99 | 62.61 | 206.5 | 1415 | 1913 | 5878 | 144.2 | 2.25 | 0 | 467.0 | 2.03 | 10.86 |
| 64K | 8 | class-fresh | 600.0 | 127.1 | 91.42 | 241.5 | 106.6 | 51.55 | 169.5 | 872.4 | 1907 | 8370 | 71.73 | 1.12 | 0 | 0 | 0.000 | 15.46 |
| 256K | 1 | today | 512.0 | 43.47 | 0.050 | 448.4 | 19.87 | 29.95 | 433.9 | 1053 | 5137 | 493.5 | 4380 | 17.11 | 0 | 157.0 | 16.27 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 31.78 | 0.050 | 181.6 | 17.29 | 31.70 | 150.4 | 422.1 | 598.7 | 2298 | 531.3 | 2.08 | 0 | 2.00 | 3.05 | 4.66 |
| 256K | 1 | class-fresh | 512.0 | 43.28 | 29.84 | 148.0 | 52.53 | 26.97 | 53.87 | 354.8 | 399.0 | 2735 | 271.5 | 1.06 | 0 | 0 | 0.000 | 5.54 |
| 256K | 8 | today | 512.0 | 15124 | 0.040 | 568.0 | 35.16 | 30.82 | 1576 | 17433 | 20719 | 517.2 | 4377 | 17.10 | 0 | 425.0 | 15.35 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 1364 | 0.040 | 309.3 | 27.56 | 29.39 | 533.2 | 2297 | 3148 | 3704 | 529.3 | 2.07 | 0 | 404.0 | 3.05 | 7.16 |
| 256K | 8 | class-fresh | 512.0 | 122.1 | 86.45 | 448.4 | 389.7 | 43.29 | 203.3 | 1352 | 2188 | 5671 | 265.6 | 1.04 | 0 | 0 | 0.000 | 10.96 |
| 1M | 1 | today | 128.0 | 42.62 | 0.050 | 755.1 | 19.91 | 30.18 | 437.6 | 1335 | 6126 | 429.2 | 5146 | 5.03 | 0 | 37.00 | 19.32 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 87.87 | 0.050 | 783.6 | 19.17 | 31.37 | 102.9 | 1011 | 2212 | 969.9 | 2064 | 2.02 | 0 | 4.00 | 4.07 | 2.26 |
| 1M | 1 | class-fresh | 128.0 | 42.50 | 24.91 | 440.6 | 45.44 | 27.58 | 53.95 | 636.0 | 675.3 | 1533 | 1034 | 1.01 | 0 | 0 | 0.000 | 3.57 |
| 1M | 8 | today | 128.0 | 13850 | 0.040 | 1533 | 21.68 | 29.96 | 2825 | 18196 | 21605 | 501.6 | 5147 | 5.03 | 0 | 102.0 | 15.34 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 4653 | 0.030 | 1584 | 19.09 | 27.81 | 1782 | 8263 | 10600 | 1059 | 2066 | 2.02 | 0 | 99.00 | 5.08 | 2.11 |
| 1M | 8 | class-fresh | 128.0 | 117.9 | 98.05 | 1744 | 1302 | 55.16 | 148.7 | 3767 | 6384 | 2045 | 1034 | 1.01 | 0 | 0 | 0.000 | 4.08 |
| 4M | 1 | today | 32.00 | 34.00 | 0.050 | 1927 | 19.52 | 31.73 | 410.9 | 2470 | 5770 | 324.2 | 8214 | 2.01 | 0 | 7.00 | 16.27 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 39.10 | 0.050 | 2148 | 19.88 | 31.25 | 467.3 | 2841 | 6798 | 283.7 | 8217 | 2.01 | 0 | 7.00 | 15.27 | 0.875 |
| 4M | 1 | class-fresh | 32.00 | 46.02 | 25.67 | 1717 | 59.69 | 29.60 | 66.50 | 1946 | 2413 | 507.0 | 4108 | 1.00 | 0 | 0 | 0.000 | 1.56 |
| 4M | 8 | today | 32.00 | 14977 | 0.030 | 4970 | 21.67 | 29.38 | 4956 | 22923 | 32180 | 334.7 | 8211 | 2.00 | 0 | 21.00 | 14.25 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 14897 | 0.030 | 6254 | 21.05 | 29.01 | 4998 | 23887 | 35152 | 336.8 | 8210 | 2.00 | 0 | 21.00 | 14.33 | 1.01 |
| 4M | 8 | class-fresh | 32.00 | 136.7 | 154.1 | 7641 | 6497 | 60.38 | 145.2 | 13525 | 14137 | 587.8 | 4106 | 1.00 | 0 | 0 | 0.000 | 1.76 |

