### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 11.24 µs |
| fdatasync of a clean file | p50 10.78 µs, p99 16.36 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 0 flushes a rename | p50 72.98 µs |
| rename, then the directory's Fsync | 3 KiB and 0 flushes a rename | p50 73.78 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.25 µs, p99 16.98 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 1 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa ssd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 600.0 | 43.22 | 0.040 | 372.2 | 21.50 | 29.40 | 431.7 | 1017 | 5602 | 489.5 | 4186 | 65.41 | 0 | 194.0 | 14.27 | 1.00 |
| 64K | 1 | class-pool | 600.0 | 42.60 | 0.050 | 68.83 | 20.44 | 31.27 | 126.9 | 300.7 | 578.1 | 3109 | 148.1 | 2.31 | 0 | 7.00 | 2.03 | 6.35 |
| 64K | 1 | class-fresh | 600.0 | 42.48 | 28.85 | 68.25 | 47.80 | 26.83 | 59.33 | 274.1 | 337.1 | 3510 | 77.77 | 1.22 | 0 | 0 | 0.000 | 7.17 |
| 64K | 8 | today | 600.0 | 14141 | 0.040 | 349.4 | 49.57 | 44.90 | 1090 | 16067 | 19868 | 555.0 | 4185 | 65.38 | 0 | 504.0 | 14.23 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 834.9 | 0.040 | 138.1 | 91.62 | 61.02 | 218.7 | 1359 | 1764 | 6113 | 144.0 | 2.25 | 0 | 466.0 | 2.03 | 11.01 |
| 64K | 8 | class-fresh | 600.0 | 131.5 | 92.79 | 197.6 | 118.8 | 49.24 | 177.8 | 850.3 | 1692 | 8679 | 72.25 | 1.13 | 0 | 0 | 0.000 | 15.64 |
| 256K | 1 | today | 512.0 | 44.16 | 0.050 | 448.0 | 19.81 | 29.95 | 432.1 | 1072 | 5403 | 486.6 | 4380 | 17.11 | 0 | 161.0 | 14.36 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 30.89 | 0.041 | 179.0 | 19.63 | 40.97 | 155.9 | 431.9 | 649.2 | 2276 | 533.1 | 2.08 | 0 | 1.00 | 3.05 | 4.68 |
| 256K | 1 | class-fresh | 512.0 | 42.54 | 28.57 | 145.9 | 50.55 | 27.08 | 53.68 | 350.0 | 423.7 | 2733 | 269.9 | 1.05 | 0 | 0 | 0.000 | 5.62 |
| 256K | 8 | today | 512.0 | 14539 | 0.040 | 530.5 | 34.29 | 30.94 | 1419 | 16524 | 22174 | 521.8 | 4378 | 17.10 | 0 | 426.0 | 13.27 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 1369 | 0.030 | 320.6 | 25.96 | 30.08 | 548.2 | 2344 | 3216 | 3625 | 530.5 | 2.07 | 0 | 407.0 | 3.06 | 6.95 |
| 256K | 8 | class-fresh | 512.0 | 125.2 | 102.6 | 468.6 | 372.7 | 43.89 | 195.2 | 1355 | 1949 | 5681 | 265.2 | 1.04 | 0 | 0 | 0.000 | 10.89 |
| 1M | 1 | today | 128.0 | 40.24 | 0.050 | 822.9 | 19.95 | 30.15 | 475.0 | 1560 | 6128 | 393.0 | 5145 | 5.02 | 0 | 38.00 | 12.20 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 43.13 | 0.050 | 772.6 | 19.41 | 35.61 | 114.7 | 1023 | 2269 | 939.9 | 2067 | 2.02 | 0 | 7.00 | 4.10 | 2.39 |
| 1M | 1 | class-fresh | 128.0 | 42.49 | 24.92 | 440.2 | 45.44 | 27.47 | 53.88 | 635.4 | 703.2 | 1537 | 1037 | 1.01 | 0 | 0 | 0.000 | 3.91 |
| 1M | 8 | today | 128.0 | 13542 | 0.040 | 1532 | 21.78 | 29.63 | 2935 | 18257 | 23224 | 498.1 | 5146 | 5.03 | 0 | 102.0 | 13.31 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 4885 | 0.040 | 1201 | 19.17 | 27.63 | 1947 | 8453 | 11034 | 1060 | 2066 | 2.02 | 0 | 100.0 | 5.08 | 2.13 |
| 1M | 8 | class-fresh | 128.0 | 127.6 | 129.6 | 1781 | 1322 | 54.53 | 102.4 | 3686 | 4492 | 2114 | 1034 | 1.01 | 0 | 0 | 0.000 | 4.24 |
| 4M | 1 | today | 32.00 | 37.58 | 0.040 | 1965 | 19.56 | 31.62 | 436.0 | 2502 | 6307 | 322.8 | 8217 | 2.01 | 0 | 6.00 | 15.25 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 39.49 | 0.050 | 1936 | 19.92 | 31.86 | 442.1 | 2591 | 6414 | 319.8 | 8218 | 2.01 | 0 | 6.00 | 15.29 | 0.991 |
| 4M | 1 | class-fresh | 32.00 | 46.39 | 26.03 | 1951 | 60.62 | 29.94 | 67.64 | 2182 | 2562 | 455.3 | 4109 | 1.00 | 0 | 0 | 0.000 | 1.41 |
| 4M | 8 | today | 32.00 | 16279 | 0.030 | 5885 | 20.89 | 28.71 | 5754 | 28043 | 34238 | 316.6 | 8210 | 2.00 | 0 | 21.00 | 14.26 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 13653 | 0.030 | 6584 | 21.29 | 28.43 | 5005 | 25189 | 31105 | 335.0 | 8211 | 2.00 | 0 | 21.00 | 13.31 | 1.06 |
| 4M | 8 | class-fresh | 32.00 | 163.8 | 111.1 | 7777 | 6531 | 61.27 | 95.25 | 13475 | 14060 | 589.1 | 4106 | 1.00 | 0 | 0 | 0.000 | 1.86 |

