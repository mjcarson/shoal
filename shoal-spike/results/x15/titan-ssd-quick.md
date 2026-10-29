### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 843.3 µs |
| fdatasync of a clean file | p50 113.3 µs, p99 122.6 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2104 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2092 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.40 µs, p99 35.78 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 1 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs · **quick: not a measurement**

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 12.00 | 81.61 | 0.080 | 293.0 | 2217 | 74.61 | 5664 | 8305 | 11774 | 132.9 | 4176 | 65.24 | 4.17 | 0 | 61.50 | 1.00 |
| 64K | 1 | class-pool | 12.00 | 81.65 | 0.080 | 193.8 | 2833 | 73.11 | 6935 | 8152 | 16878 | 113.6 | 144.0 | 2.25 | 4.25 | 0 | 4.16 | 0.855 |
| 64K | 1 | class-fresh | 12.00 | 105.3 | 74.73 | 188.6 | 2864 | 65.06 | 2033 | 5324 | 5829 | 184.5 | 73.00 | 1.14 | 2.00 | 0 | 0.000 | 1.39 |
| 64K | 8 | today | 12.00 | 194.6 | 0.050 | 674.9 | 1568 | 107.7 | 5747 | 8111 | 48912 | 216.4 | 4175 | 65.23 | 2.50 | 4.00 | 68.02 | 1.00 |
| 64K | 8 | class-pool | 12.00 | 175.8 | 0.051 | 674.8 | 1544 | 104.5 | 6358 | 8680 | 23093 | 405.0 | 141.8 | 2.22 | 2.42 | 4.00 | 30.23 | 1.87 |
| 64K | 8 | class-fresh | 12.00 | 985.4 | 564.0 | 562.1 | 5241 | 95.85 | 4114 | 11556 | 14452 | 556.7 | 70.71 | 1.10 | 1.08 | 0 | 0.000 | 2.57 |
| 1M | 1 | today | 12.00 | 105.7 | 0.081 | 1453 | 2756 | 73.96 | 5927 | 10685 | 18356 | 97.05 | 5137 | 5.02 | 4.17 | 0 | 54.54 | 1.00 |
| 1M | 1 | class-pool | 12.00 | 115.7 | 0.090 | 1355 | 2238 | 68.80 | 6728 | 10490 | 13933 | 97.62 | 2064 | 2.02 | 4.00 | 0 | 7.28 | 1.01 |
| 1M | 1 | class-fresh | 12.00 | 108.3 | 75.90 | 1372 | 2793 | 65.13 | 2000 | 6404 | 7124 | 153.4 | 1034 | 1.01 | 2.00 | 0 | 0.000 | 1.58 |
| 1M | 8 | today | 12.00 | 187.1 | 0.070 | 4838 | 6214 | 103.2 | 5618 | 17454 | 52004 | 176.4 | 5135 | 5.01 | 2.50 | 4.00 | 67.01 | 1.00 |
| 1M | 8 | class-pool | 12.00 | 167.9 | 0.051 | 5189 | 5740 | 110.0 | 4125 | 15949 | 29133 | 265.6 | 2062 | 2.01 | 2.17 | 4.00 | 40.81 | 1.51 |
| 1M | 8 | class-fresh | 12.00 | 988.1 | 375.7 | 4884 | 8923 | 96.03 | 4036 | 20467 | 21988 | 328.5 | 1032 | 1.01 | 1.00 | 0 | 0.000 | 1.86 |

