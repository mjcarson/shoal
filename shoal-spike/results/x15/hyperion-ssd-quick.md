### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 850.0 µs |
| fdatasync of a clean file | p50 112.8 µs, p99 128.1 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2085 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2110 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.34 µs, p99 33.53 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 1 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs · **quick: not a measurement**

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 12.00 | 164.7 | 0.100 | 1304 | 2457 | 72.39 | 5992 | 9351 | 15450 | 106.6 | 4176 | 65.25 | 4.33 | 0 | 49.19 | 1.00 |
| 64K | 1 | class-pool | 12.00 | 85.22 | 0.080 | 194.5 | 2876 | 73.72 | 5970 | 9128 | 16298 | 116.6 | 143.8 | 2.25 | 4.33 | 0 | 2.09 | 1.09 |
| 64K | 1 | class-fresh | 12.00 | 105.6 | 67.70 | 164.6 | 2906 | 62.99 | 2019 | 5372 | 5860 | 184.7 | 73.00 | 1.14 | 2.00 | 0 | 0.000 | 1.73 |
| 64K | 8 | today | 12.00 | 170.1 | 0.060 | 674.7 | 1527 | 93.71 | 4748 | 7838 | 48835 | 217.5 | 4174 | 65.22 | 2.50 | 4.00 | 72.09 | 1.00 |
| 64K | 8 | class-pool | 12.00 | 179.9 | 0.050 | 561.6 | 1504 | 102.8 | 4240 | 8478 | 18121 | 489.0 | 142.0 | 2.22 | 2.50 | 4.00 | 35.50 | 2.25 |
| 64K | 8 | class-fresh | 12.00 | 634.8 | 370.7 | 564.2 | 5426 | 95.54 | 5972 | 11402 | 14161 | 558.6 | 70.83 | 1.11 | 1.00 | 0 | 0.000 | 2.57 |
| 1M | 1 | today | 12.00 | 90.15 | 0.090 | 1376 | 2341 | 71.33 | 5742 | 9533 | 15337 | 102.6 | 5137 | 5.02 | 4.33 | 0 | 49.97 | 1.00 |
| 1M | 1 | class-pool | 12.00 | 110.9 | 0.090 | 1369 | 2234 | 66.72 | 6652 | 10484 | 12865 | 98.18 | 2064 | 2.02 | 4.00 | 0 | 7.28 | 0.957 |
| 1M | 1 | class-fresh | 12.00 | 106.3 | 67.28 | 1351 | 2883 | 63.64 | 2088 | 6553 | 7127 | 151.5 | 1034 | 1.01 | 2.00 | 0 | 0.000 | 1.48 |
| 1M | 8 | today | 12.00 | 186.8 | 0.060 | 5270 | 6320 | 108.3 | 5873 | 17763 | 57729 | 162.2 | 5135 | 5.01 | 2.67 | 4.00 | 64.81 | 1.00 |
| 1M | 8 | class-pool | 12.00 | 184.4 | 0.060 | 5261 | 5002 | 111.7 | 5899 | 17781 | 28679 | 267.4 | 2062 | 2.01 | 2.42 | 4.00 | 39.66 | 1.65 |
| 1M | 8 | class-fresh | 12.00 | 718.2 | 383.0 | 4907 | 8730 | 101.9 | 5383 | 21887 | 23400 | 330.2 | 1032 | 1.01 | 1.00 | 0 | 0.000 | 2.04 |

