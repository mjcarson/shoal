### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.37 KiB a sync · sync p50 852.8 µs |
| fdatasync of a clean file | p50 112.7 µs, p99 126.3 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2083 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2095 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.10 µs, p99 33.35 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 4 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 600.0 | 137.3 | 0.090 | 352.7 | 2347 | 72.05 | 5923 | 9184 | 31480 | 81.74 | 4184 | 65.38 | 4.25 | 109.0 | 62.70 | 1.00 |
| 64K | 1 | class-pool | 600.0 | 84.68 | 0.090 | 160.1 | 2836 | 74.16 | 6936 | 8529 | 14855 | 117.5 | 150.2 | 2.35 | 4.51 | 0 | 4.16 | 1.44 |
| 64K | 1 | class-fresh | 600.0 | 107.7 | 77.95 | 191.7 | 2921 | 63.42 | 2039 | 5396 | 11010 | 179.7 | 76.87 | 1.20 | 2.00 | 0 | 0.000 | 2.20 |
| 64K | 8 | today | 600.0 | 41033 | 0.100 | 2002 | 7328 | 85.76 | 17958 | 70320 | 89255 | 126.0 | 4187 | 65.42 | 2.22 | 473.0 | 68.47 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 13014 | 0.100 | 381.8 | 3221 | 100.1 | 9131 | 26148 | 36330 | 324.2 | 149.0 | 2.33 | 1.97 | 457.0 | 31.11 | 2.57 |
| 64K | 8 | class-fresh | 600.0 | 233.4 | 189.8 | 441.7 | 5767 | 89.28 | 7777 | 15047 | 25121 | 522.9 | 75.03 | 1.17 | 1.06 | 0 | 0.000 | 4.15 |
| 256K | 1 | today | 512.0 | 143.8 | 0.090 | 1305 | 2860 | 72.96 | 6115 | 10785 | 33221 | 79.34 | 4376 | 17.10 | 4.40 | 72.00 | 85.54 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 80.69 | 0.090 | 394.1 | 3067 | 91.77 | 6673 | 10937 | 15704 | 104.3 | 536.6 | 2.10 | 4.49 | 0 | 3.13 | 1.31 |
| 256K | 1 | class-fresh | 512.0 | 107.9 | 77.00 | 423.5 | 3152 | 63.96 | 2063 | 5884 | 11385 | 164.9 | 270.2 | 1.06 | 2.00 | 0 | 0.000 | 2.08 |
| 256K | 8 | today | 512.0 | 40676 | 0.100 | 2381 | 8339 | 83.19 | 18235 | 71539 | 96432 | 122.1 | 4386 | 17.13 | 2.35 | 404.0 | 68.14 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 12769 | 0.100 | 835.3 | 3506 | 86.40 | 8893 | 26534 | 34363 | 321.9 | 531.3 | 2.08 | 2.00 | 391.0 | 38.64 | 2.64 |
| 256K | 8 | class-fresh | 512.0 | 242.3 | 201.3 | 852.9 | 5484 | 88.85 | 7387 | 14776 | 24559 | 520.4 | 269.6 | 1.05 | 1.09 | 0 | 0.000 | 4.26 |
| 1M | 1 | today | 128.0 | 117.9 | 0.090 | 2179 | 2431 | 73.05 | 5986 | 11482 | 29644 | 79.17 | 5141 | 5.02 | 4.34 | 16.00 | 82.47 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 60.42 | 0.090 | 1365 | 2828 | 75.12 | 6304 | 8949 | 14164 | 101.7 | 2074 | 2.02 | 4.47 | 0 | 13.47 | 1.28 |
| 1M | 1 | class-fresh | 128.0 | 108.5 | 67.01 | 1352 | 2854 | 64.43 | 2057 | 6513 | 9520 | 150.1 | 1034 | 1.01 | 2.00 | 0 | 0.000 | 1.90 |
| 1M | 8 | today | 128.0 | 46890 | 0.091 | 4008 | 11606 | 98.68 | 12781 | 79551 | 106810 | 114.3 | 5145 | 5.02 | 2.23 | 96.00 | 64.93 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 20647 | 0.090 | 3173 | 9190 | 94.27 | 10792 | 44031 | 59832 | 201.0 | 2064 | 2.02 | 1.97 | 94.00 | 45.95 | 1.76 |
| 1M | 8 | class-fresh | 128.0 | 352.8 | 262.9 | 4941 | 10741 | 98.53 | 5835 | 21562 | 29308 | 357.5 | 1035 | 1.01 | 0.891 | 0 | 0.000 | 3.13 |
| 4M | 1 | today | 32.00 | 85.54 | 0.090 | 5177 | 2310 | 73.11 | 5931 | 13740 | 32216 | 66.39 | 8214 | 2.01 | 4.31 | 2.00 | 72.07 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 122.0 | 0.100 | 5210 | 2939 | 72.92 | 5874 | 13758 | 30461 | 66.92 | 8222 | 2.01 | 4.38 | 1.00 | 88.47 | 1.01 |
| 4M | 1 | class-fresh | 32.00 | 108.2 | 66.55 | 5263 | 2907 | 64.75 | 2053 | 10468 | 13774 | 93.21 | 4108 | 1.00 | 2.00 | 0 | 0.000 | 1.40 |
| 4M | 8 | today | 32.00 | 45519 | 0.081 | 16286 | 19552 | 117.8 | 7809 | 88237 | 134636 | 88.12 | 8214 | 2.01 | 1.84 | 21.00 | 63.18 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 57063 | 0.060 | 14823 | 19593 | 116.1 | 10179 | 98609 | 130203 | 85.45 | 8211 | 2.00 | 2.06 | 21.00 | 63.10 | 0.970 |
| 4M | 8 | class-fresh | 32.00 | 900.7 | 316.6 | 24600 | 23288 | 107.7 | 6232 | 53401 | 54782 | 151.6 | 4107 | 1.00 | 0.781 | 0 | 0.000 | 1.72 |

