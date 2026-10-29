### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.53 KiB a sync · sync p50 854.9 µs |
| fdatasync of a clean file | p50 112.7 µs, p99 126.1 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2072 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2104 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.32 µs, p99 34.48 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 2 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 600.0 | 125.0 | 0.081 | 351.4 | 2370 | 72.17 | 5867 | 9250 | 30303 | 82.41 | 4184 | 65.37 | 4.23 | 111.0 | 64.77 | 1.00 |
| 64K | 1 | class-pool | 600.0 | 85.98 | 0.080 | 155.2 | 2767 | 73.74 | 6961 | 8338 | 13888 | 118.5 | 149.4 | 2.33 | 4.61 | 0 | 3.12 | 1.44 |
| 64K | 1 | class-fresh | 600.0 | 107.8 | 77.82 | 190.5 | 2884 | 64.57 | 2034 | 5369 | 8659 | 181.4 | 76.75 | 1.20 | 2.00 | 0 | 0.000 | 2.20 |
| 64K | 8 | today | 600.0 | 40460 | 0.090 | 1983 | 6571 | 88.18 | 18703 | 69596 | 95995 | 125.7 | 4187 | 65.43 | 2.23 | 475.0 | 61.98 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 12647 | 0.100 | 386.8 | 3120 | 97.45 | 9018 | 25454 | 36763 | 333.9 | 146.1 | 2.28 | 1.93 | 459.0 | 28.08 | 2.66 |
| 64K | 8 | class-fresh | 600.0 | 235.0 | 191.2 | 461.0 | 5791 | 93.56 | 7428 | 14727 | 24326 | 533.2 | 74.39 | 1.16 | 1.04 | 0 | 0.000 | 4.24 |
| 256K | 1 | today | 512.0 | 138.5 | 0.090 | 764.8 | 2569 | 73.94 | 6048 | 10397 | 34116 | 79.50 | 4375 | 17.09 | 4.34 | 78.00 | 75.01 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 82.24 | 0.080 | 394.1 | 3064 | 90.12 | 6307 | 10509 | 14719 | 105.3 | 542.4 | 2.12 | 4.48 | 0 | 4.17 | 1.32 |
| 256K | 1 | class-fresh | 512.0 | 108.3 | 77.89 | 421.7 | 3127 | 64.27 | 2066 | 5870 | 9019 | 166.7 | 270.1 | 1.05 | 2.00 | 0 | 0.000 | 2.10 |
| 256K | 8 | today | 512.0 | 40928 | 0.090 | 2281 | 7284 | 85.52 | 18773 | 72103 | 93852 | 122.7 | 4380 | 17.11 | 2.33 | 404.0 | 74.49 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 12586 | 0.091 | 763.6 | 3296 | 86.58 | 9063 | 25576 | 37710 | 323.6 | 531.1 | 2.07 | 2.02 | 391.0 | 36.47 | 2.64 |
| 256K | 8 | class-fresh | 512.0 | 257.8 | 200.8 | 987.2 | 5429 | 91.74 | 7243 | 14624 | 22888 | 524.4 | 268.5 | 1.05 | 1.04 | 0 | 0.000 | 4.27 |
| 1M | 1 | today | 128.0 | 117.8 | 0.090 | 2403 | 2406 | 73.15 | 6058 | 11789 | 34899 | 77.89 | 5142 | 5.02 | 4.40 | 15.00 | 84.69 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 60.14 | 0.080 | 1364 | 2875 | 130.9 | 4381 | 8860 | 13600 | 103.9 | 2074 | 2.03 | 4.44 | 0 | 12.45 | 1.33 |
| 1M | 1 | class-fresh | 128.0 | 108.0 | 66.75 | 1352 | 2895 | 64.75 | 2061 | 6543 | 9503 | 149.5 | 1037 | 1.01 | 2.00 | 0 | 0.000 | 1.92 |
| 1M | 8 | today | 128.0 | 45991 | 0.090 | 4021 | 10120 | 95.93 | 14671 | 77927 | 111924 | 112.8 | 5146 | 5.03 | 2.27 | 98.00 | 58.86 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 21575 | 0.080 | 3311 | 8323 | 100.8 | 9546 | 43691 | 71324 | 196.5 | 2065 | 2.02 | 1.93 | 95.00 | 49.11 | 1.74 |
| 1M | 8 | class-fresh | 128.0 | 315.2 | 227.0 | 4088 | 9871 | 97.86 | 6128 | 22301 | 32438 | 350.0 | 1036 | 1.01 | 1.02 | 0 | 0.000 | 3.10 |
| 4M | 1 | today | 32.00 | 117.1 | 0.100 | 5206 | 2275 | 75.07 | 5949 | 13673 | 37240 | 64.43 | 8215 | 2.01 | 4.38 | 2.00 | 65.72 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 156.1 | 0.100 | 5944 | 3124 | 120.4 | 4152 | 13748 | 30649 | 65.62 | 8219 | 2.01 | 3.66 | 4.00 | 78.79 | 1.02 |
| 4M | 1 | class-fresh | 32.00 | 110.1 | 68.89 | 5279 | 2904 | 65.44 | 2065 | 10529 | 12930 | 94.14 | 4109 | 1.00 | 2.00 | 0 | 0.000 | 1.46 |
| 4M | 8 | today | 32.00 | 53470 | 0.070 | 14502 | 16737 | 131.2 | 9450 | 94773 | 138881 | 87.83 | 8213 | 2.01 | 2.12 | 21.00 | 63.01 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 53648 | 0.060 | 15344 | 19774 | 111.1 | 7832 | 90738 | 122074 | 87.86 | 8211 | 2.00 | 2.03 | 21.00 | 59.66 | 1.00 |
| 4M | 8 | class-fresh | 32.00 | 322.6 | 301.1 | 25571 | 23425 | 111.6 | 6027 | 50907 | 53214 | 156.9 | 4107 | 1.00 | 0.812 | 0 | 0.000 | 1.79 |

