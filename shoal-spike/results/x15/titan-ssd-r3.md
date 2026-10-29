### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 857.3 µs |
| fdatasync of a clean file | p50 113.8 µs, p99 159.3 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2124 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2110 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.14 µs, p99 33.51 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 3 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 600.0 | 173.8 | 0.080 | 543.5 | 2349 | 73.56 | 6016 | 9046 | 33057 | 80.80 | 4185 | 65.40 | 4.27 | 106.0 | 83.56 | 1.00 |
| 64K | 1 | class-pool | 600.0 | 85.71 | 0.080 | 156.8 | 2865 | 73.27 | 7133 | 8586 | 14936 | 115.0 | 149.2 | 2.33 | 4.58 | 0 | 3.12 | 1.42 |
| 64K | 1 | class-fresh | 600.0 | 108.2 | 78.19 | 191.3 | 3000 | 64.26 | 2068 | 5509 | 11077 | 175.9 | 78.08 | 1.22 | 2.00 | 0 | 0.000 | 2.18 |
| 64K | 8 | today | 600.0 | 40595 | 0.080 | 2061 | 7099 | 88.09 | 18379 | 68831 | 87163 | 126.9 | 4186 | 65.40 | 2.23 | 475.0 | 62.91 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 12898 | 0.090 | 388.1 | 3295 | 101.2 | 9059 | 26269 | 39148 | 322.7 | 145.6 | 2.27 | 1.95 | 458.0 | 31.23 | 2.54 |
| 64K | 8 | class-fresh | 600.0 | 234.8 | 181.7 | 435.5 | 6017 | 92.53 | 8054 | 15137 | 43280 | 498.3 | 75.06 | 1.17 | 1.04 | 0 | 0.000 | 3.93 |
| 256K | 1 | today | 512.0 | 155.7 | 0.090 | 1417 | 2988 | 73.60 | 6084 | 11250 | 31055 | 79.27 | 4382 | 17.12 | 4.39 | 72.00 | 80.21 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 79.71 | 0.071 | 393.8 | 3072 | 91.19 | 6634 | 10928 | 14699 | 105.4 | 533.4 | 2.08 | 4.49 | 0 | 3.13 | 1.33 |
| 256K | 1 | class-fresh | 512.0 | 109.0 | 78.19 | 423.2 | 3145 | 64.00 | 2063 | 5882 | 9353 | 164.7 | 270.2 | 1.06 | 2.00 | 0 | 0.000 | 2.08 |
| 256K | 8 | today | 512.0 | 40351 | 0.081 | 2300 | 8760 | 87.44 | 18144 | 71405 | 93456 | 123.5 | 4378 | 17.10 | 2.30 | 402.0 | 69.29 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 12845 | 0.090 | 760.9 | 3319 | 87.98 | 9259 | 26150 | 37727 | 319.2 | 531.3 | 2.08 | 2.01 | 391.0 | 32.45 | 2.58 |
| 256K | 8 | class-fresh | 512.0 | 261.3 | 208.7 | 1017 | 5450 | 89.17 | 6865 | 14327 | 21714 | 544.9 | 268.6 | 1.05 | 1.03 | 0 | 0.000 | 4.41 |
| 1M | 1 | today | 128.0 | 120.2 | 0.090 | 1743 | 2518 | 73.77 | 6068 | 11791 | 34378 | 77.24 | 5142 | 5.02 | 4.35 | 16.00 | 62.76 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 61.50 | 0.070 | 1370 | 2840 | 79.22 | 6276 | 9853 | 14966 | 99.24 | 2074 | 2.03 | 4.35 | 0 | 11.51 | 1.28 |
| 1M | 1 | class-fresh | 128.0 | 108.5 | 67.01 | 1353 | 2918 | 64.57 | 2065 | 6582 | 9559 | 148.1 | 1037 | 1.01 | 2.00 | 0 | 0.000 | 1.92 |
| 1M | 8 | today | 128.0 | 48986 | 0.080 | 3972 | 10059 | 96.49 | 13659 | 79143 | 94112 | 115.9 | 5145 | 5.02 | 2.20 | 97.00 | 62.94 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 20722 | 0.071 | 3210 | 8979 | 105.2 | 8600 | 43256 | 74158 | 192.4 | 2065 | 2.02 | 1.88 | 94.00 | 38.79 | 1.66 |
| 1M | 8 | class-fresh | 128.0 | 287.6 | 308.0 | 4632 | 8479 | 96.56 | 5989 | 20728 | 34601 | 369.3 | 1036 | 1.01 | 0.945 | 0 | 0.000 | 3.19 |
| 4M | 1 | today | 32.00 | 88.27 | 0.080 | 5520 | 2373 | 73.61 | 6774 | 15228 | 30206 | 63.36 | 8214 | 2.01 | 4.47 | 2.00 | 60.33 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 85.63 | 0.091 | 5506 | 3014 | 73.69 | 7060 | 15642 | 30180 | 63.34 | 8217 | 2.01 | 4.50 | 1.00 | 62.49 | 1.000 |
| 4M | 1 | class-fresh | 32.00 | 116.9 | 72.87 | 5281 | 2933 | 71.13 | 2112 | 10634 | 16071 | 90.86 | 4182 | 1.02 | 2.03 | 0 | 0.000 | 1.43 |
| 4M | 8 | today | 32.00 | 54664 | 0.070 | 15299 | 17187 | 131.6 | 8372 | 100937 | 149949 | 88.51 | 8210 | 2.00 | 1.94 | 21.00 | 66.16 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 55618 | 0.070 | 15347 | 19694 | 140.8 | 6511 | 94801 | 144546 | 86.70 | 8212 | 2.00 | 2.06 | 21.00 | 57.57 | 0.980 |
| 4M | 8 | class-fresh | 32.00 | 404.1 | 258.6 | 23602 | 23536 | 102.6 | 5721 | 50602 | 53174 | 157.4 | 4107 | 1.00 | 0.844 | 0 | 0.000 | 1.78 |

