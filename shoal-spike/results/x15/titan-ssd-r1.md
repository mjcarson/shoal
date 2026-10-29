### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 858.8 µs |
| fdatasync of a clean file | p50 113.0 µs, p99 142.2 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2066 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2096 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.23 µs, p99 33.69 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 1 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 600.0 | 119.9 | 0.090 | 331.1 | 2341 | 71.72 | 5887 | 9160 | 31607 | 82.30 | 4184 | 65.37 | 4.23 | 114.0 | 77.19 | 1.00 |
| 64K | 1 | class-pool | 600.0 | 85.44 | 0.090 | 156.5 | 2778 | 72.77 | 6921 | 8362 | 14017 | 118.3 | 148.8 | 2.33 | 4.56 | 0 | 2.15 | 1.44 |
| 64K | 1 | class-fresh | 600.0 | 108.1 | 79.09 | 193.5 | 2904 | 63.80 | 2030 | 5385 | 11047 | 181.2 | 78.29 | 1.22 | 2.00 | 0 | 0.000 | 2.20 |
| 64K | 8 | today | 600.0 | 40017 | 0.090 | 1947 | 6602 | 86.96 | 18414 | 68866 | 99298 | 127.2 | 4186 | 65.41 | 2.22 | 477.0 | 63.17 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 12755 | 0.090 | 386.3 | 3286 | 101.4 | 9151 | 25926 | 38789 | 329.2 | 145.9 | 2.28 | 1.93 | 458.0 | 34.40 | 2.59 |
| 64K | 8 | class-fresh | 600.0 | 220.1 | 158.3 | 424.4 | 5777 | 92.81 | 8100 | 15228 | 21509 | 523.4 | 73.70 | 1.15 | 1.06 | 0 | 0.000 | 4.11 |
| 256K | 1 | today | 512.0 | 175.5 | 0.090 | 1225 | 2979 | 72.83 | 6109 | 11153 | 31527 | 79.24 | 4375 | 17.09 | 4.41 | 72.00 | 81.58 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 83.08 | 0.090 | 394.3 | 3079 | 99.64 | 6381 | 10318 | 15283 | 105.2 | 534.2 | 2.09 | 4.49 | 0 | 2.14 | 1.33 |
| 256K | 1 | class-fresh | 512.0 | 108.5 | 78.93 | 424.2 | 3161 | 63.94 | 2071 | 5915 | 9066 | 164.9 | 270.2 | 1.06 | 2.00 | 0 | 0.000 | 2.08 |
| 256K | 8 | today | 512.0 | 40580 | 0.090 | 2345 | 7704 | 82.19 | 19451 | 71243 | 93177 | 123.3 | 4378 | 17.10 | 2.28 | 402.0 | 69.40 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 12770 | 0.091 | 803.1 | 3489 | 85.96 | 9503 | 26503 | 39930 | 310.8 | 531.2 | 2.07 | 2.04 | 389.0 | 32.42 | 2.52 |
| 256K | 8 | class-fresh | 512.0 | 277.7 | 210.2 | 1145 | 5477 | 87.33 | 6090 | 13376 | 21551 | 561.4 | 267.8 | 1.05 | 0.961 | 0 | 0.000 | 4.55 |
| 1M | 1 | today | 128.0 | 189.0 | 0.110 | 2301 | 2508 | 73.46 | 5935 | 10667 | 33528 | 75.74 | 5143 | 5.02 | 4.34 | 17.00 | 84.15 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 64.78 | 0.090 | 1361 | 2849 | 80.07 | 6327 | 9029 | 14701 | 101.9 | 2073 | 2.02 | 4.40 | 0 | 11.43 | 1.35 |
| 1M | 1 | class-fresh | 128.0 | 108.1 | 78.98 | 1376 | 2916 | 64.12 | 2067 | 6619 | 9695 | 146.7 | 1038 | 1.01 | 2.00 | 0 | 0.000 | 1.94 |
| 1M | 8 | today | 128.0 | 45958 | 0.090 | 4168 | 8462 | 95.07 | 16932 | 79755 | 99373 | 114.9 | 5144 | 5.02 | 2.20 | 97.00 | 60.06 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 21126 | 0.080 | 3422 | 8725 | 106.9 | 7954 | 43095 | 56490 | 208.3 | 2065 | 2.02 | 1.91 | 94.00 | 49.02 | 1.81 |
| 1M | 8 | class-fresh | 128.0 | 299.1 | 204.6 | 5343 | 9941 | 101.5 | 6021 | 21284 | 29221 | 356.2 | 1035 | 1.01 | 0.883 | 0 | 0.000 | 3.10 |
| 4M | 1 | today | 32.00 | 133.2 | 0.100 | 5423 | 2798 | 73.19 | 6932 | 15313 | 30024 | 63.54 | 8215 | 2.01 | 4.47 | 1.00 | 72.91 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 122.9 | 0.091 | 5276 | 2991 | 73.97 | 5941 | 14782 | 37349 | 65.68 | 8216 | 2.01 | 4.28 | 2.00 | 69.93 | 1.03 |
| 4M | 1 | class-fresh | 32.00 | 107.6 | 76.11 | 5281 | 2925 | 66.47 | 2063 | 10518 | 13565 | 91.94 | 4109 | 1.00 | 2.00 | 0 | 0.000 | 1.45 |
| 4M | 8 | today | 32.00 | 52958 | 0.070 | 14515 | 16065 | 108.8 | 19741 | 92433 | 156067 | 88.17 | 8213 | 2.01 | 2.00 | 21.00 | 71.24 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 55641 | 0.080 | 12921 | 16959 | 105.7 | 11745 | 105869 | 122869 | 85.72 | 8213 | 2.01 | 2.22 | 21.00 | 55.56 | 0.972 |
| 4M | 8 | class-fresh | 32.00 | 629.3 | 254.5 | 20559 | 23520 | 105.3 | 6132 | 51630 | 87044 | 152.4 | 4107 | 1.00 | 0.938 | 0 | 0.000 | 1.73 |

