### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 910.2 µs |
| fdatasync of a clean file | p50 112.3 µs, p99 133.2 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2078 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2099 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.04 µs, p99 32.95 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 1 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion ssd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 600.0 | 117.2 | 0.090 | 340.4 | 2356 | 72.10 | 5889 | 9131 | 31472 | 81.91 | 4183 | 65.36 | 4.24 | 110.0 | 81.16 | 1.00 |
| 64K | 1 | class-pool | 600.0 | 79.63 | 0.081 | 159.4 | 2850 | 76.06 | 6880 | 8985 | 15133 | 117.0 | 149.7 | 2.34 | 4.48 | 0 | 3.12 | 1.43 |
| 64K | 1 | class-fresh | 600.0 | 107.4 | 76.72 | 192.0 | 2926 | 63.39 | 2034 | 5389 | 8543 | 180.5 | 76.96 | 1.20 | 2.00 | 0 | 0.000 | 2.20 |
| 64K | 8 | today | 600.0 | 40456 | 0.100 | 1937 | 7257 | 85.81 | 17237 | 69713 | 90498 | 126.4 | 4186 | 65.40 | 2.23 | 473.0 | 73.27 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 12974 | 0.100 | 390.1 | 3313 | 102.0 | 9130 | 26255 | 36651 | 323.0 | 146.1 | 2.28 | 1.97 | 458.0 | 28.18 | 2.56 |
| 64K | 8 | class-fresh | 600.0 | 230.0 | 179.2 | 427.9 | 5901 | 91.45 | 7778 | 15069 | 25084 | 517.6 | 74.56 | 1.16 | 1.03 | 0 | 0.000 | 4.09 |
| 256K | 1 | today | 512.0 | 139.2 | 0.100 | 1436 | 3003 | 73.52 | 6156 | 11208 | 33049 | 78.54 | 4381 | 17.11 | 4.40 | 71.00 | 65.95 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 84.50 | 0.090 | 394.1 | 3087 | 89.13 | 6349 | 10465 | 15424 | 104.9 | 534.1 | 2.09 | 4.49 | 0 | 4.20 | 1.34 |
| 256K | 1 | class-fresh | 512.0 | 107.2 | 77.68 | 422.3 | 3180 | 63.56 | 2066 | 5913 | 9240 | 165.0 | 270.3 | 1.06 | 2.00 | 0 | 0.000 | 2.10 |
| 256K | 8 | today | 512.0 | 39678 | 0.100 | 2297 | 8224 | 83.27 | 18698 | 71702 | 93975 | 123.0 | 4378 | 17.10 | 2.25 | 402.0 | 70.25 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 12908 | 0.110 | 761.1 | 3356 | 84.26 | 9586 | 26452 | 43879 | 312.0 | 531.5 | 2.08 | 2.05 | 389.0 | 36.39 | 2.54 |
| 256K | 8 | class-fresh | 512.0 | 286.3 | 265.8 | 1178 | 5693 | 88.41 | 6032 | 13829 | 20697 | 557.9 | 267.9 | 1.05 | 0.957 | 0 | 0.000 | 4.54 |
| 1M | 1 | today | 128.0 | 114.2 | 0.091 | 2388 | 2491 | 72.73 | 6299 | 12023 | 32338 | 76.38 | 5143 | 5.02 | 4.45 | 15.00 | 65.54 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 67.98 | 0.090 | 1360 | 2770 | 73.79 | 6382 | 9013 | 14298 | 102.5 | 2074 | 2.03 | 4.48 | 0 | 11.46 | 1.34 |
| 1M | 1 | class-fresh | 128.0 | 107.4 | 65.64 | 1352 | 2911 | 63.75 | 2065 | 6571 | 9861 | 148.0 | 1037 | 1.01 | 2.00 | 0 | 0.000 | 1.94 |
| 1M | 8 | today | 128.0 | 45556 | 0.100 | 4286 | 10767 | 87.89 | 19321 | 80723 | 101258 | 113.3 | 5146 | 5.02 | 2.26 | 97.00 | 69.20 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 20945 | 0.100 | 3680 | 9958 | 101.7 | 9164 | 43067 | 58630 | 197.3 | 2063 | 2.01 | 1.95 | 94.00 | 41.68 | 1.74 |
| 1M | 8 | class-fresh | 128.0 | 327.6 | 239.1 | 5474 | 9924 | 106.0 | 5861 | 21083 | 28508 | 363.3 | 1034 | 1.01 | 0.844 | 0 | 0.000 | 3.21 |
| 4M | 1 | today | 32.00 | 80.03 | 0.100 | 6178 | 3049 | 73.12 | 7008 | 16292 | 30449 | 62.49 | 8215 | 2.01 | 4.53 | 1.00 | 63.55 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 113.9 | 0.100 | 5180 | 2943 | 73.46 | 6915 | 15375 | 28551 | 65.97 | 8213 | 2.01 | 4.47 | 1.00 | 80.27 | 1.06 |
| 4M | 1 | class-fresh | 32.00 | 107.9 | 67.25 | 5260 | 2917 | 65.09 | 2054 | 10479 | 11449 | 94.36 | 4108 | 1.00 | 2.00 | 0 | 0.000 | 1.51 |
| 4M | 8 | today | 32.00 | 45836 | 0.051 | 20371 | 19678 | 141.4 | 7683 | 84407 | 119989 | 89.71 | 8212 | 2.00 | 1.78 | 21.00 | 64.99 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 54840 | 0.080 | 14956 | 19436 | 106.3 | 10766 | 93858 | 137023 | 86.55 | 8212 | 2.00 | 2.03 | 21.00 | 67.78 | 0.965 |
| 4M | 8 | class-fresh | 32.00 | 505.1 | 237.1 | 20629 | 25559 | 100.5 | 5734 | 52659 | 90921 | 151.8 | 4106 | 1.00 | 0.875 | 0 | 0.000 | 1.69 |

