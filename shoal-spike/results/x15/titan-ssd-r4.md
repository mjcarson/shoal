### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.37 KiB a sync · sync p50 855.2 µs |
| fdatasync of a clean file | p50 112.7 µs, p99 141.9 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 2 KiB and 1.00 flushes a rename | p50 2104 µs |
| rename, then the directory's Fsync | 3 KiB and 1.00 flushes a rename | p50 2122 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.24 µs, p99 33.57 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 4 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan ssd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 600.0 | 126.9 | 0.100 | 309.7 | 2281 | 73.02 | 5865 | 8857 | 31844 | 82.23 | 4188 | 65.43 | 4.20 | 120.0 | 74.39 | 1.00 |
| 64K | 1 | class-pool | 600.0 | 86.30 | 0.090 | 160.1 | 2759 | 73.83 | 6958 | 8380 | 13221 | 118.1 | 148.0 | 2.31 | 4.56 | 0 | 18.71 | 1.44 |
| 64K | 1 | class-fresh | 600.0 | 109.9 | 79.68 | 200.0 | 2881 | 63.54 | 2031 | 5375 | 11047 | 180.4 | 77.35 | 1.21 | 2.00 | 0 | 0.000 | 2.19 |
| 64K | 8 | today | 600.0 | 40569 | 0.100 | 1949 | 6503 | 87.26 | 17956 | 69309 | 90501 | 128.1 | 4186 | 65.40 | 2.25 | 473.0 | 72.61 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 12605 | 0.110 | 404.1 | 3242 | 98.67 | 9065 | 25911 | 36583 | 329.5 | 146.7 | 2.29 | 1.94 | 457.0 | 30.15 | 2.57 |
| 64K | 8 | class-fresh | 600.0 | 246.6 | 173.3 | 422.1 | 5543 | 96.13 | 7558 | 14555 | 23028 | 547.7 | 73.53 | 1.15 | 1.04 | 0 | 0.000 | 4.28 |
| 256K | 1 | today | 512.0 | 150.7 | 0.100 | 774.1 | 2949 | 73.29 | 6087 | 11411 | 30393 | 79.12 | 4384 | 17.12 | 4.38 | 74.00 | 64.88 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 82.10 | 0.090 | 394.0 | 3054 | 86.63 | 6441 | 10580 | 16382 | 105.4 | 535.1 | 2.09 | 4.49 | 0 | 3.12 | 1.33 |
| 256K | 1 | class-fresh | 512.0 | 110.6 | 83.29 | 432.3 | 3143 | 64.17 | 2057 | 5890 | 9016 | 165.4 | 271.0 | 1.06 | 2.00 | 0 | 0.000 | 2.09 |
| 256K | 8 | today | 512.0 | 40505 | 0.101 | 2285 | 7508 | 86.49 | 18079 | 71591 | 92832 | 124.2 | 4377 | 17.10 | 2.24 | 404.0 | 58.82 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 12690 | 0.110 | 835.0 | 3430 | 84.27 | 9196 | 26178 | 36718 | 319.3 | 531.4 | 2.08 | 2.02 | 390.0 | 35.47 | 2.57 |
| 256K | 8 | class-fresh | 512.0 | 251.1 | 177.4 | 897.2 | 5624 | 86.93 | 6980 | 14547 | 22654 | 536.6 | 267.3 | 1.04 | 1.03 | 0 | 0.000 | 4.32 |
| 1M | 1 | today | 128.0 | 120.6 | 0.101 | 1662 | 2478 | 73.31 | 5987 | 11911 | 30621 | 77.09 | 5144 | 5.02 | 4.38 | 16.00 | 76.05 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 62.27 | 0.090 | 1356 | 2823 | 80.21 | 6158 | 10560 | 13525 | 100.0 | 2072 | 2.02 | 4.20 | 0 | 12.46 | 1.30 |
| 1M | 1 | class-fresh | 128.0 | 111.4 | 80.72 | 1377 | 2904 | 64.91 | 2064 | 6606 | 9710 | 147.0 | 1037 | 1.01 | 2.00 | 0 | 0.000 | 1.91 |
| 1M | 8 | today | 128.0 | 42966 | 0.100 | 4164 | 10772 | 96.07 | 17224 | 77145 | 110060 | 115.9 | 5144 | 5.02 | 2.20 | 96.00 | 71.68 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 21121 | 0.090 | 3387 | 8363 | 106.9 | 9377 | 42797 | 56722 | 203.2 | 2066 | 2.02 | 1.95 | 95.00 | 43.76 | 1.75 |
| 1M | 8 | class-fresh | 128.0 | 325.9 | 204.9 | 5337 | 9468 | 107.7 | 6009 | 21011 | 27130 | 365.4 | 1035 | 1.01 | 0.852 | 0 | 0.000 | 3.15 |
| 4M | 1 | today | 32.00 | 81.05 | 0.090 | 5422 | 2886 | 73.52 | 6850 | 15504 | 28643 | 64.52 | 8215 | 2.01 | 4.44 | 1.00 | 74.20 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 151.6 | 0.110 | 5391 | 2380 | 73.61 | 6122 | 14770 | 30434 | 64.83 | 8216 | 2.01 | 4.41 | 2.00 | 71.40 | 1.00 |
| 4M | 1 | class-fresh | 32.00 | 111.7 | 76.97 | 5298 | 2902 | 65.48 | 2070 | 10532 | 19309 | 91.53 | 4109 | 1.00 | 2.00 | 0 | 0.000 | 1.42 |
| 4M | 8 | today | 32.00 | 53375 | 0.080 | 12116 | 15202 | 107.5 | 15577 | 81661 | 146044 | 87.84 | 8214 | 2.01 | 2.09 | 21.00 | 61.76 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 62310 | 0.080 | 15306 | 17300 | 109.5 | 10276 | 107716 | 131463 | 85.06 | 8212 | 2.00 | 2.00 | 21.00 | 57.86 | 0.968 |
| 4M | 8 | class-fresh | 32.00 | 285.3 | 204.8 | 23827 | 23454 | 88.54 | 5998 | 50666 | 53419 | 157.2 | 4107 | 1.00 | 1.00 | 0 | 0.000 | 1.79 |

