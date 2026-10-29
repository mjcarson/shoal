### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.88 µs |
| fdatasync of a clean file | p50 24.05 µs, p99 45.35 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8244 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8261 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.28 µs, p99 33.84 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 2 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 220.0 | 64.36 | 0.090 | 31296 | 46.63 | 71.97 | 40714 | 83359 | 216860 | 10.99 | 4195 | 65.55 | 0 | 38.00 | 534.4 | 1.00 |
| 64K | 1 | class-pool | 354.0 | 70.06 | 0.090 | 23317 | 45.23 | 71.22 | 26691 | 50065 | 141819 | 17.67 | 158.2 | 2.47 | 0 | 31.00 | 226.1 | 1.61 |
| 64K | 1 | class-fresh | 600.0 | 109.1 | 78.55 | 12222 | 11949 | 66.08 | 8319 | 33366 | 50028 | 30.26 | 81.04 | 1.27 | 0 | 0 | 0.000 | 2.75 |
| 64K | 8 | today | 458.0 | 233671 | 0.090 | 31919 | 51.62 | 73.47 | 84650 | 375241 | 741903 | 22.61 | 4188 | 65.44 | 0 | 361.0 | 541.9 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 67025 | 0.090 | 23668 | 33.66 | 65.19 | 35704 | 125439 | 192127 | 66.76 | 147.6 | 2.31 | 0 | 469.0 | 258.6 | 2.95 |
| 64K | 8 | class-fresh | 600.0 | 283.9 | 191.0 | 21986 | 23698 | 122.5 | 24883 | 66958 | 91806 | 114.0 | 75.49 | 1.18 | 0 | 0 | 0.000 | 5.04 |
| 256K | 1 | today | 202.0 | 68.01 | 0.090 | 25228 | 46.73 | 71.67 | 47016 | 83400 | 266708 | 10.06 | 4388 | 17.14 | 0 | 40.00 | 501.1 | 1.00 |
| 256K | 1 | class-pool | 327.0 | 77.95 | 0.090 | 22168 | 44.42 | 68.62 | 31620 | 58350 | 150167 | 16.35 | 539.3 | 2.11 | 0 | 43.00 | 334.4 | 1.62 |
| 256K | 1 | class-fresh | 512.0 | 109.2 | 79.46 | 13042 | 12041 | 66.04 | 8320 | 33367 | 50088 | 28.71 | 275.5 | 1.08 | 0 | 0 | 0.000 | 2.85 |
| 256K | 8 | today | 397.0 | 250303 | 0.090 | 35881 | 52.54 | 69.78 | 118240 | 433876 | 659130 | 19.67 | 4375 | 17.09 | 0 | 312.0 | 592.4 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 67241 | 0.080 | 25277 | 33.92 | 62.76 | 39855 | 133645 | 333981 | 61.33 | 534.0 | 2.09 | 0 | 401.0 | 251.2 | 3.12 |
| 256K | 8 | class-fresh | 512.0 | 295.3 | 180.6 | 20089 | 19150 | 121.5 | 24917 | 66641 | 75604 | 125.1 | 267.6 | 1.05 | 0 | 0 | 0.000 | 6.36 |
| 1M | 1 | today | 128.0 | 64.44 | 0.090 | 37455 | 47.14 | 70.74 | 46370 | 91698 | 233423 | 10.66 | 5146 | 5.03 | 0 | 17.00 | 509.5 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 61.49 | 0.090 | 36658 | 45.73 | 68.05 | 31472 | 66727 | 125466 | 14.20 | 2073 | 2.02 | 0 | 6.00 | 267.2 | 1.33 |
| 1M | 1 | class-fresh | 128.0 | 109.4 | 69.11 | 27095 | 26455 | 66.91 | 8318 | 58366 | 191754 | 16.23 | 1050 | 1.03 | 0 | 0 | 0.000 | 1.52 |
| 1M | 8 | today | 128.0 | 234893 | 0.100 | 113758 | 52.86 | 75.65 | 92100 | 458729 | 667280 | 19.50 | 5141 | 5.02 | 0 | 96.00 | 533.8 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 108581 | 0.090 | 52765 | 46.36 | 80.75 | 52908 | 208625 | 875545 | 36.01 | 2073 | 2.02 | 0 | 102.0 | 259.7 | 1.85 |
| 1M | 8 | class-fresh | 128.0 | 303.4 | 164.2 | 36999 | 41376 | 131.1 | 24822 | 99913 | 108871 | 82.43 | 1034 | 1.01 | 0 | 0 | 0.000 | 4.23 |
| 4M | 1 | today | 32.00 | 72.20 | 0.090 | 41257 | 46.98 | 70.68 | 40619 | 91733 | 249993 | 9.41 | 8215 | 2.01 | 0 | 4.00 | 509.3 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 62.34 | 0.090 | 56607 | 46.53 | 69.60 | 48661 | 100013 | 183518 | 10.60 | 8215 | 2.01 | 0 | 1.00 | 559.2 | 1.13 |
| 4M | 1 | class-fresh | 32.00 | 109.5 | 68.07 | 24155 | 6273 | 65.03 | 8323 | 41673 | 50045 | 25.08 | 4108 | 1.00 | 0 | 0 | 0.000 | 2.67 |
| 4M | 8 | today | 32.00 | 242541 | 0.080 | 187145 | 52.48 | 74.95 | 93542 | 516422 | 942459 | 16.84 | 8212 | 2.00 | 0 | 20.00 | 425.7 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 208551 | 0.080 | 131998 | 53.50 | 73.86 | 94288 | 358600 | 925577 | 16.19 | 8212 | 2.00 | 0 | 19.00 | 459.1 | 0.962 |
| 4M | 8 | class-fresh | 32.00 | 349.5 | 179.4 | 164454 | 147037 | 133.1 | 24863 | 316691 | 324891 | 25.42 | 4106 | 1.00 | 0 | 0 | 0.000 | 1.51 |

