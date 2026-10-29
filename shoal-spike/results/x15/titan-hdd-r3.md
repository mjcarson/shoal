### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 29.05 µs |
| fdatasync of a clean file | p50 23.76 µs, p99 40.53 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8246 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8258 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.89 µs, p99 33.23 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 3 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 200.0 | 75.93 | 0.080 | 23686 | 47.84 | 80.72 | 39754 | 66736 | 241824 | 9.98 | 4192 | 65.50 | 0 | 49.00 | 634.5 | 1.00 |
| 64K | 1 | class-pool | 382.0 | 62.81 | 0.080 | 31091 | 45.74 | 72.89 | 17803 | 50042 | 116817 | 19.09 | 158.2 | 2.47 | 0 | 18.00 | 242.6 | 1.91 |
| 64K | 1 | class-fresh | 513.0 | 113.8 | 80.66 | 15070 | 14780 | 71.02 | 8311 | 41685 | 58374 | 25.61 | 80.91 | 1.26 | 0 | 0 | 0.000 | 2.57 |
| 64K | 8 | today | 496.0 | 208446 | 0.080 | 44513 | 51.70 | 75.11 | 74794 | 342206 | 599188 | 24.60 | 4188 | 65.44 | 0 | 398.0 | 458.9 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 59018 | 0.080 | 20592 | 34.17 | 68.28 | 35178 | 117055 | 183789 | 73.65 | 147.6 | 2.31 | 0 | 467.0 | 176.0 | 2.99 |
| 64K | 8 | class-fresh | 600.0 | 291.1 | 187.5 | 23062 | 23267 | 120.7 | 24839 | 66863 | 325352 | 107.3 | 77.74 | 1.21 | 0 | 0 | 0.000 | 4.36 |
| 256K | 1 | today | 199.0 | 75.48 | 0.080 | 23942 | 47.64 | 73.99 | 37649 | 75047 | 266803 | 9.94 | 4380 | 17.11 | 0 | 50.00 | 675.8 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 67.69 | 0.080 | 13243 | 38.22 | 70.12 | 21089 | 33399 | 108526 | 26.53 | 543.8 | 2.12 | 0 | 9.00 | 17.55 | 2.67 |
| 256K | 1 | class-fresh | 450.0 | 115.7 | 81.17 | 16858 | 15916 | 74.23 | 8309 | 41703 | 58393 | 22.48 | 275.3 | 1.08 | 0 | 0 | 0.000 | 2.26 |
| 256K | 8 | today | 375.0 | 274926 | 0.080 | 46027 | 52.26 | 72.69 | 106080 | 466937 | 667559 | 18.44 | 4376 | 17.10 | 0 | 299.0 | 542.3 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 59695 | 0.090 | 18415 | 34.19 | 71.35 | 35823 | 117099 | 467044 | 68.94 | 534.9 | 2.09 | 0 | 398.0 | 167.6 | 3.74 |
| 256K | 8 | class-fresh | 512.0 | 303.1 | 187.2 | 23735 | 22656 | 123.3 | 24884 | 66942 | 100201 | 112.1 | 267.5 | 1.04 | 0 | 0 | 0.000 | 6.08 |
| 1M | 1 | today | 128.0 | 75.88 | 0.090 | 21873 | 48.28 | 75.48 | 28621 | 66642 | 200217 | 11.73 | 5148 | 5.03 | 0 | 31.00 | 442.9 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 72.05 | 0.081 | 25372 | 47.35 | 74.42 | 25264 | 50039 | 134177 | 16.35 | 2089 | 2.04 | 0 | 18.00 | 708.8 | 1.39 |
| 1M | 1 | class-fresh | 128.0 | 108.8 | 68.51 | 20048 | 15463 | 66.47 | 8317 | 41701 | 74994 | 21.46 | 1038 | 1.01 | 0 | 0 | 0.000 | 1.83 |
| 1M | 8 | today | 128.0 | 266957 | 0.080 | 146973 | 52.79 | 79.71 | 87127 | 508582 | 733064 | 17.44 | 5141 | 5.02 | 0 | 95.00 | 642.9 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 125549 | 0.080 | 53623 | 47.50 | 89.04 | 65329 | 241921 | 383616 | 34.17 | 2066 | 2.02 | 0 | 97.00 | 425.8 | 1.96 |
| 1M | 8 | class-fresh | 128.0 | 294.6 | 164.2 | 42031 | 45009 | 138.3 | 22959 | 108388 | 125012 | 75.15 | 1034 | 1.01 | 0 | 0 | 0.000 | 4.31 |
| 4M | 1 | today | 32.00 | 78.10 | 0.090 | 68848 | 49.87 | 88.99 | 47007 | 116701 | 308563 | 7.33 | 8217 | 2.01 | 0 | 4.00 | 759.6 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 78.13 | 0.090 | 51443 | 50.91 | 91.68 | 37381 | 100039 | 274982 | 8.31 | 8235 | 2.01 | 0 | 5.00 | 551.0 | 1.13 |
| 4M | 1 | class-fresh | 32.00 | 115.0 | 71.73 | 24214 | 6758 | 74.39 | 8309 | 41669 | 66662 | 25.10 | 4108 | 1.00 | 0 | 0 | 0.000 | 3.43 |
| 4M | 8 | today | 32.00 | 242204 | 0.070 | 150470 | 53.24 | 77.90 | 92625 | 483698 | 558654 | 18.37 | 8209 | 2.00 | 0 | 20.00 | 534.0 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 267048 | 0.060 | 178026 | 53.47 | 74.63 | 97576 | 533838 | 600478 | 16.84 | 8211 | 2.00 | 0 | 21.00 | 483.4 | 0.917 |
| 4M | 8 | class-fresh | 32.00 | 436.2 | 203.9 | 150756 | 133501 | 120.0 | 16816 | 283433 | 299512 | 28.03 | 4104 | 1.00 | 0 | 0 | 0.000 | 1.53 |

