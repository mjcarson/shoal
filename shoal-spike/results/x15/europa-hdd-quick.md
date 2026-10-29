### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.88 KiB a sync · sync p50 51.78 µs |
| fdatasync of a clean file | p50 14.42 µs, p99 23.67 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8084 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8100 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 14.15 µs, p99 18.31 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 1 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs · **quick: not a measurement**

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 12.00 | 40.14 | 0.070 | 13411 | 25.41 | 37.87 | 15752 | 33295 | 132351 | 26.65 | 4181 | 65.33 | 0 | 1.00 | 500.8 | 1.00 |
| 64K | 1 | class-pool | 12.00 | 52.06 | 0.071 | 23257 | 29.53 | 55.42 | 27152 | 50232 | 75132 | 18.69 | 150.3 | 2.35 | 0 | 0 | 116.6 | 0.701 |
| 64K | 1 | class-fresh | 12.00 | 64.56 | 42.38 | 12766 | 13477 | 58.71 | 8152 | 33372 | 41713 | 28.21 | 76.00 | 1.19 | 0 | 0 | 0.000 | 1.06 |
| 64K | 8 | today | 12.00 | 114.1 | 0.030 | 14081 | 30.76 | 34.09 | 36723 | 58168 | 225437 | 43.60 | 4179 | 65.30 | 0 | 4.00 | 484.3 | 1.00 |
| 64K | 8 | class-pool | 12.00 | 120.2 | 0.030 | 11678 | 28.96 | 33.61 | 28857 | 41356 | 108473 | 84.77 | 145.0 | 2.27 | 0 | 4.00 | 226.2 | 1.94 |
| 64K | 8 | class-fresh | 12.00 | 547.2 | 133.4 | 17837 | 14912 | 130.8 | 24746 | 58328 | 58475 | 102.8 | 71.33 | 1.11 | 0 | 0 | 0.000 | 2.36 |
| 1M | 1 | today | 12.00 | 50.69 | 0.070 | 11751 | 37.65 | 55.26 | 17488 | 33357 | 141884 | 21.49 | 5146 | 5.03 | 0 | 2.00 | 425.9 | 1.00 |
| 1M | 1 | class-pool | 12.00 | 56.48 | 0.080 | 25499 | 30.45 | 56.21 | 61406 | 91557 | 116970 | 12.86 | 2093 | 2.04 | 0 | 0 | 92.25 | 0.598 |
| 1M | 1 | class-fresh | 12.00 | 65.31 | 41.67 | 22095 | 11758 | 59.01 | 8128 | 41703 | 49985 | 23.61 | 1036 | 1.01 | 0 | 0 | 0.000 | 1.10 |
| 1M | 8 | today | 12.00 | 124.1 | 0.040 | 42003 | 31.41 | 48.43 | 120644 | 200258 | 266998 | 32.03 | 5139 | 5.02 | 0 | 4.00 | 442.5 | 1.00 |
| 1M | 8 | class-pool | 12.00 | 122.2 | 0.030 | 29806 | 31.49 | 37.70 | 56004 | 90978 | 167112 | 48.06 | 2065 | 2.02 | 0 | 4.00 | 300.3 | 1.50 |
| 1M | 8 | class-fresh | 12.00 | 603.6 | 161.9 | 50413 | 46003 | 58.14 | 16777 | 116212 | 124722 | 55.45 | 1033 | 1.01 | 0 | 0 | 0.000 | 1.73 |

