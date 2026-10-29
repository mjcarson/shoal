### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.55 µs |
| fdatasync of a clean file | p50 23.86 µs, p99 41.83 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8247 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8264 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.05 µs, p99 33.89 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 1 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 207.0 | 66.53 | 0.080 | 24654 | 45.81 | 70.65 | 48311 | 83400 | 241802 | 10.30 | 4192 | 65.50 | 0 | 37.00 | 534.4 | 1.00 |
| 64K | 1 | class-pool | 341.0 | 60.80 | 0.080 | 36755 | 45.16 | 68.38 | 22382 | 58394 | 108470 | 17.04 | 154.6 | 2.42 | 0 | 12.00 | 259.1 | 1.65 |
| 64K | 1 | class-fresh | 416.0 | 108.9 | 77.86 | 20781 | 18838 | 68.42 | 8316 | 50020 | 66662 | 20.77 | 84.43 | 1.32 | 0 | 0 | 0.000 | 2.02 |
| 64K | 8 | today | 478.0 | 200884 | 0.080 | 39259 | 51.77 | 66.88 | 84631 | 350654 | 725211 | 23.80 | 4186 | 65.40 | 0 | 381.0 | 509.2 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 50854 | 0.080 | 16490 | 33.08 | 65.36 | 31859 | 100704 | 167066 | 79.93 | 148.4 | 2.32 | 0 | 477.0 | 134.5 | 3.36 |
| 64K | 8 | class-fresh | 600.0 | 289.3 | 193.2 | 23282 | 24360 | 121.3 | 24896 | 66946 | 91614 | 113.0 | 75.59 | 1.18 | 0 | 0 | 0.000 | 4.75 |
| 256K | 1 | today | 205.0 | 65.17 | 0.080 | 26461 | 46.31 | 71.97 | 46384 | 83406 | 283433 | 10.24 | 4390 | 17.15 | 0 | 36.00 | 575.7 | 1.00 |
| 256K | 1 | class-pool | 461.0 | 75.27 | 0.080 | 15025 | 44.84 | 69.85 | 23434 | 41699 | 108532 | 23.04 | 542.0 | 2.12 | 0 | 45.00 | 200.5 | 2.25 |
| 256K | 1 | class-fresh | 478.0 | 108.6 | 79.21 | 16854 | 16202 | 65.26 | 8319 | 41699 | 58362 | 23.86 | 273.0 | 1.07 | 0 | 0 | 0.000 | 2.33 |
| 256K | 8 | today | 388.0 | 250340 | 0.090 | 41003 | 52.08 | 73.33 | 131581 | 450379 | 792054 | 19.07 | 4380 | 17.11 | 0 | 307.0 | 500.4 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 50365 | 0.081 | 13710 | 33.94 | 65.41 | 30734 | 100078 | 310182 | 79.12 | 533.6 | 2.08 | 0 | 388.0 | 167.6 | 4.15 |
| 256K | 8 | class-fresh | 512.0 | 285.8 | 184.5 | 32269 | 25047 | 119.8 | 24882 | 83348 | 100643 | 99.26 | 267.5 | 1.05 | 0 | 0 | 0.000 | 5.20 |
| 1M | 1 | today | 128.0 | 74.82 | 0.081 | 23512 | 46.94 | 72.17 | 29070 | 75035 | 225106 | 10.81 | 5152 | 5.03 | 0 | 25.00 | 567.5 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 70.61 | 0.080 | 14893 | 45.17 | 64.40 | 8864 | 50033 | 100093 | 21.88 | 2075 | 2.03 | 0 | 27.00 | 234.6 | 2.02 |
| 1M | 1 | class-fresh | 128.0 | 108.2 | 68.65 | 18455 | 12373 | 65.15 | 8321 | 41694 | 50394 | 23.87 | 1039 | 1.01 | 0 | 0 | 0.000 | 2.21 |
| 1M | 8 | today | 128.0 | 299960 | 0.080 | 71637 | 54.51 | 69.81 | 94911 | 508431 | 850367 | 16.74 | 5147 | 5.03 | 0 | 101.0 | 559.4 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 174903 | 0.080 | 95310 | 42.71 | 85.94 | 72086 | 317144 | 667365 | 25.03 | 2067 | 2.02 | 0 | 97.00 | 409.5 | 1.50 |
| 1M | 8 | class-fresh | 128.0 | 284.0 | 166.7 | 41530 | 46068 | 114.2 | 16770 | 108221 | 133343 | 75.56 | 1034 | 1.01 | 0 | 0 | 0.000 | 4.51 |
| 4M | 1 | today | 32.00 | 72.89 | 0.081 | 59243 | 47.15 | 69.28 | 36243 | 108411 | 275185 | 7.82 | 8217 | 2.01 | 0 | 5.00 | 708.8 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 63.47 | 0.100 | 50334 | 47.42 | 72.84 | 48741 | 108356 | 191724 | 9.87 | 8216 | 2.01 | 0 | 2.00 | 534.2 | 1.26 |
| 4M | 1 | class-fresh | 32.00 | 108.8 | 68.38 | 27216 | 10217 | 65.83 | 8320 | 50004 | 66694 | 21.33 | 4108 | 1.00 | 0 | 0 | 0.000 | 2.73 |
| 4M | 8 | today | 32.00 | 241972 | 0.070 | 135198 | 52.67 | 68.47 | 89050 | 458491 | 559241 | 19.19 | 8210 | 2.00 | 0 | 20.00 | 525.6 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 366705 | 0.060 | 271368 | 52.98 | 78.85 | 141971 | 733840 | 1717817 | 10.07 | 8247 | 2.01 | 0 | 20.00 | 634.5 | 0.525 |
| 4M | 8 | class-fresh | 32.00 | 436.4 | 200.3 | 133558 | 104018 | 145.8 | 16717 | 266979 | 491753 | 27.62 | 4105 | 1.00 | 0 | 0 | 0.000 | 1.44 |

