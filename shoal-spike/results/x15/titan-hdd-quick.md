### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 29.55 µs |
| fdatasync of a clean file | p50 23.88 µs, p99 37.16 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8232 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8243 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.19 µs, p99 33.12 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 1 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs · **quick: not a measurement**

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 12.00 | 75.14 | 0.080 | 40605 | 48.29 | 83.13 | 65266 | 108342 | 116726 | 9.79 | 4180 | 65.31 | 0 | 0 | 116.6 | 1.00 |
| 64K | 1 | class-pool | 12.00 | 73.12 | 0.090 | 14600 | 33.56 | 70.81 | 25042 | 41633 | 41709 | 33.46 | 150.3 | 2.35 | 0 | 0 | 126.0 | 3.42 |
| 64K | 1 | class-fresh | 12.00 | 116.3 | 74.09 | 24432 | 25055 | 74.63 | 8305 | 58338 | 58377 | 17.77 | 76.00 | 1.19 | 0 | 0 | 0.000 | 1.81 |
| 64K | 8 | today | 12.00 | 230.1 | 0.090 | 40199 | 71.62 | 88.84 | 39663 | 74688 | 458347 | 22.84 | 4178 | 65.28 | 0 | 4.00 | 724.9 | 1.00 |
| 64K | 8 | class-pool | 12.00 | 203.0 | 0.060 | 26495 | 68.34 | 62.63 | 23405 | 58141 | 125157 | 68.52 | 145.0 | 2.27 | 0 | 4.00 | 300.7 | 3.00 |
| 64K | 8 | class-fresh | 12.00 | 981.6 | 260.0 | 16638 | 15471 | 113.2 | 24903 | 58267 | 66755 | 102.8 | 72.33 | 1.13 | 0 | 0 | 0.000 | 4.50 |
| 1M | 1 | today | 12.00 | 78.88 | 0.080 | 36380 | 50.58 | 82.97 | 31905 | 75014 | 108407 | 14.12 | 5140 | 5.02 | 0 | 0 | 425.9 | 1.00 |
| 1M | 1 | class-pool | 12.00 | 68.52 | 0.090 | 27215 | 48.04 | 79.29 | 28237 | 58372 | 91694 | 16.95 | 2069 | 2.02 | 0 | 0 | 308.6 | 1.20 |
| 1M | 1 | class-fresh | 12.00 | 118.4 | 73.56 | 29297 | 17225 | 73.50 | 8312 | 57632 | 75012 | 19.47 | 1036 | 1.01 | 0 | 0 | 0.000 | 1.38 |
| 1M | 8 | today | 12.00 | 183.1 | 0.050 | 63986 | 49.99 | 66.53 | 51821 | 99036 | 316842 | 29.41 | 5137 | 5.02 | 0 | 4.00 | 492.4 | 1.00 |
| 1M | 8 | class-pool | 12.00 | 173.1 | 0.060 | 41135 | 70.40 | 73.26 | 41703 | 82643 | 249891 | 36.98 | 2065 | 2.02 | 0 | 4.00 | 208.8 | 1.26 |
| 1M | 8 | class-fresh | 12.00 | 938.1 | 356.9 | 42209 | 46043 | 114.5 | 16667 | 124420 | 124462 | 55.49 | 1032 | 1.01 | 0 | 0 | 0.000 | 1.89 |

