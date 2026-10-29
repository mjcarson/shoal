### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.28 KiB a sync · sync p50 28.53 µs |
| fdatasync of a clean file | p50 23.76 µs, p99 37.42 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8250 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8260 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.08 µs, p99 32.86 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 3 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 167.0 | 65.44 | 0.090 | 25938 | 45.66 | 68.61 | 44479 | 100043 | 283509 | 8.30 | 4188 | 65.43 | 0 | 39.00 | 584.1 | 1.00 |
| 64K | 1 | class-pool | 456.0 | 67.53 | 0.091 | 17584 | 45.34 | 71.70 | 20570 | 41714 | 91867 | 22.76 | 156.6 | 2.45 | 0 | 45.00 | 234.4 | 2.74 |
| 64K | 1 | class-fresh | 445.0 | 109.0 | 78.14 | 17644 | 17613 | 67.43 | 8317 | 41704 | 66686 | 22.21 | 83.76 | 1.31 | 0 | 0 | 0.000 | 2.68 |
| 64K | 8 | today | 488.0 | 191748 | 0.090 | 49495 | 51.56 | 75.40 | 97819 | 342355 | 551021 | 24.39 | 4187 | 65.42 | 0 | 387.0 | 492.4 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 66944 | 0.090 | 21971 | 34.06 | 65.76 | 37499 | 125353 | 192200 | 66.27 | 147.6 | 2.31 | 0 | 473.0 | 217.1 | 2.72 |
| 64K | 8 | class-fresh | 600.0 | 284.2 | 189.8 | 22071 | 21194 | 122.2 | 24902 | 66895 | 91585 | 117.8 | 75.49 | 1.18 | 0 | 0 | 0.000 | 4.83 |
| 256K | 1 | today | 167.0 | 63.74 | 0.090 | 30891 | 46.85 | 70.53 | 40648 | 83404 | 300054 | 8.29 | 4391 | 17.15 | 0 | 39.00 | 742.0 | 1.00 |
| 256K | 1 | class-pool | 427.0 | 78.02 | 0.090 | 23820 | 45.37 | 69.39 | 24458 | 50038 | 66751 | 21.06 | 541.4 | 2.11 | 0 | 1.00 | 216.9 | 2.54 |
| 256K | 1 | class-fresh | 512.0 | 108.7 | 78.24 | 13358 | 12206 | 64.84 | 8321 | 33369 | 50047 | 29.14 | 273.0 | 1.07 | 0 | 0 | 0.000 | 3.51 |
| 256K | 8 | today | 483.0 | 200387 | 0.100 | 37886 | 52.00 | 71.08 | 90196 | 350548 | 725493 | 23.92 | 4379 | 17.10 | 0 | 384.0 | 534.7 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 75272 | 0.090 | 24281 | 34.27 | 66.98 | 43973 | 142054 | 225545 | 58.08 | 531.4 | 2.08 | 0 | 401.0 | 217.0 | 2.43 |
| 256K | 8 | class-fresh | 512.0 | 287.7 | 184.8 | 18363 | 18516 | 123.2 | 24902 | 58603 | 92062 | 128.8 | 267.8 | 1.05 | 0 | 0 | 0.000 | 5.38 |
| 1M | 1 | today | 128.0 | 64.33 | 0.090 | 26623 | 47.77 | 70.89 | 25653 | 58380 | 233438 | 11.65 | 5161 | 5.04 | 0 | 27.00 | 475.7 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 72.13 | 0.100 | 28709 | 46.09 | 67.22 | 30360 | 58406 | 141844 | 13.33 | 2074 | 2.03 | 0 | 27.00 | 350.8 | 1.14 |
| 1M | 1 | class-fresh | 128.0 | 109.0 | 69.11 | 23761 | 17745 | 66.30 | 8316 | 50022 | 58389 | 19.93 | 1039 | 1.01 | 0 | 0 | 0.000 | 1.71 |
| 1M | 8 | today | 128.0 | 208686 | 0.091 | 62770 | 54.08 | 71.36 | 87080 | 375458 | 734083 | 21.79 | 5148 | 5.03 | 0 | 97.00 | 467.1 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 116822 | 0.090 | 55446 | 40.05 | 90.49 | 52059 | 225138 | 333961 | 38.16 | 2066 | 2.02 | 0 | 97.00 | 276.3 | 1.75 |
| 1M | 8 | class-fresh | 128.0 | 350.4 | 190.9 | 39327 | 41334 | 124.9 | 16760 | 100099 | 116719 | 81.12 | 1034 | 1.01 | 0 | 0 | 0.000 | 3.72 |
| 4M | 1 | today | 32.00 | 64.31 | 0.090 | 47827 | 47.36 | 69.37 | 36195 | 100016 | 241763 | 10.05 | 8217 | 2.01 | 0 | 2.00 | 542.8 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 74.73 | 0.090 | 58950 | 47.24 | 72.28 | 33607 | 116661 | 275164 | 7.66 | 8216 | 2.01 | 0 | 6.00 | 642.8 | 0.763 |
| 4M | 1 | class-fresh | 32.00 | 109.0 | 67.81 | 34913 | 12501 | 67.65 | 8319 | 58329 | 66713 | 17.77 | 4108 | 1.00 | 0 | 0 | 0.000 | 1.77 |
| 4M | 8 | today | 32.00 | 216706 | 0.090 | 158172 | 50.41 | 68.48 | 84060 | 416754 | 683907 | 18.72 | 8211 | 2.00 | 0 | 20.00 | 467.4 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 242010 | 0.060 | 137902 | 52.70 | 68.53 | 92566 | 450431 | 708919 | 17.85 | 8213 | 2.01 | 0 | 21.00 | 475.6 | 0.954 |
| 4M | 8 | class-fresh | 32.00 | 402.7 | 168.5 | 205561 | 171975 | 126.0 | 24816 | 383466 | 399918 | 20.76 | 4104 | 1.00 | 0 | 0 | 0.000 | 1.11 |

