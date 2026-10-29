### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 24.76 µs |
| fdatasync of a clean file | p50 10.54 µs, p99 25.11 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8071 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8080 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.24 µs, p99 19.99 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 4 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 290.0 | 57.10 | 0.081 | 12778 | 31.03 | 56.14 | 18276 | 41517 | 175277 | 14.49 | 4196 | 65.56 | 0 | 103.0 | 459.5 | 1.00 |
| 64K | 1 | class-pool | 353.0 | 56.26 | 0.160 | 22283 | 38.00 | 55.96 | 29231 | 50526 | 92419 | 17.63 | 155.3 | 2.43 | 0 | 49.00 | 309.2 | 1.22 |
| 64K | 1 | class-fresh | 539.0 | 75.82 | 48.76 | 14063 | 14083 | 67.49 | 8092 | 33413 | 66704 | 26.91 | 82.60 | 1.29 | 0 | 0 | 0.000 | 1.86 |
| 64K | 8 | today | 508.0 | 209206 | 0.070 | 22312 | 23.90 | 39.90 | 89498 | 334168 | 591841 | 25.23 | 4185 | 65.39 | 0 | 411.0 | 617.9 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 75654 | 0.060 | 21835 | 16.71 | 35.74 | 49234 | 142306 | 250494 | 58.30 | 150.9 | 2.36 | 0 | 473.0 | 217.1 | 2.31 |
| 64K | 8 | class-fresh | 600.0 | 181.9 | 106.2 | 20966 | 21991 | 77.66 | 33323 | 75238 | 116926 | 105.8 | 74.39 | 1.16 | 0 | 0 | 0.000 | 4.19 |
| 256K | 1 | today | 286.0 | 60.75 | 0.081 | 13424 | 27.77 | 41.81 | 18278 | 41552 | 167373 | 14.30 | 4387 | 17.14 | 0 | 102.0 | 575.9 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 42.92 | 0.081 | 13741 | 21.37 | 38.27 | 23808 | 41766 | 75323 | 25.78 | 545.6 | 2.13 | 0 | 57.00 | 167.2 | 1.80 |
| 256K | 1 | class-fresh | 512.0 | 66.01 | 45.30 | 7708 | 6545 | 60.37 | 8108 | 25045 | 41707 | 43.30 | 277.0 | 1.08 | 0 | 0 | 0.000 | 3.03 |
| 256K | 8 | today | 494.0 | 200954 | 0.061 | 28504 | 24.32 | 39.77 | 90343 | 342910 | 576581 | 24.48 | 4378 | 17.10 | 0 | 393.0 | 451.0 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 58621 | 0.050 | 15729 | 13.56 | 33.05 | 35614 | 108835 | 217031 | 75.93 | 535.8 | 2.09 | 0 | 401.0 | 133.9 | 3.10 |
| 256K | 8 | class-fresh | 512.0 | 152.4 | 92.47 | 17229 | 17720 | 72.19 | 24945 | 58637 | 100643 | 126.8 | 267.8 | 1.05 | 0 | 0 | 0.000 | 5.18 |
| 1M | 1 | today | 128.0 | 56.30 | 0.091 | 20928 | 38.47 | 54.39 | 17574 | 50038 | 175326 | 13.76 | 5153 | 5.03 | 0 | 41.00 | 467.5 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 55.97 | 0.071 | 13327 | 35.29 | 56.33 | 21758 | 41358 | 108687 | 21.76 | 2079 | 2.03 | 0 | 31.00 | 218.0 | 1.58 |
| 1M | 1 | class-fresh | 128.0 | 64.47 | 41.45 | 11918 | 6835 | 59.79 | 8105 | 25058 | 33452 | 36.57 | 1040 | 1.02 | 0 | 0 | 0.000 | 2.66 |
| 1M | 8 | today | 128.0 | 217067 | 0.070 | 47242 | 28.05 | 50.21 | 86273 | 359478 | 583941 | 23.08 | 5146 | 5.03 | 0 | 100.0 | 434.7 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 108441 | 0.060 | 37230 | 22.35 | 45.48 | 54326 | 200496 | 309679 | 39.78 | 2070 | 2.02 | 0 | 99.00 | 166.4 | 1.72 |
| 1M | 8 | class-fresh | 128.0 | 230.9 | 126.9 | 29691 | 28306 | 75.33 | 25369 | 92818 | 142342 | 80.97 | 1037 | 1.01 | 0 | 0 | 0.000 | 3.51 |
| 4M | 1 | today | 32.00 | 57.05 | 0.080 | 38123 | 38.21 | 54.24 | 16439 | 65907 | 200001 | 11.59 | 8220 | 2.01 | 0 | 9.00 | 408.8 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 54.13 | 0.080 | 40942 | 38.21 | 53.79 | 16405 | 58346 | 216877 | 11.73 | 8217 | 2.01 | 0 | 8.00 | 408.8 | 1.01 |
| 4M | 1 | class-fresh | 32.00 | 66.66 | 42.37 | 29617 | 7299 | 61.78 | 8122 | 41722 | 91043 | 21.56 | 4116 | 1.00 | 0 | 0 | 0.000 | 1.86 |
| 4M | 8 | today | 32.00 | 275726 | 0.040 | 134952 | 29.92 | 57.24 | 116839 | 517379 | 909132 | 16.68 | 8214 | 2.01 | 0 | 22.00 | 458.5 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 250297 | 0.081 | 147853 | 25.77 | 42.42 | 103454 | 475337 | 675463 | 17.94 | 8212 | 2.00 | 0 | 22.00 | 425.9 | 1.08 |
| 4M | 8 | class-fresh | 32.00 | 251.0 | 90.61 | 75649 | 110294 | 83.05 | 24809 | 233559 | 366604 | 33.97 | 4107 | 1.00 | 0 | 0 | 0.000 | 2.04 |

