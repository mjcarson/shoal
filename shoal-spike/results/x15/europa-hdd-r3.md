### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 24.84 µs |
| fdatasync of a clean file | p50 10.35 µs, p99 24.46 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8099 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8103 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.11 µs, p99 20.32 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 3 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 179.0 | 66.36 | 0.090 | 24562 | 30.97 | 57.46 | 29853 | 58384 | 291983 | 8.90 | 4195 | 65.55 | 0 | 59.00 | 608.4 | 1.00 |
| 64K | 1 | class-pool | 538.0 | 42.32 | 0.070 | 13027 | 21.20 | 38.66 | 22325 | 33568 | 75233 | 26.85 | 159.0 | 2.48 | 0 | 43.00 | 150.1 | 3.02 |
| 64K | 1 | class-fresh | 551.0 | 66.95 | 46.84 | 14084 | 13727 | 60.52 | 8121 | 33406 | 50042 | 27.53 | 80.91 | 1.26 | 0 | 0 | 0.000 | 3.09 |
| 64K | 8 | today | 334.0 | 325513 | 0.060 | 46245 | 27.82 | 41.08 | 143555 | 525985 | 725641 | 16.52 | 4188 | 65.44 | 0 | 268.0 | 659.3 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 42940 | 0.040 | 13591 | 13.81 | 32.51 | 30486 | 92170 | 150626 | 91.82 | 149.1 | 2.33 | 0 | 487.0 | 92.31 | 5.56 |
| 64K | 8 | class-fresh | 600.0 | 174.6 | 101.0 | 20564 | 22172 | 72.29 | 33249 | 75142 | 133793 | 103.7 | 74.41 | 1.16 | 0 | 0 | 0.000 | 6.28 |
| 256K | 1 | today | 211.0 | 66.72 | 0.080 | 22495 | 28.08 | 50.76 | 28244 | 58204 | 250516 | 10.52 | 4385 | 17.13 | 0 | 69.00 | 483.7 | 1.00 |
| 256K | 1 | class-pool | 356.0 | 55.87 | 0.080 | 21202 | 27.57 | 44.31 | 28733 | 50068 | 100789 | 17.76 | 543.3 | 2.12 | 0 | 54.00 | 250.4 | 1.69 |
| 256K | 1 | class-fresh | 512.0 | 76.20 | 50.95 | 15028 | 14116 | 93.38 | 8099 | 33442 | 67013 | 26.11 | 275.3 | 1.08 | 0 | 0 | 0.000 | 2.48 |
| 256K | 8 | today | 446.0 | 226197 | 0.070 | 30461 | 26.13 | 39.73 | 122436 | 375657 | 551153 | 22.14 | 4376 | 17.09 | 0 | 359.0 | 533.6 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 92007 | 0.060 | 23122 | 17.80 | 35.66 | 60832 | 175374 | 217635 | 51.18 | 535.7 | 2.09 | 0 | 400.0 | 191.8 | 2.31 |
| 256K | 8 | class-fresh | 512.0 | 174.8 | 106.3 | 24080 | 24708 | 75.80 | 33586 | 83657 | 150373 | 90.14 | 267.9 | 1.05 | 0 | 0 | 0.000 | 4.07 |
| 1M | 1 | today | 128.0 | 66.69 | 0.170 | 28021 | 43.13 | 79.81 | 27389 | 58606 | 258580 | 10.46 | 5152 | 5.03 | 0 | 39.00 | 442.2 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 66.76 | 0.160 | 25266 | 30.56 | 56.74 | 59472 | 83545 | 192168 | 11.76 | 2087 | 2.04 | 0 | 7.00 | 301.0 | 1.12 |
| 1M | 1 | class-fresh | 128.0 | 80.53 | 52.32 | 18127 | 14580 | 95.15 | 8080 | 41713 | 58397 | 24.27 | 1040 | 1.02 | 0 | 0 | 0.000 | 2.32 |
| 1M | 8 | today | 128.0 | 217440 | 0.070 | 68097 | 28.53 | 41.44 | 85857 | 384117 | 699068 | 21.42 | 5141 | 5.02 | 0 | 99.00 | 491.8 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 141816 | 0.071 | 39118 | 22.31 | 39.21 | 95910 | 267438 | 317313 | 32.75 | 2068 | 2.02 | 0 | 95.00 | 258.1 | 1.53 |
| 1M | 8 | class-fresh | 128.0 | 254.6 | 150.0 | 42334 | 35892 | 69.66 | 24560 | 100354 | 133672 | 77.36 | 1036 | 1.01 | 0 | 0 | 0.000 | 3.61 |
| 4M | 1 | today | 32.00 | 72.50 | 0.120 | 43717 | 35.42 | 69.64 | 24734 | 74894 | 308640 | 8.94 | 8242 | 2.01 | 0 | 8.00 | 483.7 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 63.93 | 0.080 | 44847 | 38.60 | 85.60 | 26856 | 75007 | 275251 | 9.20 | 8221 | 2.01 | 0 | 8.00 | 550.6 | 1.03 |
| 4M | 1 | class-fresh | 32.00 | 81.86 | 59.55 | 32270 | 14575 | 96.40 | 8090 | 58344 | 58432 | 18.27 | 4112 | 1.00 | 0 | 0 | 0.000 | 2.04 |
| 4M | 8 | today | 32.00 | 256791 | 0.030 | 129653 | 30.35 | 41.65 | 123478 | 506172 | 942879 | 15.73 | 8214 | 2.01 | 0 | 21.00 | 517.5 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 240471 | 0.070 | 114202 | 29.65 | 44.16 | 148855 | 533447 | 766979 | 16.82 | 8213 | 2.01 | 0 | 21.00 | 450.3 | 1.07 |
| 4M | 8 | class-fresh | 32.00 | 204.5 | 153.0 | 116787 | 101993 | 67.63 | 24534 | 241936 | 252409 | 33.66 | 4108 | 1.00 | 0 | 0 | 0.000 | 2.14 |

