### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 22.14 µs |
| fdatasync of a clean file | p50 10.31 µs, p99 24.13 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8080 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8085 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.32 µs, p99 19.90 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 2 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 182.0 | 55.68 | 0.080 | 23325 | 30.00 | 55.40 | 30099 | 58418 | 300515 | 9.03 | 4195 | 65.55 | 0 | 60.00 | 709.0 | 1.00 |
| 64K | 1 | class-pool | 361.0 | 53.46 | 0.070 | 20963 | 37.36 | 56.20 | 28330 | 50059 | 100325 | 18.01 | 158.0 | 2.47 | 0 | 50.00 | 183.5 | 1.99 |
| 64K | 1 | class-fresh | 600.0 | 64.06 | 41.97 | 6503 | 6211 | 58.43 | 8133 | 24993 | 33574 | 46.79 | 82.79 | 1.29 | 0 | 0 | 0.000 | 5.18 |
| 64K | 8 | today | 337.0 | 309717 | 0.070 | 50355 | 27.58 | 45.63 | 129026 | 517082 | 734850 | 16.53 | 4189 | 65.45 | 0 | 272.0 | 592.3 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 75339 | 0.050 | 20927 | 19.65 | 36.76 | 49196 | 142214 | 208516 | 60.17 | 147.9 | 2.31 | 0 | 472.0 | 183.7 | 3.64 |
| 64K | 8 | class-fresh | 600.0 | 158.1 | 96.79 | 12208 | 13835 | 75.56 | 24821 | 50206 | 83654 | 160.2 | 74.45 | 1.16 | 0 | 0 | 0.000 | 9.69 |
| 256K | 1 | today | 175.0 | 65.00 | 0.090 | 25617 | 38.31 | 60.74 | 30834 | 66101 | 325233 | 8.66 | 4391 | 17.15 | 0 | 56.00 | 650.9 | 1.00 |
| 256K | 1 | class-pool | 338.0 | 54.95 | 0.080 | 23260 | 37.89 | 56.28 | 29682 | 58194 | 108533 | 16.86 | 541.3 | 2.11 | 0 | 54.00 | 275.7 | 1.95 |
| 256K | 1 | class-fresh | 490.0 | 64.08 | 41.33 | 16787 | 15039 | 59.91 | 8116 | 41714 | 66716 | 24.50 | 275.5 | 1.08 | 0 | 0 | 0.000 | 2.83 |
| 256K | 8 | today | 316.0 | 341975 | 0.071 | 47411 | 34.73 | 57.26 | 155519 | 542027 | 809506 | 15.61 | 4378 | 17.10 | 0 | 255.0 | 567.9 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 100413 | 0.050 | 25856 | 18.61 | 38.72 | 66640 | 191821 | 258220 | 46.43 | 535.9 | 2.09 | 0 | 401.0 | 234.4 | 2.97 |
| 256K | 8 | class-fresh | 512.0 | 176.8 | 105.2 | 27783 | 26026 | 96.22 | 33586 | 83840 | 150558 | 85.26 | 267.8 | 1.05 | 0 | 0 | 0.000 | 5.46 |
| 1M | 1 | today | 128.0 | 65.06 | 0.080 | 37537 | 40.17 | 59.17 | 30946 | 75038 | 341890 | 8.03 | 5154 | 5.03 | 0 | 38.00 | 576.0 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 43.99 | 0.070 | 30167 | 37.69 | 55.95 | 64131 | 91918 | 158344 | 10.64 | 2079 | 2.03 | 0 | 5.00 | 284.0 | 1.32 |
| 1M | 1 | class-fresh | 128.0 | 65.21 | 41.83 | 23059 | 15554 | 60.99 | 8112 | 49969 | 50431 | 21.64 | 1038 | 1.01 | 0 | 0 | 0.000 | 2.69 |
| 1M | 8 | today | 128.0 | 367095 | 0.070 | 85932 | 37.73 | 57.61 | 132089 | 567027 | 809450 | 14.92 | 5144 | 5.02 | 0 | 99.00 | 600.3 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 167256 | 0.080 | 67792 | 27.69 | 52.56 | 93822 | 322522 | 417551 | 26.67 | 2069 | 2.02 | 0 | 96.00 | 284.0 | 1.79 |
| 1M | 8 | class-fresh | 128.0 | 227.2 | 123.4 | 63504 | 46384 | 60.75 | 24515 | 125463 | 167107 | 59.40 | 1036 | 1.01 | 0 | 0 | 0.000 | 3.98 |
| 4M | 1 | today | 32.00 | 66.59 | 0.071 | 61139 | 41.33 | 77.66 | 32411 | 108409 | 317017 | 7.20 | 8221 | 2.01 | 0 | 6.00 | 717.9 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 59.75 | 0.070 | 62087 | 42.43 | 87.05 | 29678 | 108408 | 349992 | 6.97 | 8238 | 2.01 | 0 | 7.00 | 633.8 | 0.967 |
| 4M | 1 | class-fresh | 32.00 | 73.09 | 44.04 | 47031 | 16143 | 94.95 | 8099 | 75034 | 100045 | 13.51 | 4112 | 1.00 | 0 | 0 | 0.000 | 1.88 |
| 4M | 8 | today | 32.00 | 384261 | 0.080 | 197490 | 40.47 | 57.86 | 204192 | 767287 | 1092202 | 11.39 | 8213 | 2.01 | 0 | 21.00 | 509.2 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 392058 | 0.040 | 212073 | 38.95 | 58.91 | 157963 | 767113 | 1042374 | 11.32 | 8214 | 2.01 | 0 | 21.00 | 550.1 | 0.994 |
| 4M | 8 | class-fresh | 32.00 | 241.0 | 161.5 | 173691 | 146771 | 75.25 | 24494 | 344997 | 358836 | 23.40 | 4108 | 1.00 | 0 | 0 | 0.000 | 2.05 |

