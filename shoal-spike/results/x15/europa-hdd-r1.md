### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 24.56 µs |
| fdatasync of a clean file | p50 10.35 µs, p99 20.46 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8102 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8117 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.15 µs, p99 19.72 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 1 (latencies µs p50; directory sync Fdatasync)

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 208.0 | 55.97 | 0.070 | 20860 | 28.27 | 54.85 | 27643 | 58063 | 258542 | 10.37 | 4195 | 65.54 | 0 | 68.00 | 467.5 | 1.00 |
| 64K | 1 | class-pool | 353.0 | 52.84 | 0.071 | 22204 | 37.04 | 53.62 | 29020 | 50079 | 92445 | 17.63 | 158.4 | 2.48 | 0 | 51.00 | 242.3 | 1.70 |
| 64K | 1 | class-fresh | 600.0 | 64.85 | 42.71 | 6225 | 6281 | 56.68 | 8141 | 16770 | 33397 | 47.62 | 81.29 | 1.27 | 0 | 0 | 0.000 | 4.59 |
| 64K | 8 | today | 454.0 | 217526 | 0.060 | 25909 | 25.50 | 39.49 | 124431 | 383497 | 550778 | 22.57 | 4186 | 65.41 | 0 | 363.0 | 550.7 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 83477 | 0.050 | 22337 | 17.65 | 35.31 | 55533 | 158747 | 225611 | 55.99 | 149.0 | 2.33 | 0 | 470.0 | 233.7 | 2.48 |
| 64K | 8 | class-fresh | 600.0 | 155.9 | 101.0 | 13334 | 13770 | 79.91 | 24919 | 50248 | 83754 | 151.7 | 74.74 | 1.17 | 0 | 0 | 0.000 | 6.72 |
| 256K | 1 | today | 291.0 | 54.61 | 0.070 | 12156 | 25.69 | 38.84 | 17571 | 41518 | 175204 | 14.53 | 4390 | 17.15 | 0 | 104.0 | 492.9 | 1.00 |
| 256K | 1 | class-pool | 512.0 | 42.19 | 0.070 | 13387 | 20.67 | 37.81 | 23892 | 41752 | 75261 | 26.13 | 543.9 | 2.12 | 0 | 58.00 | 150.9 | 1.80 |
| 256K | 1 | class-fresh | 512.0 | 64.52 | 42.24 | 7666 | 6641 | 58.92 | 8138 | 25042 | 41670 | 42.71 | 273.4 | 1.07 | 0 | 0 | 0.000 | 2.94 |
| 256K | 8 | today | 492.0 | 217163 | 0.060 | 25519 | 22.67 | 39.53 | 92451 | 333920 | 584087 | 24.33 | 4381 | 17.11 | 0 | 407.0 | 443.2 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 58645 | 0.041 | 13543 | 13.75 | 32.79 | 37151 | 108961 | 167206 | 77.51 | 534.3 | 2.09 | 0 | 408.0 | 151.1 | 3.19 |
| 256K | 8 | class-fresh | 512.0 | 161.5 | 100.3 | 17612 | 17587 | 81.29 | 24893 | 58635 | 100495 | 131.4 | 268.3 | 1.05 | 0 | 0 | 0.000 | 5.40 |
| 1M | 1 | today | 128.0 | 55.79 | 0.060 | 20513 | 38.24 | 54.02 | 18077 | 50116 | 183494 | 13.36 | 5162 | 5.04 | 0 | 42.00 | 434.2 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 55.77 | 0.070 | 13762 | 37.19 | 53.52 | 18717 | 33239 | 108616 | 21.83 | 2079 | 2.03 | 0 | 33.00 | 250.4 | 1.63 |
| 1M | 1 | class-fresh | 128.0 | 64.69 | 41.66 | 12345 | 6716 | 59.17 | 8123 | 25058 | 41721 | 36.24 | 1040 | 1.02 | 0 | 0 | 0.000 | 2.71 |
| 1M | 8 | today | 128.0 | 233739 | 0.051 | 49199 | 28.06 | 42.39 | 93581 | 375520 | 550800 | 22.03 | 5145 | 5.02 | 0 | 99.00 | 426.0 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 108846 | 0.040 | 30452 | 22.56 | 45.13 | 61507 | 200465 | 284061 | 42.55 | 2070 | 2.02 | 0 | 99.00 | 242.4 | 1.93 |
| 1M | 8 | class-fresh | 128.0 | 208.1 | 124.9 | 28058 | 30090 | 79.09 | 25157 | 100123 | 166410 | 78.50 | 1037 | 1.01 | 0 | 0 | 0.000 | 3.56 |
| 4M | 1 | today | 32.00 | 54.05 | 0.070 | 41029 | 39.50 | 57.70 | 18092 | 58385 | 175425 | 11.84 | 8225 | 2.01 | 0 | 8.00 | 451.3 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 55.86 | 0.071 | 39883 | 40.36 | 64.72 | 17311 | 58476 | 258397 | 11.45 | 8246 | 2.01 | 0 | 8.00 | 475.7 | 0.967 |
| 4M | 1 | class-fresh | 32.00 | 68.42 | 43.65 | 29633 | 7213 | 61.17 | 8110 | 41741 | 50114 | 22.44 | 4112 | 1.00 | 0 | 0 | 0.000 | 1.90 |
| 4M | 8 | today | 32.00 | 276065 | 0.050 | 123858 | 27.98 | 51.59 | 124973 | 500364 | 850874 | 16.76 | 8216 | 2.01 | 0 | 21.00 | 509.3 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 200093 | 0.030 | 173699 | 23.41 | 43.35 | 88481 | 475383 | 700805 | 18.36 | 8213 | 2.01 | 0 | 22.00 | 467.6 | 1.10 |
| 4M | 8 | class-fresh | 32.00 | 238.0 | 132.2 | 100993 | 102627 | 58.02 | 24602 | 258613 | 458436 | 31.43 | 4108 | 1.00 | 0 | 0 | 0.000 | 1.88 |

