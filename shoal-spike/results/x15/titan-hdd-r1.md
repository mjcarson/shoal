### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 31.93 µs |
| fdatasync of a clean file | p50 23.95 µs, p99 46.75 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8236 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8254 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.85 µs, p99 33.44 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 1 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 234.0 | 76.56 | 0.080 | 17497 | 46.91 | 77.33 | 26958 | 58394 | 233504 | 11.69 | 4191 | 65.48 | 0 | 65.00 | 450.7 | 1.00 |
| 64K | 1 | class-pool | 319.0 | 72.54 | 0.080 | 24847 | 46.10 | 79.18 | 29690 | 58369 | 141825 | 15.94 | 158.9 | 2.48 | 0 | 33.00 | 308.7 | 1.36 |
| 64K | 1 | class-fresh | 600.0 | 110.3 | 80.64 | 6053 | 6135 | 66.86 | 8318 | 17427 | 41722 | 46.15 | 81.03 | 1.27 | 0 | 0 | 0.000 | 3.95 |
| 64K | 8 | today | 320.0 | 300604 | 0.080 | 55862 | 52.15 | 81.62 | 96814 | 493052 | 1076588 | 15.83 | 4189 | 65.46 | 0 | 258.0 | 659.4 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 58372 | 0.071 | 18438 | 33.78 | 67.73 | 31899 | 108571 | 167067 | 79.48 | 148.3 | 2.32 | 0 | 473.0 | 200.9 | 5.02 |
| 64K | 8 | class-fresh | 600.0 | 297.6 | 204.1 | 25285 | 25054 | 137.0 | 24864 | 75084 | 300153 | 99.40 | 78.66 | 1.23 | 0 | 0 | 0.000 | 6.28 |
| 256K | 1 | today | 240.0 | 76.35 | 0.080 | 17859 | 47.98 | 76.81 | 28883 | 58407 | 208535 | 11.98 | 4381 | 17.11 | 0 | 64.00 | 467.3 | 1.00 |
| 256K | 1 | class-pool | 371.0 | 72.06 | 0.080 | 29818 | 46.71 | 80.09 | 27468 | 50061 | 91736 | 18.54 | 542.3 | 2.12 | 0 | 2.00 | 242.7 | 1.55 |
| 256K | 1 | class-fresh | 382.0 | 114.9 | 80.87 | 20765 | 19427 | 72.99 | 8308 | 50036 | 75015 | 19.07 | 276.3 | 1.08 | 0 | 0 | 0.000 | 1.59 |
| 256K | 8 | today | 444.0 | 234521 | 0.080 | 41958 | 52.87 | 72.09 | 83103 | 383969 | 617495 | 21.89 | 4376 | 17.09 | 0 | 357.0 | 359.0 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 66856 | 0.080 | 21655 | 33.93 | 68.11 | 35178 | 117305 | 325612 | 66.83 | 535.5 | 2.09 | 0 | 401.0 | 209.0 | 3.05 |
| 256K | 8 | class-fresh | 512.0 | 290.2 | 195.6 | 27148 | 27169 | 125.1 | 24908 | 75399 | 124973 | 98.76 | 267.8 | 1.05 | 0 | 0 | 0.000 | 4.51 |
| 1M | 1 | today | 128.0 | 70.15 | 0.080 | 36061 | 49.98 | 76.80 | 52232 | 100017 | 233522 | 10.22 | 5147 | 5.03 | 0 | 14.00 | 617.6 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 68.56 | 0.080 | 30242 | 47.70 | 75.97 | 26330 | 50072 | 233650 | 15.25 | 2088 | 2.04 | 0 | 14.00 | 350.7 | 1.49 |
| 1M | 1 | class-fresh | 128.0 | 111.6 | 71.61 | 10393 | 6434 | 66.77 | 8317 | 25024 | 50035 | 37.77 | 1039 | 1.01 | 0 | 0 | 0.000 | 3.70 |
| 1M | 8 | today | 128.0 | 225457 | 0.080 | 125386 | 52.99 | 81.42 | 74731 | 442002 | 726081 | 19.62 | 5141 | 5.02 | 0 | 96.00 | 467.4 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 133874 | 0.090 | 66072 | 49.64 | 100.9 | 63328 | 266863 | 300429 | 34.09 | 2065 | 2.02 | 0 | 95.00 | 317.4 | 1.74 |
| 1M | 8 | class-fresh | 128.0 | 332.8 | 181.8 | 66315 | 61937 | 136.8 | 16790 | 150023 | 166804 | 54.60 | 1034 | 1.01 | 0 | 0 | 0.000 | 2.78 |
| 4M | 1 | today | 32.00 | 71.45 | 0.080 | 51717 | 50.37 | 85.73 | 53372 | 108382 | 250209 | 9.01 | 8218 | 2.01 | 0 | 2.00 | 533.5 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 79.67 | 0.080 | 33836 | 48.41 | 77.51 | 9705 | 50001 | 333491 | 11.92 | 8258 | 2.02 | 0 | 8.00 | 425.7 | 1.32 |
| 4M | 1 | class-fresh | 32.00 | 116.4 | 75.84 | 30063 | 14345 | 75.60 | 8305 | 50014 | 74990 | 18.46 | 4108 | 1.00 | 0 | 0 | 0.000 | 2.05 |
| 4M | 8 | today | 32.00 | 267679 | 0.070 | 183088 | 53.74 | 70.72 | 93369 | 558695 | 617023 | 16.07 | 8210 | 2.00 | 0 | 22.00 | 442.4 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 292811 | 0.051 | 210573 | 53.22 | 69.37 | 125665 | 591929 | 700723 | 14.32 | 8209 | 2.00 | 0 | 22.00 | 484.0 | 0.891 |
| 4M | 8 | class-fresh | 32.00 | 418.4 | 326.2 | 111561 | 89528 | 161.5 | 16775 | 240924 | 433145 | 32.27 | 4105 | 1.00 | 0 | 0 | 0.000 | 2.01 |

