### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 29.58 µs |
| fdatasync of a clean file | p50 24.24 µs, p99 47.37 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8231 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8250 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.08 µs, p99 33.19 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 4 (latencies µs p50; directory sync Fdatasync)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 232.0 | 74.45 | 0.090 | 18258 | 46.98 | 76.24 | 30685 | 58405 | 216745 | 11.53 | 4196 | 65.56 | 0 | 57.00 | 442.5 | 1.00 |
| 64K | 1 | class-pool | 318.0 | 72.34 | 0.081 | 24618 | 46.57 | 76.63 | 31180 | 58366 | 141838 | 15.89 | 155.0 | 2.42 | 0 | 40.00 | 308.6 | 1.38 |
| 64K | 1 | class-fresh | 600.0 | 112.1 | 80.47 | 10649 | 10257 | 71.41 | 8312 | 33339 | 50043 | 32.39 | 82.46 | 1.29 | 0 | 0 | 0.000 | 2.81 |
| 64K | 8 | today | 440.0 | 241901 | 0.090 | 32238 | 51.50 | 71.22 | 99223 | 392199 | 608963 | 21.79 | 4184 | 65.37 | 0 | 349.0 | 618.0 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 66925 | 0.080 | 22847 | 34.08 | 67.68 | 37074 | 125342 | 358626 | 64.02 | 150.6 | 2.35 | 0 | 475.0 | 250.4 | 2.94 |
| 64K | 8 | class-fresh | 600.0 | 292.4 | 196.0 | 22723 | 22307 | 127.8 | 24877 | 66868 | 99938 | 114.6 | 75.49 | 1.18 | 0 | 0 | 0.000 | 5.26 |
| 256K | 1 | today | 197.0 | 76.08 | 0.080 | 25004 | 47.61 | 75.71 | 33387 | 58381 | 266885 | 9.83 | 4381 | 17.11 | 0 | 56.00 | 626.3 | 1.00 |
| 256K | 1 | class-pool | 348.0 | 81.76 | 0.090 | 22271 | 36.93 | 77.25 | 29661 | 50086 | 141840 | 17.38 | 542.0 | 2.12 | 0 | 33.00 | 283.8 | 1.77 |
| 256K | 1 | class-fresh | 512.0 | 112.0 | 80.53 | 11857 | 10366 | 71.47 | 8312 | 33355 | 50050 | 30.87 | 276.2 | 1.08 | 0 | 0 | 0.000 | 3.14 |
| 256K | 8 | today | 357.0 | 267165 | 0.090 | 50872 | 52.46 | 81.50 | 100597 | 483678 | 834214 | 17.72 | 4382 | 17.12 | 0 | 283.0 | 692.5 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 75108 | 0.091 | 27834 | 34.59 | 68.93 | 40900 | 142023 | 217285 | 59.53 | 531.9 | 2.08 | 0 | 399.0 | 292.8 | 3.36 |
| 256K | 8 | class-fresh | 512.0 | 276.5 | 193.9 | 16049 | 15054 | 118.6 | 24920 | 58344 | 383345 | 118.6 | 270.7 | 1.06 | 0 | 0 | 0.000 | 6.69 |
| 1M | 1 | today | 128.0 | 75.64 | 0.081 | 23488 | 49.21 | 78.80 | 38357 | 75082 | 241900 | 10.61 | 5156 | 5.03 | 0 | 26.00 | 551.1 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 71.59 | 0.080 | 35350 | 46.83 | 76.54 | 27847 | 66688 | 166777 | 13.94 | 2073 | 2.02 | 0 | 14.00 | 259.1 | 1.31 |
| 1M | 1 | class-fresh | 128.0 | 115.3 | 73.15 | 21558 | 17310 | 74.55 | 8306 | 50012 | 66699 | 20.82 | 1039 | 1.01 | 0 | 0 | 0.000 | 1.96 |
| 1M | 8 | today | 128.0 | 275234 | 0.090 | 73353 | 52.50 | 70.26 | 85922 | 450474 | 751374 | 18.94 | 5142 | 5.02 | 0 | 101.0 | 509.4 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 125410 | 0.090 | 63301 | 46.89 | 101.5 | 60147 | 250111 | 391504 | 34.24 | 2066 | 2.02 | 0 | 97.00 | 258.6 | 1.81 |
| 1M | 8 | class-fresh | 128.0 | 346.4 | 181.8 | 33226 | 39186 | 124.7 | 16779 | 91776 | 108511 | 88.13 | 1034 | 1.01 | 0 | 0 | 0.000 | 4.65 |
| 4M | 1 | today | 32.00 | 76.67 | 0.090 | 44513 | 49.88 | 84.92 | 35377 | 99948 | 241766 | 9.50 | 8214 | 2.01 | 0 | 4.00 | 459.0 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 74.81 | 0.090 | 35811 | 48.63 | 78.06 | 43460 | 91749 | 225157 | 10.37 | 8221 | 2.01 | 0 | 3.00 | 492.1 | 1.09 |
| 4M | 1 | class-fresh | 32.00 | 113.0 | 73.04 | 29634 | 11014 | 74.22 | 8309 | 50006 | 66672 | 20.00 | 4108 | 1.00 | 0 | 0 | 0.000 | 2.10 |
| 4M | 8 | today | 32.00 | 233995 | 0.090 | 155913 | 52.23 | 73.46 | 71124 | 433641 | 742156 | 17.85 | 8210 | 2.00 | 0 | 20.00 | 383.5 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 324883 | 0.051 | 192291 | 51.86 | 73.38 | 95078 | 658363 | 675524 | 15.06 | 8211 | 2.00 | 0 | 20.00 | 500.4 | 0.843 |
| 4M | 8 | class-fresh | 32.00 | 363.3 | 172.6 | 197533 | 153243 | 151.2 | 24876 | 449982 | 492101 | 20.21 | 4142 | 1.01 | 0 | 0 | 0.000 | 1.13 |

