### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.54 µs |
| fdatasync of a clean file | p50 23.73 µs, p99 36.36 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 6 KiB and 0 flushes a rename | p50 8244 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8262 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.02 µs, p99 32.80 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

### X15. A short chunk's file, round 4 (latencies µs p50; directory sync Fdatasync)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| size | writers | side | n | take | alloc | write | durable | rename | dir syncs | cycle p50 | cycle p99/max | chunks/s | dev KiB/chunk | dev B/B | flushes/chunk | held | refill ms | ÷today |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64K | 1 | today | 305.0 | 75.16 | 0.080 | 11849 | 45.50 | 66.99 | 11285 | 41667 | 191771 | 15.23 | 4196 | 65.57 | 0 | 106.0 | 450.5 | 1.00 |
| 64K | 1 | class-pool | 313.0 | 71.80 | 0.080 | 24453 | 44.17 | 69.73 | 31710 | 58372 | 158508 | 15.63 | 158.3 | 2.47 | 0 | 33.00 | 309.6 | 1.03 |
| 64K | 1 | class-fresh | 600.0 | 108.5 | 78.74 | 10240 | 10179 | 65.17 | 8319 | 25062 | 50012 | 34.04 | 80.91 | 1.26 | 0 | 0 | 0.000 | 2.23 |
| 64K | 8 | today | 365.0 | 292033 | 0.080 | 33945 | 50.86 | 70.06 | 101596 | 460964 | 842343 | 18.11 | 4190 | 65.46 | 0 | 295.0 | 559.4 | 1.00 |
| 64K | 8 | class-pool | 600.0 | 58495 | 0.080 | 15822 | 33.23 | 67.48 | 33334 | 108549 | 175350 | 79.86 | 147.9 | 2.31 | 0 | 473.0 | 133.7 | 4.41 |
| 64K | 8 | class-fresh | 600.0 | 273.1 | 183.7 | 32223 | 34954 | 125.6 | 24895 | 91781 | 108874 | 88.20 | 75.45 | 1.18 | 0 | 0 | 0.000 | 4.87 |
| 256K | 1 | today | 164.0 | 63.30 | 0.080 | 30516 | 46.16 | 70.98 | 39942 | 83377 | 333614 | 8.20 | 4388 | 17.14 | 0 | 36.00 | 751.1 | 1.00 |
| 256K | 1 | class-pool | 389.0 | 65.33 | 0.080 | 30514 | 45.23 | 71.41 | 19259 | 50058 | 83379 | 19.43 | 541.9 | 2.12 | 0 | 2.00 | 192.1 | 2.37 |
| 256K | 1 | class-fresh | 512.0 | 108.5 | 78.32 | 10957 | 10145 | 65.72 | 8319 | 33351 | 33703 | 33.38 | 273.0 | 1.07 | 0 | 0 | 0.000 | 4.07 |
| 256K | 8 | today | 496.0 | 192502 | 0.080 | 43959 | 52.29 | 72.23 | 91291 | 350331 | 600464 | 24.53 | 4379 | 17.11 | 0 | 390.0 | 609.5 | 1.00 |
| 256K | 8 | class-pool | 512.0 | 66703 | 0.080 | 22821 | 34.07 | 66.50 | 34361 | 124962 | 175749 | 69.64 | 532.0 | 2.08 | 0 | 401.0 | 167.6 | 2.84 |
| 256K | 8 | class-fresh | 512.0 | 276.7 | 186.7 | 24458 | 21473 | 124.3 | 24903 | 66965 | 100132 | 112.9 | 267.4 | 1.04 | 0 | 0 | 0.000 | 4.60 |
| 1M | 1 | today | 128.0 | 74.56 | 0.080 | 19696 | 47.33 | 70.96 | 13466 | 41716 | 208614 | 14.20 | 5158 | 5.04 | 0 | 39.00 | 442.7 | 1.00 |
| 1M | 1 | class-pool | 128.0 | 70.52 | 0.080 | 34112 | 45.08 | 68.93 | 26912 | 58391 | 133488 | 15.80 | 2073 | 2.02 | 0 | 12.00 | 275.6 | 1.11 |
| 1M | 1 | class-fresh | 128.0 | 110.3 | 68.60 | 19857 | 15248 | 67.13 | 8318 | 41706 | 58379 | 21.86 | 1039 | 1.01 | 0 | 0 | 0.000 | 1.54 |
| 1M | 8 | today | 128.0 | 300388 | 0.080 | 113887 | 53.04 | 79.20 | 92382 | 558787 | 1276608 | 14.12 | 5143 | 5.02 | 0 | 99.00 | 709.5 | 1.00 |
| 1M | 8 | class-pool | 128.0 | 142206 | 0.080 | 63501 | 44.92 | 80.90 | 81356 | 275324 | 425628 | 30.20 | 2066 | 2.02 | 0 | 97.00 | 325.7 | 2.14 |
| 1M | 8 | class-fresh | 128.0 | 305.8 | 175.5 | 47229 | 39377 | 127.9 | 16783 | 108306 | 116779 | 75.55 | 1034 | 1.01 | 0 | 0 | 0.000 | 5.35 |
| 4M | 1 | today | 32.00 | 61.54 | 0.080 | 46515 | 47.41 | 71.18 | 30824 | 83308 | 241697 | 10.52 | 8214 | 2.01 | 0 | 3.00 | 525.9 | 1.00 |
| 4M | 1 | class-pool | 32.00 | 64.42 | 0.080 | 34405 | 47.30 | 70.62 | 11272 | 66667 | 350110 | 11.39 | 8258 | 2.02 | 0 | 9.00 | 393.0 | 1.08 |
| 4M | 1 | class-fresh | 32.00 | 109.5 | 68.34 | 40512 | 15532 | 67.31 | 8318 | 66664 | 75032 | 15.42 | 4108 | 1.00 | 0 | 0 | 0.000 | 1.47 |
| 4M | 8 | today | 32.00 | 259221 | 0.070 | 176273 | 52.03 | 69.55 | 60310 | 542245 | 600333 | 16.84 | 8210 | 2.00 | 0 | 21.00 | 483.9 | 1.00 |
| 4M | 8 | class-pool | 32.00 | 341799 | 0.080 | 235941 | 51.97 | 65.57 | 117939 | 625439 | 741924 | 13.47 | 8210 | 2.00 | 0 | 21.00 | 583.4 | 0.800 |
| 4M | 8 | class-fresh | 32.00 | 420.3 | 173.6 | 104922 | 85954 | 132.0 | 24860 | 216827 | 383461 | 35.89 | 4105 | 1.00 | 0 | 0 | 0.000 | 2.13 |

