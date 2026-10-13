### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 8065 µs |
| fdatasync of a clean file | p50 490.4 µs, p99 519.9 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 8233 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 8250 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.04 µs, p99 32.92 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 4 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wb | 0 | 96.00 | no | 299.0 | 352.7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | kernel-wb | 48.00 | 35.20 | **yes** | 808.9 | 1292 | 134.5 | 346.3 | 414.9 | 429.4 | 705.5 | 1.09 | 238.0 | 0.380 | 7.65 |
| 32 | offset-ino-wb | 48.00 | 8.00 | **yes** | 3322 | 3469 | 64.69 | 180.5 | 234.1 | 198.6 | 534.9 | 1.05 | 92.78 | 0.361 | 10.65 |
| 32 | offset-qd1-wb | 48.00 | 8.00 | **yes** | 3402 | 3453 | 53.66 | 112.0 | 135.1 | 248.3 | 359.6 | 1.05 | 98.93 | 0.345 | 10.80 |
| 32 | arrival-qd1-wb | 48.00 | 8.00 | **yes** | 3402 | 3544 | 58.37 | 126.1 | 137.9 | 232.9 | 353.2 | 1.03 | 67.23 | 0.223 | 10.45 |
| 32 | idle-wb | 0 | 0 | no | 0 | 0 | 52.96 | 99.58 | 114.8 | 248.9 | 294.5 | 1.05 | 3.23 | 0.408 | 10.00 |
| 128 | capacity-wb | 0 | 128.0 | no | 774.3 | 817.3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | kernel-wb | 64.00 | 57.60 | **yes** | 1334 | 1441 | 88.19 | 723.9 | 827.0 | 390.3 | 1120 | 1.07 | 455.1 | 0.438 | 11.00 |
| 128 | offset-ino-wb | 64.00 | 0 | **yes** | 0 | 0 | 51.92 | 133.5 | 233.9 | 243.8 | 507.0 | 1.00 | 3.88 | 0.362 | 10.45 |
| 128 | offset-qd1-wb | 64.00 | 0 | **yes** | 0 | 0 | 51.57 | 111.1 | 125.6 | 208.5 | 315.1 | 1.03 | 4.15 | 0.142 | 10.60 |
| 128 | arrival-qd1-wb | 64.00 | 0 | **yes** | 0 | 0 | 57.57 | 137.4 | 162.1 | 222.4 | 331.0 | 1.05 | 3.90 | 0.214 | 10.20 |
| 128 | idle-wb | 0 | 0 | no | 0 | 0 | 72.43 | 93.89 | 158.3 | 171.8 | 197.3 | 1.04 | 3.39 | 0.276 | 10.15 |

### X7 · A foreground under a deep scrub's budget, round 4 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 411.9 | 560.5 | 613.0 | 110.5 | 219.3 | 0.397 | 12.20 |
| 10 | 9.81 | 497.7 | 672.9 | 831.0 | 127.2 | 278.0 | 0.404 | 10.20 |
| 20 | 18.22 | 565.8 | 743.9 | 753.8 | 157.4 | 317.4 | 0.484 | 9.30 |
| 40 | 17.82 | 550.2 | 767.5 | 797.7 | 152.7 | 315.6 | 0.448 | 9.00 |
| 60 | 17.22 | 570.7 | 770.3 | 784.7 | 161.6 | 320.4 | 0.454 | 8.60 |
| unbounded | 15.82 | 624.3 | 969.8 | 1006 | 176.8 | 392.2 | 0.469 | 8.05 |

