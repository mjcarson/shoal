### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.72 µs |
| fdatasync of a clean file | p50 23.91 µs, p99 36.80 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8247 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8263 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.21 µs, p99 33.24 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 3 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wt | 0 | 83.20 | no | 364.4 | 469.4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | idle-wt | 0 | 0 | no | 0 | 0 | 23.36 | 42.27 | 44.66 | 13.09 | 42.48 | 1.05 | 1.24 | 0.553 | 0 |
| 32 | arrival-qd1-wt | 41.60 | 27.20 | **yes** | 1097 | 1258 | 40.66 | 207.4 | 288.4 | 35.32 | 151.8 | 1.06 | 1.31 | 0.841 | 0 |
| 32 | offset-qd1-wt | 41.60 | 41.60 | no | 556.7 | 1013 | 62.11 | 418.9 | 495.8 | 54.23 | 346.5 | 1.01 | 3.89 | 0.846 | 0 |
| 32 | offset-ino-wt | 41.60 | 41.60 | no | 574.1 | 663.6 | 61.70 | 438.5 | 495.9 | 54.67 | 338.3 | 1.04 | 3.93 | 0.839 | 0 |
| 32 | kernel-wt | 41.60 | 41.60 | no | 382.7 | 487.5 | 35.20 | 333.8 | 386.3 | 38.51 | 288.9 | 1.05 | 1.41 | 0.756 | 0 |
| 128 | capacity-wt | 0 | 102.4 | no | 1180 | 1196 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | idle-wt | 0 | 0 | no | 0 | 0 | 23.59 | 56.93 | 176.3 | 14.32 | 45.88 | 1.05 | 1.99 | 0.573 | 0 |
| 128 | arrival-qd1-wt | 51.20 | 25.60 | **yes** | 4708 | 4727 | 39.29 | 211.8 | 288.2 | 33.80 | 116.6 | 1.03 | 3.89 | 0.850 | 0 |
| 128 | offset-qd1-wt | 51.20 | 44.80 | **yes** | 2283 | 2493 | 83.33 | 833.8 | 932.0 | 63.79 | 727.8 | 1.06 | 1.27 | 0.884 | 0 |
| 128 | offset-ino-wt | 51.20 | 44.80 | **yes** | 2256 | 2666 | 84.76 | 828.5 | 1443 | 80.18 | 620.5 | 1.07 | 1.37 | 0.869 | 0 |
| 128 | kernel-wt | 51.20 | 44.80 | **yes** | 1432 | 1492 | 63.22 | 780.3 | 881.4 | 106.3 | 1083 | 1.13 | 932.7 | 0.821 | 0 |

### X7 · A foreground under a deep scrub's budget, round 3 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 50.00 | 125.6 | 243.1 | 24.44 | 113.6 | 0.643 | 0 |
| 10 | 10.01 | 55.11 | 128.4 | 146.9 | 26.23 | 92.52 | 0.671 | 0 |
| 20 | 20.02 | 61.51 | 142.0 | 167.2 | 30.05 | 113.9 | 0.705 | 0 |
| 40 | 40.04 | 98.05 | 250.6 | 279.4 | 39.65 | 205.7 | 0.767 | 0 |
| 60 | 59.86 | 142.0 | 420.4 | 445.2 | 61.60 | 346.1 | 0.812 | 0 |
| unbounded | 72.07 | 216.5 | 624.8 | 672.6 | 83.66 | 543.8 | 0.859 | 0 |

