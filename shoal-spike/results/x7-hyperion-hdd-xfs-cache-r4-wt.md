### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.47 µs |
| fdatasync of a clean file | p50 24.04 µs, p99 46.39 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8246 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8262 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.12 µs, p99 32.92 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 4 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wt | 0 | 89.60 | no | 317.5 | 356.7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | kernel-wt | 44.80 | 43.20 | no | 356.9 | 445.1 | 37.50 | 298.7 | 361.5 | 45.20 | 331.8 | 1.01 | 3.90 | 0.785 | 0 |
| 32 | offset-ino-wt | 44.80 | 43.20 | no | 566.9 | 898.5 | 63.64 | 455.4 | 591.9 | 117.8 | 502.6 | 1.04 | 1.35 | 0.844 | 0 |
| 32 | offset-qd1-wt | 44.80 | 43.20 | no | 543.4 | 821.2 | 58.69 | 446.0 | 536.1 | 101.9 | 470.2 | 1.08 | 1.47 | 0.840 | 0 |
| 32 | arrival-qd1-wt | 44.80 | 27.20 | **yes** | 1132 | 1390 | 36.35 | 190.3 | 278.5 | 51.74 | 219.5 | 1.03 | 3.87 | 0.821 | 0 |
| 32 | idle-wt | 0 | 0 | no | 0 | 0 | 23.47 | 72.31 | 137.6 | 13.33 | 80.78 | 1.02 | 1.34 | 0.615 | 0 |
| 128 | capacity-wt | 0 | 102.4 | no | 1182 | 1189 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | kernel-wt | 51.20 | 44.80 | **yes** | 1373 | 1481 | 47.01 | 1003 | 1222 | 96.60 | 1283 | 1.08 | 978.3 | 0.813 | 0 |
| 128 | offset-ino-wt | 51.20 | 44.80 | **yes** | 2068 | 2413 | 81.99 | 745.7 | 903.3 | 142.2 | 984.1 | 1.05 | 1.38 | 0.879 | 0 |
| 128 | offset-qd1-wt | 51.20 | 44.80 | **yes** | 2288 | 2550 | 84.61 | 738.6 | 1022 | 126.1 | 804.0 | 1.06 | 1.21 | 0.874 | 0 |
| 128 | arrival-qd1-wt | 51.20 | 19.20 | **yes** | 4656 | 4956 | 41.63 | 185.3 | 194.3 | 53.19 | 211.7 | 1.04 | 1.24 | 0.821 | 0 |
| 128 | idle-wt | 0 | 0 | no | 0 | 0 | 22.56 | 46.53 | 51.25 | 12.25 | 49.32 | 1.02 | 1.24 | 0.595 | 0 |

### X7 · A foreground under a deep scrub's budget, round 4 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 47.67 | 74.75 | 93.37 | 14.11 | 58.04 | 0.550 | 0 |
| 10 | 10.01 | 51.22 | 108.2 | 131.9 | 13.97 | 65.45 | 0.599 | 0 |
| 20 | 20.02 | 57.17 | 141.5 | 166.4 | 16.56 | 82.08 | 0.651 | 0 |
| 40 | 40.04 | 86.89 | 202.7 | 303.7 | 21.86 | 122.2 | 0.707 | 0 |
| 60 | 59.86 | 147.3 | 423.3 | 624.5 | 52.66 | 298.8 | 0.799 | 0 |
| unbounded | 72.07 | 287.6 | 865.9 | 1040 | 87.46 | 476.3 | 0.862 | 0 |

