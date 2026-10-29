### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.63 µs |
| fdatasync of a clean file | p50 24.09 µs, p99 45.68 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8245 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8257 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.49 µs, p99 33.90 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 3 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wt | 0 | 76.80 | no | 369.8 | 494.6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | idle-wt | 0 | 0 | no | 0 | 0 | 21.48 | 43.06 | 46.06 | 13.44 | 41.28 | 1.05 | 3.92 | 0.534 | 0 |
| 32 | arrival-qd1-wt | 38.40 | 25.60 | **yes** | 1125 | 1415 | 42.74 | 186.6 | 297.3 | 34.69 | 118.0 | 1.03 | 1.26 | 0.824 | 0 |
| 32 | offset-qd1-wt | 38.40 | 36.80 | no | 561.3 | 1043 | 45.63 | 445.6 | 526.5 | 46.02 | 359.8 | 1.04 | 3.85 | 0.825 | 0 |
| 32 | offset-ino-wt | 38.40 | 38.40 | no | 617.4 | 803.3 | 50.39 | 454.8 | 485.7 | 48.90 | 390.1 | 1.05 | 1.21 | 0.827 | 0 |
| 32 | kernel-wt | 38.40 | 38.40 | no | 404.5 | 582.5 | 40.21 | 349.1 | 379.3 | 37.35 | 300.0 | 1.04 | 3.90 | 0.764 | 0 |
| 128 | capacity-wt | 0 | 76.80 | no | 1315 | 1420 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | idle-wt | 0 | 0 | no | 0 | 0 | 23.03 | 124.6 | 265.8 | 14.38 | 99.37 | 1.02 | 3.89 | 0.566 | 0 |
| 128 | arrival-qd1-wt | 38.40 | 19.20 | **yes** | 4719 | 4866 | 41.06 | 166.9 | 247.0 | 37.38 | 140.4 | 1.03 | 3.88 | 0.833 | 0 |
| 128 | offset-qd1-wt | 38.40 | 38.40 | no | 2354 | 2433 | 44.55 | 775.9 | 1119 | 41.72 | 678.5 | 1.05 | 1.26 | 0.795 | 0 |
| 128 | offset-ino-wt | 38.40 | 38.40 | no | 2247 | 2442 | 47.87 | 719.2 | 1032 | 42.99 | 672.1 | 1.04 | 3.90 | 0.809 | 0 |
| 128 | kernel-wt | 38.40 | 38.40 | no | 1603 | 1665 | 39.74 | 1047 | 1238 | 38.86 | 1230 | 1.06 | 1063 | 0.776 | 0 |

### X7 · A foreground under a deep scrub's budget, round 3 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 53.16 | 310.0 | 393.5 | 24.69 | 120.0 | 0.667 | 0 |
| 10 | 10.01 | 57.60 | 122.0 | 165.2 | 24.18 | 110.6 | 0.667 | 0 |
| 20 | 20.02 | 63.23 | 140.3 | 195.1 | 29.26 | 113.1 | 0.712 | 0 |
| 40 | 40.04 | 97.22 | 303.5 | 327.8 | 42.81 | 205.3 | 0.760 | 0 |
| 60 | 59.86 | 160.6 | 531.9 | 543.0 | 60.16 | 420.4 | 0.818 | 0 |
| unbounded | 70.87 | 229.0 | 710.7 | 755.3 | 100.6 | 461.9 | 0.853 | 0 |

