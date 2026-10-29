### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 29.07 µs |
| fdatasync of a clean file | p50 24.05 µs, p99 36.23 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8244 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8261 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.15 µs, p99 32.79 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 4 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wt | 0 | 89.60 | no | 336.1 | 449.9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | kernel-wt | 44.80 | 43.20 | no | 366.3 | 713.7 | 40.96 | 349.7 | 394.9 | 47.75 | 383.2 | 1.01 | 1.83 | 0.801 | 0 |
| 32 | offset-ino-wt | 44.80 | 43.20 | no | 592.3 | 836.7 | 68.44 | 461.7 | 516.9 | 111.2 | 479.2 | 1.05 | 3.92 | 0.847 | 0 |
| 32 | offset-qd1-wt | 44.80 | 43.20 | no | 608.4 | 917.8 | 67.49 | 428.0 | 567.1 | 123.0 | 521.4 | 1.06 | 3.91 | 0.857 | 0 |
| 32 | arrival-qd1-wt | 44.80 | 25.60 | **yes** | 1200 | 1306 | 40.86 | 182.9 | 396.7 | 54.00 | 234.2 | 1.03 | 3.88 | 0.837 | 0 |
| 32 | idle-wt | 0 | 0 | no | 0 | 0 | 21.74 | 45.43 | 47.69 | 12.56 | 45.96 | 1.04 | 3.83 | 0.583 | 0 |
| 128 | capacity-wt | 0 | 76.80 | no | 1277 | 1521 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | kernel-wt | 38.40 | 38.40 | no | 1342 | 1501 | 32.72 | 988.2 | 1126 | 38.18 | 1189 | 1.06 | 1012 | 0.762 | 0 |
| 128 | offset-ino-wt | 38.40 | 38.40 | no | 2211 | 2514 | 37.43 | 710.4 | 998.6 | 73.55 | 971.6 | 1.05 | 3.87 | 0.829 | 0 |
| 128 | offset-qd1-wt | 38.40 | 38.40 | no | 2164 | 2382 | 42.83 | 819.6 | 906.4 | 73.67 | 736.8 | 1.03 | 1.33 | 0.806 | 0 |
| 128 | arrival-qd1-wt | 38.40 | 19.20 | **yes** | 4826 | 4968 | 40.35 | 177.5 | 471.1 | 54.87 | 221.7 | 1.04 | 3.89 | 0.819 | 0 |
| 128 | idle-wt | 0 | 0 | no | 0 | 0 | 21.47 | 68.26 | 297.2 | 13.58 | 113.8 | 1.04 | 1.96 | 0.595 | 0 |

### X7 · A foreground under a deep scrub's budget, round 4 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 51.33 | 90.45 | 104.6 | 14.82 | 59.14 | 0.550 | 0 |
| 10 | 10.01 | 56.08 | 118.9 | 168.4 | 16.59 | 83.50 | 0.606 | 0 |
| 20 | 20.02 | 62.66 | 145.4 | 164.5 | 21.17 | 94.17 | 0.651 | 0 |
| 40 | 39.84 | 88.36 | 290.0 | 321.7 | 25.82 | 130.2 | 0.726 | 0 |
| 60 | 59.86 | 153.6 | 460.5 | 575.1 | 58.18 | 386.8 | 0.810 | 0 |
| unbounded | 77.08 | 340.3 | 955.9 | 996.6 | 95.65 | 539.1 | 0.877 | 0 |

