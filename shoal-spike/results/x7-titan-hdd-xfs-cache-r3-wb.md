### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 8068 µs |
| fdatasync of a clean file | p50 491.6 µs, p99 530.5 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 8231 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 8252 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.10 µs, p99 33.24 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 3 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wb | 0 | 89.60 | no | 328.8 | 369.8 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | idle-wb | 0 | 0 | no | 0 | 0 | 93.84 | 216.1 | 330.3 | 331.3 | 613.1 | 1.01 | 3.95 | 0.397 | 7.20 |
| 32 | arrival-qd1-wb | 44.80 | 8.00 | **yes** | 3402 | 3494 | 54.28 | 120.2 | 170.2 | 255.0 | 371.3 | 1.04 | 63.98 | 0.206 | 10.65 |
| 32 | offset-qd1-wb | 44.80 | 8.00 | **yes** | 3403 | 3544 | 55.74 | 101.6 | 140.5 | 237.7 | 337.9 | 1.04 | 80.29 | 0.286 | 10.70 |
| 32 | offset-ino-wb | 44.80 | 8.00 | **yes** | 3445 | 3561 | 51.89 | 119.3 | 135.2 | 251.7 | 369.8 | 1.03 | 15.03 | 0.227 | 10.60 |
| 32 | kernel-wb | 44.80 | 43.20 | no | 603.3 | 866.0 | 80.52 | 321.7 | 357.8 | 318.8 | 616.1 | 1.06 | 215.1 | 0.242 | 10.10 |
| 128 | capacity-wb | 0 | 102.4 | no | 865.0 | 929.8 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | idle-wb | 0 | 0 | no | 0 | 0 | 54.02 | 78.28 | 208.8 | 209.9 | 276.9 | 1.03 | 3.99 | 0.109 | 10.15 |
| 128 | arrival-qd1-wb | 51.20 | 0 | **yes** | 0 | 0 | 55.51 | 123.2 | 132.0 | 261.6 | 330.5 | 1.03 | 1.28 | 0.216 | 10.20 |
| 128 | offset-qd1-wb | 51.20 | 0 | **yes** | 0 | 0 | 57.00 | 114.3 | 143.8 | 225.3 | 326.2 | 1.03 | 3.98 | 0.321 | 10.30 |
| 128 | offset-ino-wb | 51.20 | 0 | **yes** | 0 | 0 | 56.67 | 113.0 | 142.3 | 213.1 | 327.7 | 1.02 | 4.03 | 0.169 | 10.25 |
| 128 | kernel-wb | 51.20 | 44.80 | **yes** | 1326 | 1535 | 82.39 | 864.1 | 980.6 | 377.4 | 1158 | 1.06 | 490.1 | 0.387 | 9.92 |

### X7 · A foreground under a deep scrub's budget, round 3 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 459.0 | 694.0 | 805.7 | 123.4 | 254.1 | 0.391 | 10.70 |
| 10 | 9.81 | 548.4 | 822.3 | 918.1 | 160.5 | 314.3 | 0.447 | 9.10 |
| 20 | 17.22 | 584.5 | 806.7 | 884.0 | 161.0 | 332.9 | 0.453 | 8.55 |
| 40 | 18.02 | 549.7 | 752.1 | 779.7 | 154.5 | 298.5 | 0.445 | 9.05 |
| 60 | 18.02 | 538.0 | 749.5 | 811.3 | 149.6 | 299.2 | 0.449 | 9.20 |
| unbounded | 17.82 | 569.2 | 748.7 | 751.2 | 159.2 | 333.2 | 0.484 | 9.10 |

