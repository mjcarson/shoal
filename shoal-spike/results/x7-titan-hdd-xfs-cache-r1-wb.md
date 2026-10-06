### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 8066 µs |
| fdatasync of a clean file | p50 492.7 µs, p99 524.5 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 8236 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 8244 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.24 µs, p99 33.62 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 1 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wb | 0 | 96.00 | no | 302.3 | 345.7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | idle-wb | 0 | 0 | no | 0 | 0 | 99.24 | 255.0 | 337.1 | 222.1 | 592.9 | 1.03 | 1.61 | 0.373 | 6.75 |
| 32 | arrival-qd1-wb | 48.00 | 8.00 | **yes** | 3403 | 3685 | 51.88 | 117.9 | 196.7 | 251.8 | 428.1 | 1.02 | 46.64 | 0.186 | 10.70 |
| 32 | offset-qd1-wb | 48.00 | 6.40 | **yes** | 3402 | 3444 | 52.49 | 92.85 | 102.2 | 251.8 | 293.4 | 1.03 | 81.11 | 0.255 | 10.80 |
| 32 | offset-ino-wb | 48.00 | 8.00 | **yes** | 3403 | 3444 | 51.89 | 91.39 | 139.7 | 226.7 | 290.2 | 1.05 | 79.80 | 0.251 | 10.90 |
| 32 | kernel-wb | 48.00 | 46.40 | no | 550.9 | 702.3 | 73.07 | 250.7 | 303.4 | 296.2 | 526.7 | 1.06 | 179.2 | 0.250 | 11.10 |
| 128 | capacity-wb | 0 | 153.6 | no | 741.5 | 839.2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | idle-wb | 0 | 0 | no | 0 | 0 | 54.45 | 107.8 | 183.9 | 156.9 | 233.1 | 1.02 | 1.22 | 0.357 | 10.15 |
| 128 | arrival-qd1-wb | 76.80 | 0 | **yes** | 0 | 0 | 53.91 | 122.6 | 213.9 | 249.3 | 377.6 | 1.01 | 4.14 | 0.227 | 10.45 |
| 128 | offset-qd1-wb | 76.80 | 0 | **yes** | 0 | 0 | 66.30 | 101.8 | 145.8 | 225.6 | 334.9 | 1.03 | 3.93 | 0.347 | 10.55 |
| 128 | offset-ino-wb | 76.80 | 0 | **yes** | 0 | 0 | 51.69 | 110.8 | 118.1 | 245.7 | 315.6 | 1.03 | 1.41 | 0.400 | 10.60 |
| 128 | kernel-wb | 76.80 | 76.80 | no | 1241 | 1314 | 99.63 | 756.9 | 849.1 | 475.8 | 1086 | 1.08 | 472.2 | 0.425 | 11.35 |

### X7 · A foreground under a deep scrub's budget, round 1 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 474.2 | 741.0 | 825.1 | 132.3 | 259.2 | 0.377 | 10.10 |
| 10 | 10.01 | 538.5 | 704.1 | 711.7 | 160.6 | 305.5 | 0.472 | 9.40 |
| 20 | 17.82 | 580.3 | 758.1 | 832.1 | 167.0 | 330.0 | 0.491 | 8.85 |
| 40 | 17.82 | 558.7 | 714.5 | 771.7 | 151.3 | 311.3 | 0.425 | 8.90 |
| 60 | 19.02 | 528.7 | 677.8 | 728.8 | 142.5 | 284.8 | 0.460 | 9.50 |
| unbounded | 18.62 | 526.4 | 724.6 | 740.7 | 150.2 | 316.9 | 0.472 | 9.40 |

