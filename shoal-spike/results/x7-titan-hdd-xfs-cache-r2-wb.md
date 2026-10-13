### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 8063 µs |
| fdatasync of a clean file | p50 494.9 µs, p99 522.6 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 8230 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 8248 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.19 µs, p99 33.29 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 2 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wb | 0 | 102.4 | no | 312.5 | 358.7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | kernel-wb | 51.20 | 36.80 | **yes** | 800.6 | 1160 | 147.7 | 378.9 | 432.8 | 461.2 | 906.3 | 1.07 | 281.0 | 0.415 | 7.60 |
| 32 | offset-ino-wb | 51.20 | 8.00 | **yes** | 3452 | 3520 | 52.17 | 145.4 | 295.7 | 252.7 | 500.4 | 1.02 | 51.23 | 0.299 | 10.30 |
| 32 | offset-qd1-wb | 51.20 | 8.00 | **yes** | 3419 | 3710 | 52.04 | 127.2 | 170.9 | 252.7 | 374.8 | 1.02 | 64.49 | 0.193 | 10.55 |
| 32 | arrival-qd1-wb | 51.20 | 8.00 | **yes** | 3511 | 3586 | 52.52 | 130.3 | 205.3 | 253.9 | 402.5 | 1.02 | 53.56 | 0.233 | 10.50 |
| 32 | idle-wb | 0 | 0 | no | 0 | 0 | 58.70 | 86.34 | 272.1 | 158.2 | 264.7 | 1.04 | 3.95 | 0.139 | 10.05 |
| 128 | capacity-wb | 0 | 128.0 | no | 788.3 | 880.4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | kernel-wb | 64.00 | 57.60 | **yes** | 1286 | 1958 | 104.0 | 889.5 | 1048 | 424.4 | 1528 | 1.07 | 1161 | 0.461 | 11.05 |
| 128 | offset-ino-wb | 64.00 | 0 | **yes** | 0 | 0 | 51.57 | 122.6 | 212.8 | 211.8 | 411.0 | 1.02 | 1.81 | 0.195 | 10.40 |
| 128 | offset-qd1-wb | 64.00 | 0 | **yes** | 0 | 0 | 56.39 | 120.0 | 131.3 | 208.3 | 316.0 | 1.01 | 1.36 | 0.194 | 10.55 |
| 128 | arrival-qd1-wb | 64.00 | 0 | **yes** | 0 | 0 | 61.60 | 144.2 | 161.9 | 166.6 | 307.9 | 1.03 | 3.86 | 0.230 | 10.00 |
| 128 | idle-wb | 0 | 0 | no | 0 | 0 | 50.84 | 123.4 | 217.9 | 250.2 | 345.4 | 1.03 | 3.90 | 0.394 | 10.05 |

### X7 · A foreground under a deep scrub's budget, round 2 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 432.4 | 593.7 | 686.9 | 111.0 | 228.9 | 0.360 | 11.65 |
| 10 | 10.01 | 480.6 | 643.1 | 684.8 | 137.9 | 290.5 | 0.416 | 10.45 |
| 20 | 18.22 | 555.0 | 791.6 | 847.3 | 146.5 | 327.8 | 0.443 | 9.10 |
| 40 | 17.22 | 578.8 | 748.7 | 799.5 | 170.1 | 331.5 | 0.470 | 8.65 |
| 60 | 16.42 | 610.4 | 783.2 | 836.6 | 179.5 | 344.7 | 0.480 | 8.35 |
| unbounded | 16.22 | 617.0 | 876.3 | 888.9 | 178.1 | 361.4 | 0.454 | 8.10 |

