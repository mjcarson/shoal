### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1052 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 8062 µs |
| fdatasync of a clean file | p50 495.8 µs, p99 519.7 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 8233 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 8252 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.29 µs, p99 34.20 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 3 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wb | 0 | 89.60 | no | 325.5 | 382.8 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | idle-wb | 0 | 0 | no | 0 | 0 | 90.51 | 183.4 | 310.1 | 217.6 | 358.9 | 1.02 | 1.22 | 0.409 | 7.70 |
| 32 | arrival-qd1-wb | 44.80 | 8.00 | **yes** | 3402 | 3443 | 52.28 | 119.2 | 124.0 | 236.8 | 316.0 | 1.02 | 67.33 | 0.187 | 10.45 |
| 32 | offset-qd1-wb | 44.80 | 8.00 | **yes** | 3385 | 3452 | 55.43 | 90.83 | 105.2 | 256.6 | 292.1 | 1.05 | 81.45 | 0.277 | 10.55 |
| 32 | offset-ino-wb | 44.80 | 9.60 | **yes** | 2719 | 3410 | 57.70 | 103.9 | 119.6 | 184.7 | 311.5 | 1.06 | 84.90 | 0.313 | 10.85 |
| 32 | kernel-wb | 44.80 | 43.20 | no | 563.0 | 786.3 | 81.89 | 274.0 | 330.5 | 297.8 | 547.1 | 1.06 | 202.3 | 0.288 | 10.90 |
| 128 | capacity-wb | 0 | 128.0 | no | 755.5 | 834.6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | idle-wb | 0 | 0 | no | 0 | 0 | 65.25 | 117.8 | 253.8 | 164.8 | 317.4 | 1.02 | 3.86 | 0.198 | 10.05 |
| 128 | arrival-qd1-wb | 64.00 | 6.40 | **yes** | 12992 | 12992 | 53.26 | 122.5 | 128.4 | 213.1 | 315.3 | 1.04 | 1.38 | 0.181 | 10.70 |
| 128 | offset-qd1-wb | 64.00 | 6.40 | **yes** | 12884 | 12884 | 44.50 | 79.62 | 81.45 | 214.8 | 277.7 | 1.05 | 3.98 | 0.180 | 11.05 |
| 128 | offset-ino-wb | 64.00 | 0 | **yes** | 0 | 0 | 53.99 | 112.7 | 132.6 | 210.7 | 311.4 | 1.03 | 3.95 | 0.189 | 10.45 |
| 128 | kernel-wb | 64.00 | 57.60 | **yes** | 1271 | 1364 | 75.34 | 752.2 | 843.9 | 404.5 | 1153 | 1.06 | 435.7 | 0.285 | 10.65 |

### X7 · A foreground under a deep scrub's budget, round 3 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 460.5 | 620.2 | 676.3 | 123.3 | 242.1 | 0.371 | 10.70 |
| 10 | 10.01 | 560.6 | 860.4 | 960.4 | 148.8 | 317.7 | 0.442 | 9.25 |
| 20 | 18.02 | 570.7 | 754.1 | 800.5 | 162.4 | 325.0 | 0.487 | 9.05 |
| 40 | 18.02 | 552.2 | 713.6 | 717.0 | 146.4 | 322.2 | 0.438 | 9.20 |
| 60 | 19.02 | 509.0 | 690.7 | 730.1 | 141.2 | 300.8 | 0.450 | 9.70 |
| unbounded | 18.62 | 530.7 | 744.1 | 761.1 | 151.6 | 334.6 | 0.488 | 9.50 |

