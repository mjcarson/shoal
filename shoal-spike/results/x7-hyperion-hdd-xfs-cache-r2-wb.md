### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 103 · fs device 0 | 16.44 KiB a sync · sync p50 8067 µs |
| fdatasync of a clean file | p50 524.6 µs, p99 555.4 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 8233 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 8248 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.15 µs, p99 33.07 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 2 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wb | 0 | 102.4 | no | 299.0 | 330.0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | kernel-wb | 51.20 | 40.00 | **yes** | 750.3 | 1034 | 132.1 | 359.2 | 461.4 | 419.1 | 750.0 | 1.10 | 226.1 | 0.387 | 8.05 |
| 32 | offset-ino-wb | 51.20 | 6.40 | **yes** | 3452 | 3452 | 51.70 | 105.2 | 121.1 | 251.4 | 310.2 | 1.05 | 50.82 | 0.360 | 10.45 |
| 32 | offset-qd1-wb | 51.20 | 8.00 | **yes** | 3385 | 3418 | 54.37 | 100.4 | 173.2 | 233.3 | 329.6 | 1.05 | 78.70 | 0.265 | 10.80 |
| 32 | arrival-qd1-wb | 51.20 | 6.40 | **yes** | 3494 | 3502 | 54.21 | 126.0 | 149.7 | 264.6 | 354.9 | 1.04 | 67.30 | 0.293 | 10.55 |
| 32 | idle-wb | 0 | 0 | no | 0 | 0 | 52.80 | 82.36 | 208.4 | 152.3 | 255.8 | 1.03 | 1.23 | 0.075 | 10.05 |
| 128 | capacity-wb | 0 | 153.6 | no | 730.3 | 818.0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | kernel-wb | 76.80 | 76.80 | no | 1234 | 1914 | 123.8 | 752.3 | 821.6 | 514.7 | 1177 | 1.07 | 700.4 | 0.346 | 10.95 |
| 128 | offset-ino-wb | 76.80 | 0 | **yes** | 0 | 0 | 52.07 | 83.09 | 116.2 | 214.1 | 281.6 | 1.05 | 3.91 | 0.222 | 10.50 |
| 128 | offset-qd1-wb | 76.80 | 0 | **yes** | 0 | 0 | 51.53 | 100.6 | 119.4 | 227.7 | 364.9 | 0.999 | 4.18 | 0.344 | 10.65 |
| 128 | arrival-qd1-wb | 76.80 | 0 | **yes** | 0 | 0 | 52.13 | 124.6 | 148.0 | 252.8 | 359.4 | 1.04 | 3.93 | 0.283 | 10.45 |
| 128 | idle-wb | 0 | 0 | no | 0 | 0 | 64.95 | 114.8 | 235.8 | 252.6 | 373.4 | 1.08 | 1.26 | 0.417 | 10.15 |

### X7 · A foreground under a deep scrub's budget, round 2 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 433.9 | 566.1 | 594.8 | 114.1 | 238.6 | 0.444 | 12.00 |
| 10 | 10.01 | 482.4 | 632.1 | 689.8 | 131.0 | 265.4 | 0.390 | 10.10 |
| 20 | 18.82 | 547.7 | 685.7 | 722.0 | 139.0 | 285.6 | 0.428 | 9.40 |
| 40 | 18.62 | 552.8 | 693.7 | 702.1 | 158.5 | 296.2 | 0.502 | 9.35 |
| 60 | 17.22 | 560.7 | 754.5 | 764.2 | 163.5 | 318.1 | 0.485 | 8.75 |
| unbounded | 16.02 | 607.0 | 773.8 | 823.0 | 173.2 | 378.0 | 0.486 | 8.30 |

