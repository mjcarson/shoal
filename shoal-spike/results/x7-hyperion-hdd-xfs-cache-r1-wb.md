### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 8066 µs |
| fdatasync of a clean file | p50 494.3 µs, p99 537.1 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 8238 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 8252 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.41 µs, p99 33.09 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 1 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wb | 0 | 96.00 | no | 306.3 | 341.2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | idle-wb | 0 | 0 | no | 0 | 0 | 88.95 | 195.2 | 285.7 | 261.3 | 600.8 | 1.04 | 1.22 | 0.353 | 6.85 |
| 32 | arrival-qd1-wb | 48.00 | 8.00 | **yes** | 3394 | 3494 | 51.86 | 126.7 | 211.1 | 252.3 | 474.3 | 1.04 | 44.70 | 0.244 | 10.55 |
| 32 | offset-qd1-wb | 48.00 | 8.00 | **yes** | 3352 | 3419 | 51.50 | 104.0 | 117.5 | 233.5 | 300.2 | 1.05 | 57.55 | 0.353 | 10.85 |
| 32 | offset-ino-wb | 48.00 | 8.00 | **yes** | 3119 | 3419 | 51.11 | 95.92 | 116.0 | 220.3 | 292.5 | 1.05 | 76.27 | 0.258 | 10.95 |
| 32 | kernel-wb | 48.00 | 46.40 | no | 566.0 | 833.9 | 68.55 | 259.5 | 298.0 | 314.9 | 579.7 | 1.08 | 208.5 | 0.245 | 10.70 |
| 128 | capacity-wb | 0 | 153.6 | no | 749.2 | 847.8 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | idle-wb | 0 | 0 | no | 0 | 0 | 53.02 | 115.6 | 363.3 | 234.9 | 314.9 | 1.00 | 3.86 | 0.345 | 10.15 |
| 128 | arrival-qd1-wb | 76.80 | 0 | **yes** | 0 | 0 | 52.89 | 126.0 | 140.6 | 221.6 | 285.5 | 1.03 | 3.88 | 0.248 | 10.25 |
| 128 | offset-qd1-wb | 76.80 | 0 | **yes** | 0 | 0 | 61.99 | 132.0 | 232.2 | 222.6 | 430.1 | 1.05 | 1.36 | 0.287 | 10.55 |
| 128 | offset-ino-wb | 76.80 | 0 | **yes** | 0 | 0 | 54.79 | 98.23 | 146.4 | 216.4 | 333.8 | 1.03 | 1.24 | 0.236 | 10.60 |
| 128 | kernel-wb | 76.80 | 70.40 | **yes** | 1305 | 1392 | 135.2 | 753.2 | 804.2 | 535.1 | 1204 | 1.09 | 517.2 | 0.413 | 11.30 |

### X7 · A foreground under a deep scrub's budget, round 1 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 470.7 | 586.2 | 591.7 | 127.6 | 251.3 | 0.399 | 10.75 |
| 10 | 9.81 | 582.8 | 760.9 | 802.2 | 169.0 | 337.3 | 0.453 | 8.95 |
| 20 | 16.22 | 611.6 | 769.2 | 782.6 | 172.5 | 350.9 | 0.455 | 8.20 |
| 40 | 16.82 | 594.4 | 834.1 | 939.7 | 173.1 | 343.9 | 0.495 | 9.00 |
| 60 | 18.42 | 551.6 | 790.3 | 889.0 | 146.9 | 307.9 | 0.444 | 9.30 |
| unbounded | 18.22 | 546.4 | 713.3 | 730.1 | 149.4 | 300.4 | 0.444 | 9.25 |

