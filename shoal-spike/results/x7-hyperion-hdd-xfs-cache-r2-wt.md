### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.64 µs |
| fdatasync of a clean file | p50 24.20 µs, p99 42.80 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 33 KiB and 0 flushes a rename | p50 8248 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8263 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.50 µs, p99 33.16 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 2 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wt | 0 | 96.00 | no | 325.7 | 394.7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | kernel-wt | 48.00 | 46.40 | no | 366.1 | 433.4 | 41.54 | 298.7 | 400.6 | 40.03 | 278.2 | 1.04 | 3.87 | 0.801 | 0 |
| 32 | offset-ino-wt | 48.00 | 46.40 | no | 564.5 | 797.8 | 69.01 | 419.5 | 497.5 | 72.45 | 415.1 | 1.04 | 1.31 | 0.878 | 0 |
| 32 | offset-qd1-wt | 48.00 | 46.40 | no | 576.4 | 830.2 | 82.55 | 446.2 | 505.7 | 78.93 | 419.4 | 1.05 | 3.89 | 0.873 | 0 |
| 32 | arrival-qd1-wt | 48.00 | 27.20 | **yes** | 1126 | 1301 | 40.25 | 166.2 | 374.9 | 38.59 | 170.0 | 1.06 | 3.92 | 0.826 | 0 |
| 32 | idle-wt | 0 | 0 | no | 0 | 0 | 22.26 | 45.37 | 228.8 | 13.50 | 46.32 | 1.05 | 1.21 | 0.567 | 0 |
| 128 | capacity-wt | 0 | 102.4 | no | 1111 | 1125 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | kernel-wt | 51.20 | 44.80 | **yes** | 1323 | 1409 | 55.68 | 788.1 | 1132 | 82.87 | 1083 | 1.08 | 951.0 | 0.789 | 0 |
| 128 | offset-ino-wt | 51.20 | 44.80 | **yes** | 2216 | 2727 | 89.77 | 803.6 | 941.1 | 99.26 | 663.4 | 1.02 | 1.29 | 0.884 | 0 |
| 128 | offset-qd1-wt | 51.20 | 44.80 | **yes** | 2449 | 2576 | 78.37 | 668.9 | 1256 | 89.72 | 621.4 | 1.07 | 3.97 | 0.877 | 0 |
| 128 | arrival-qd1-wt | 51.20 | 25.60 | **yes** | 4501 | 4527 | 44.71 | 208.3 | 380.5 | 44.72 | 203.9 | 1.06 | 1.34 | 0.836 | 0 |
| 128 | idle-wt | 0 | 0 | no | 0 | 0 | 23.18 | 48.10 | 231.3 | 13.72 | 49.01 | 1.01 | 3.87 | 0.580 | 0 |

### X7 · A foreground under a deep scrub's budget, round 2 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 51.06 | 84.76 | 95.23 | 16.80 | 61.93 | 0.601 | 0 |
| 10 | 10.01 | 55.04 | 120.6 | 147.0 | 18.85 | 83.33 | 0.615 | 0 |
| 20 | 20.02 | 62.84 | 141.2 | 166.4 | 19.48 | 97.17 | 0.666 | 0 |
| 40 | 40.04 | 94.52 | 237.7 | 282.3 | 25.58 | 146.2 | 0.704 | 0 |
| 60 | 60.06 | 149.7 | 470.9 | 542.9 | 51.14 | 295.6 | 0.796 | 0 |
| unbounded | 78.08 | 269.5 | 665.3 | 862.8 | 100.9 | 477.2 | 0.870 | 0 |

