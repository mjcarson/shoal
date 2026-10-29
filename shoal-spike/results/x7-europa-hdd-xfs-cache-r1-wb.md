### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 326.5 µs |
| fdatasync of a clean file | p50 50.82 µs, p99 70.95 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 591.2 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 662.0 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.28 µs, p99 19.77 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 1 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wb | 0 | 121.6 | no | 246.9 | 278.0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | idle-wb | 0 | 0 | no | 0 | 0 | 14.00 | 58.61 | 187.9 | 14.71 | 72.10 | 0.725 | 0.916 | 0.285 | 20.10 |
| 32 | arrival-qd1-wb | 60.80 | 60.80 | no | 340.8 | 547.5 | 32.80 | 257.1 | 283.9 | 59.89 | 289.8 | 0.469 | 0.930 | 0.310 | 19.35 |
| 32 | offset-qd1-wb | 60.80 | 60.80 | no | 324.4 | 505.8 | 26.40 | 256.1 | 282.9 | 53.81 | 286.6 | 0.558 | 0.915 | 0.326 | 19.50 |
| 32 | offset-ino-wb | 60.80 | 60.80 | no | 349.4 | 606.4 | 37.99 | 251.8 | 299.9 | 75.88 | 305.1 | 0.571 | 0.887 | 0.395 | 18.90 |
| 32 | kernel-wb | 60.80 | 60.80 | no | 333.5 | 391.5 | 41.24 | 272.6 | 290.6 | 75.87 | 324.2 | 0.564 | 1.03 | 0.333 | 17.75 |
| 128 | capacity-wb | 0 | 153.6 | no | 677.2 | 714.6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | idle-wb | 0 | 0 | no | 0 | 0 | 13.67 | 33.81 | 134.0 | 14.18 | 47.50 | 0.748 | 0.941 | 0.269 | 20.10 |
| 128 | arrival-qd1-wb | 76.80 | 70.40 | **yes** | 1559 | 1791 | 56.25 | 247.0 | 320.5 | 104.2 | 337.9 | 0.361 | 0.867 | 0.429 | 22.75 |
| 128 | offset-qd1-wb | 76.80 | 76.80 | no | 1277 | 2105 | 34.24 | 201.3 | 293.9 | 69.17 | 246.8 | 0.345 | 0.941 | 0.495 | 26.90 |
| 128 | offset-ino-wb | 76.80 | 70.40 | **yes** | 1270 | 1764 | 31.91 | 188.8 | 264.5 | 66.42 | 234.7 | 0.427 | 0.924 | 0.478 | 25.10 |
| 128 | kernel-wb | 76.80 | 76.80 | no | 1033 | 1114 | 68.76 | 511.4 | 575.8 | 186.7 | 879.2 | 0.620 | 267.5 | 0.405 | 21.00 |

### X7 · A foreground under a deep scrub's budget, round 1 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 14.64 | 31.82 | 198.0 | 13.60 | 33.86 | 0.265 | 20.15 |
| 10 | 10.01 | 16.60 | 77.64 | 95.60 | 14.66 | 49.41 | 0.368 | 20.25 |
| 20 | 20.02 | 17.84 | 84.43 | 90.78 | 14.88 | 56.60 | 0.435 | 20.00 |
| 40 | 40.04 | 36.64 | 127.8 | 145.5 | 18.56 | 104.7 | 0.487 | 20.00 |
| 60 | 59.86 | 89.55 | 318.4 | 364.0 | 46.17 | 147.7 | 0.593 | 20.00 |
| unbounded | 73.27 | 166.7 | 371.5 | 405.4 | 69.89 | 179.0 | 0.726 | 19.95 |

