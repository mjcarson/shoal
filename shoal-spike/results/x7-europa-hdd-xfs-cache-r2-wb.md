### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 319.1 µs |
| fdatasync of a clean file | p50 49.91 µs, p99 54.59 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 530.2 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 587.5 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.15 µs, p99 20.53 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 2 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wb | 0 | 121.6 | no | 244.9 | 275.2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | kernel-wb | 60.80 | 60.80 | no | 338.9 | 483.8 | 48.59 | 269.4 | 298.5 | 91.31 | 344.5 | 0.550 | 0.908 | 0.348 | 17.15 |
| 32 | offset-ino-wb | 60.80 | 60.80 | no | 360.1 | 626.9 | 44.69 | 281.0 | 313.0 | 78.41 | 317.0 | 0.313 | 0.853 | 0.411 | 18.70 |
| 32 | offset-qd1-wb | 60.80 | 60.80 | no | 382.2 | 790.5 | 50.27 | 275.3 | 294.8 | 86.29 | 333.8 | 0.300 | 0.874 | 0.400 | 18.85 |
| 32 | arrival-qd1-wb | 60.80 | 60.80 | no | 392.8 | 627.2 | 49.51 | 265.8 | 294.6 | 84.86 | 300.8 | 0.300 | 0.828 | 0.421 | 19.05 |
| 32 | idle-wb | 0 | 0 | no | 0 | 0 | 13.14 | 32.87 | 100.0 | 13.64 | 32.88 | 0.552 | 0.886 | 0.274 | 20.15 |
| 128 | capacity-wb | 0 | 179.2 | no | 658.5 | 689.5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | kernel-wb | 89.60 | 83.20 | **yes** | 1095 | 1229 | 161.1 | 671.2 | 686.2 | 338.2 | 919.8 | 0.389 | 335.3 | 0.469 | 19.35 |
| 128 | offset-ino-wb | 89.60 | 64.00 | **yes** | 1816 | 2120 | 44.11 | 208.0 | 268.8 | 80.09 | 256.7 | 0.330 | 0.807 | 0.579 | 24.60 |
| 128 | offset-qd1-wb | 89.60 | 64.00 | **yes** | 1752 | 2135 | 48.63 | 226.1 | 317.4 | 85.95 | 290.4 | 0.326 | 0.754 | 0.593 | 24.85 |
| 128 | arrival-qd1-wb | 89.60 | 64.00 | **yes** | 1891 | 2040 | 68.26 | 269.3 | 307.4 | 110.6 | 339.9 | 0.348 | 0.803 | 0.467 | 20.50 |
| 128 | idle-wb | 0 | 0 | no | 0 | 0 | 14.43 | 54.37 | 135.9 | 15.18 | 60.40 | 0.691 | 0.905 | 0.317 | 20.15 |

### X7 · A foreground under a deep scrub's budget, round 2 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 14.02 | 23.77 | 25.65 | 12.74 | 23.80 | 0.237 | 20.25 |
| 10 | 10.01 | 15.84 | 84.92 | 93.70 | 14.36 | 46.03 | 0.363 | 20.15 |
| 20 | 20.02 | 18.48 | 83.01 | 96.69 | 15.17 | 57.12 | 0.432 | 20.20 |
| 40 | 40.04 | 37.88 | 247.5 | 257.9 | 18.14 | 121.3 | 0.490 | 20.00 |
| 60 | 60.06 | 75.60 | 362.8 | 385.2 | 44.08 | 151.9 | 0.570 | 20.05 |
| unbounded | 71.07 | 176.9 | 439.6 | 484.4 | 72.38 | 189.3 | 0.699 | 19.50 |

