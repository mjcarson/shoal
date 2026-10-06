### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 53.97 µs |
| fdatasync of a clean file | p50 10.25 µs, p99 24.18 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7707 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7610 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.27 µs, p99 20.41 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 1 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wt | 0 | 108.8 | no | 266.0 | 303.9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | idle-wt | 0 | 0 | no | 0 | 0 | 16.15 | 35.63 | 113.0 | 12.14 | 42.41 | 0.718 | 0.906 | 0.448 | 0 |
| 32 | arrival-qd1-wt | 54.40 | 33.60 | **yes** | 892.0 | 1027 | 22.10 | 66.17 | 126.0 | 30.75 | 79.24 | 0.584 | 0.889 | 0.867 | 0 |
| 32 | offset-qd1-wt | 54.40 | 38.40 | **yes** | 793.8 | 946.1 | 20.45 | 55.86 | 94.26 | 30.16 | 75.39 | 0.575 | 0.967 | 0.880 | 0 |
| 32 | offset-ino-wt | 54.40 | 36.80 | **yes** | 817.2 | 1067 | 21.46 | 62.91 | 74.38 | 31.17 | 82.99 | 0.548 | 0.918 | 0.882 | 0 |
| 32 | kernel-wt | 54.40 | 52.80 | no | 289.8 | 355.8 | 23.04 | 195.1 | 233.3 | 27.99 | 245.7 | 0.567 | 34.84 | 0.696 | 0 |
| 128 | capacity-wt | 0 | 102.4 | no | 963.2 | 1099 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | idle-wt | 0 | 0 | no | 0 | 0 | 15.32 | 35.03 | 95.55 | 11.07 | 48.49 | 0.694 | 0.935 | 0.434 | 0 |
| 128 | arrival-qd1-wt | 51.20 | 32.00 | **yes** | 3593 | 4098 | 20.43 | 58.99 | 91.81 | 31.01 | 78.16 | 0.347 | 0.894 | 0.865 | 0 |
| 128 | offset-qd1-wt | 51.20 | 38.40 | **yes** | 3205 | 3273 | 22.09 | 76.20 | 131.7 | 28.19 | 82.92 | 0.415 | 0.903 | 0.874 | 0 |
| 128 | offset-ino-wt | 51.20 | 38.40 | **yes** | 3242 | 3447 | 20.66 | 59.93 | 115.2 | 30.24 | 81.52 | 0.352 | 0.851 | 0.890 | 0 |
| 128 | kernel-wt | 51.20 | 44.80 | **yes** | 1054 | 1193 | 21.57 | 757.0 | 848.9 | 28.34 | 773.1 | 0.707 | 723.1 | 0.637 | 0 |

### X7 · A foreground under a deep scrub's budget, round 1 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 37.86 | 58.38 | 61.02 | 11.27 | 33.01 | 0.455 | 0 |
| 10 | 10.01 | 46.28 | 107.2 | 128.1 | 14.67 | 49.63 | 0.587 | 0 |
| 20 | 20.02 | 55.35 | 110.2 | 139.1 | 15.93 | 55.90 | 0.627 | 0 |
| 40 | 40.04 | 76.30 | 244.9 | 267.8 | 18.91 | 234.2 | 0.615 | 0 |
| 60 | 60.06 | 101.3 | 367.2 | 446.3 | 35.06 | 290.3 | 0.693 | 0 |
| unbounded | 90.49 | 226.5 | 805.6 | 889.3 | 64.97 | 347.7 | 0.804 | 0 |

