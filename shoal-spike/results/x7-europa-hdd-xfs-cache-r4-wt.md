### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 54.65 µs |
| fdatasync of a clean file | p50 10.50 µs, p99 24.05 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7700 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7664 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.12 µs, p99 20.17 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 4 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wt | 0 | 115.2 | no | 266.2 | 295.6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | kernel-wt | 57.60 | 56.00 | no | 307.9 | 370.7 | 25.21 | 237.3 | 270.2 | 28.17 | 223.9 | 0.580 | 30.00 | 0.727 | 0 |
| 32 | offset-ino-wt | 57.60 | 38.40 | **yes** | 796.5 | 1095 | 26.29 | 125.1 | 285.2 | 25.55 | 82.10 | 0.554 | 0.927 | 0.884 | 0 |
| 32 | offset-qd1-wt | 57.60 | 40.00 | **yes** | 798.5 | 1059 | 24.56 | 110.5 | 135.8 | 25.28 | 62.85 | 0.571 | 0.877 | 0.880 | 0 |
| 32 | arrival-qd1-wt | 57.60 | 36.80 | **yes** | 840.7 | 964.5 | 24.41 | 76.14 | 103.4 | 25.13 | 63.05 | 0.574 | 0.863 | 0.879 | 0 |
| 32 | idle-wt | 0 | 0 | no | 0 | 0 | 16.03 | 32.94 | 141.0 | 11.49 | 36.58 | 0.722 | 0.891 | 0.415 | 0 |
| 128 | capacity-wt | 0 | 102.4 | no | 1014 | 1093 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | kernel-wt | 51.20 | 44.80 | **yes** | 1183 | 1235 | 24.86 | 789.6 | 913.4 | 26.70 | 874.7 | 0.818 | 740.5 | 0.637 | 0 |
| 128 | offset-ino-wt | 51.20 | 32.00 | **yes** | 3057 | 3209 | 21.51 | 73.87 | 156.8 | 25.26 | 68.53 | 0.550 | 0.884 | 0.890 | 0 |
| 128 | offset-qd1-wt | 51.20 | 32.00 | **yes** | 2920 | 3099 | 20.46 | 109.1 | 301.3 | 25.30 | 65.57 | 0.587 | 0.950 | 0.891 | 0 |
| 128 | arrival-qd1-wt | 51.20 | 32.00 | **yes** | 3377 | 3521 | 21.24 | 66.30 | 94.97 | 25.98 | 67.16 | 0.576 | 0.909 | 0.863 | 0 |
| 128 | idle-wt | 0 | 0 | no | 0 | 0 | 16.16 | 33.93 | 37.98 | 13.37 | 35.32 | 0.724 | 0.922 | 0.420 | 0 |

### X7 · A foreground under a deep scrub's budget, round 4 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 39.46 | 59.70 | 72.14 | 13.41 | 33.30 | 0.481 | 0 |
| 10 | 10.01 | 44.56 | 104.2 | 133.9 | 14.48 | 57.74 | 0.571 | 0 |
| 20 | 20.02 | 51.73 | 118.1 | 136.0 | 15.20 | 50.20 | 0.620 | 0 |
| 40 | 40.04 | 86.07 | 263.9 | 390.8 | 22.69 | 194.6 | 0.612 | 0 |
| 60 | 60.06 | 109.8 | 476.5 | 576.0 | 42.49 | 326.7 | 0.694 | 0 |
| unbounded | 87.49 | 197.5 | 622.7 | 667.3 | 68.07 | 416.4 | 0.771 | 0 |

