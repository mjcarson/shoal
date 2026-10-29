### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 54.02 µs |
| fdatasync of a clean file | p50 10.38 µs, p99 25.62 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7755 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 7649 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.10 µs, p99 20.06 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 3 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wt | 0 | 102.4 | no | 281.5 | 744.1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | idle-wt | 0 | 0 | no | 0 | 0 | 16.28 | 34.78 | 65.84 | 14.19 | 37.30 | 0.703 | 0.898 | 0.441 | 0 |
| 32 | arrival-qd1-wt | 51.20 | 35.20 | **yes** | 859.5 | 1150 | 20.18 | 76.96 | 166.5 | 28.74 | 77.92 | 0.561 | 0.895 | 0.886 | 0 |
| 32 | offset-qd1-wt | 51.20 | 38.40 | **yes** | 810.8 | 1033 | 20.72 | 72.55 | 91.67 | 27.81 | 68.64 | 0.583 | 0.960 | 0.871 | 0 |
| 32 | offset-ino-wt | 51.20 | 36.80 | **yes** | 832.2 | 1073 | 21.16 | 72.09 | 109.9 | 29.11 | 80.55 | 0.552 | 0.904 | 0.882 | 0 |
| 32 | kernel-wt | 51.20 | 51.20 | no | 309.4 | 390.0 | 21.51 | 237.4 | 271.3 | 25.46 | 226.5 | 0.577 | 1.55 | 0.676 | 0 |
| 128 | capacity-wt | 0 | 102.4 | no | 1017 | 1091 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | idle-wt | 0 | 0 | no | 0 | 0 | 15.92 | 32.29 | 107.5 | 13.53 | 45.48 | 0.727 | 0.908 | 0.435 | 0 |
| 128 | arrival-qd1-wt | 51.20 | 32.00 | **yes** | 3520 | 3748 | 21.01 | 69.75 | 109.4 | 26.76 | 61.84 | 0.555 | 0.904 | 0.875 | 0 |
| 128 | offset-qd1-wt | 51.20 | 38.40 | **yes** | 3224 | 3499 | 22.97 | 78.35 | 135.9 | 26.55 | 63.33 | 0.536 | 0.910 | 0.880 | 0 |
| 128 | offset-ino-wt | 51.20 | 32.00 | **yes** | 3151 | 3300 | 21.64 | 105.0 | 171.6 | 27.90 | 83.21 | 0.574 | 0.962 | 0.881 | 0 |
| 128 | kernel-wt | 51.20 | 44.80 | **yes** | 1127 | 1208 | 24.06 | 702.6 | 780.8 | 30.90 | 810.2 | 0.818 | 730.4 | 0.676 | 0 |

### X7 · A foreground under a deep scrub's budget, round 3 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 42.89 | 73.76 | 213.1 | 12.90 | 42.79 | 0.520 | 0 |
| 10 | 10.01 | 49.10 | 104.5 | 109.8 | 14.75 | 48.24 | 0.604 | 0 |
| 20 | 20.02 | 59.60 | 111.4 | 114.2 | 15.59 | 54.32 | 0.649 | 0 |
| 40 | 40.04 | 84.42 | 306.2 | 330.6 | 20.58 | 256.3 | 0.615 | 0 |
| 60 | 60.06 | 106.2 | 427.9 | 491.8 | 35.69 | 244.5 | 0.675 | 0 |
| unbounded | 87.49 | 232.3 | 688.9 | 758.4 | 70.25 | 410.4 | 0.794 | 0 |

