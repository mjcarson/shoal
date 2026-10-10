### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 51.75 µs |
| fdatasync of a clean file | p50 10.24 µs, p99 20.56 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 7727 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8075 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.09 µs, p99 19.73 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 2 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wt | 0 | 115.2 | no | 265.4 | 297.6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | kernel-wt | 57.60 | 56.00 | no | 305.0 | 360.3 | 26.59 | 271.8 | 676.5 | 27.08 | 249.4 | 0.461 | 1.30 | 0.722 | 0 |
| 32 | offset-ino-wt | 57.60 | 36.80 | **yes** | 807.8 | 1000.0 | 26.44 | 142.6 | 190.0 | 29.63 | 95.95 | 0.320 | 0.871 | 0.880 | 0 |
| 32 | offset-qd1-wt | 57.60 | 38.40 | **yes** | 822.3 | 1033 | 26.33 | 110.0 | 245.2 | 27.57 | 79.28 | 0.330 | 0.832 | 0.878 | 0 |
| 32 | arrival-qd1-wt | 57.60 | 33.60 | **yes** | 881.0 | 1163 | 25.41 | 72.23 | 108.6 | 28.62 | 79.52 | 0.545 | 0.897 | 0.875 | 0 |
| 32 | idle-wt | 0 | 0 | no | 0 | 0 | 16.01 | 43.00 | 150.8 | 11.81 | 40.52 | 0.566 | 0.806 | 0.424 | 0 |
| 128 | capacity-wt | 0 | 102.4 | no | 1075 | 1076 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | kernel-wt | 51.20 | 44.80 | **yes** | 1111 | 1204 | 24.79 | 707.4 | 871.8 | 29.21 | 786.4 | 0.771 | 694.8 | 0.674 | 0 |
| 128 | offset-ino-wt | 51.20 | 32.00 | **yes** | 3175 | 3429 | 22.74 | 146.3 | 302.1 | 27.11 | 76.73 | 0.336 | 0.843 | 0.878 | 0 |
| 128 | offset-qd1-wt | 51.20 | 32.00 | **yes** | 2974 | 3297 | 23.39 | 105.2 | 126.5 | 27.47 | 84.57 | 0.550 | 0.795 | 0.873 | 0 |
| 128 | arrival-qd1-wt | 51.20 | 32.00 | **yes** | 3328 | 3409 | 21.72 | 88.61 | 102.2 | 26.77 | 74.94 | 0.469 | 0.876 | 0.871 | 0 |
| 128 | idle-wt | 0 | 0 | no | 0 | 0 | 16.02 | 35.37 | 110.8 | 13.72 | 38.63 | 0.574 | 0.937 | 0.444 | 0 |

### X7 · A foreground under a deep scrub's budget, round 2 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 42.04 | 63.69 | 67.37 | 12.91 | 34.19 | 0.520 | 0 |
| 10 | 10.01 | 49.10 | 104.8 | 118.5 | 14.21 | 45.85 | 0.603 | 0 |
| 20 | 20.02 | 61.46 | 111.6 | 177.1 | 15.55 | 52.67 | 0.650 | 0 |
| 40 | 40.04 | 83.63 | 366.7 | 554.1 | 21.06 | 190.4 | 0.605 | 0 |
| 60 | 59.86 | 111.3 | 498.5 | 773.2 | 34.92 | 257.1 | 0.676 | 0 |
| unbounded | 89.49 | 274.7 | 699.1 | 771.0 | 76.84 | 362.7 | 0.789 | 0 |

