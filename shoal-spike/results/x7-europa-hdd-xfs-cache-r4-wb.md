### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 316.5 µs |
| fdatasync of a clean file | p50 51.00 µs, p99 55.99 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 552.6 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 593.6 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.36 µs, p99 16.11 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 4 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wb | 0 | 121.6 | no | 248.8 | 374.3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | kernel-wb | 60.80 | 60.80 | no | 333.3 | 428.4 | 43.43 | 263.9 | 305.0 | 78.85 | 336.0 | 0.566 | 0.955 | 0.341 | 17.60 |
| 32 | offset-ino-wb | 60.80 | 60.80 | no | 362.6 | 887.4 | 49.22 | 259.8 | 281.7 | 91.10 | 303.1 | 0.510 | 0.883 | 0.464 | 19.05 |
| 32 | offset-qd1-wb | 60.80 | 59.20 | no | 411.0 | 744.5 | 51.04 | 257.0 | 283.3 | 91.96 | 327.7 | 0.329 | 0.890 | 0.456 | 18.30 |
| 32 | arrival-qd1-wb | 60.80 | 60.80 | no | 453.2 | 827.3 | 54.62 | 261.3 | 303.2 | 96.78 | 301.2 | 0.548 | 0.836 | 0.444 | 18.90 |
| 32 | idle-wb | 0 | 0 | no | 0 | 0 | 14.23 | 34.20 | 36.73 | 14.89 | 34.21 | 0.725 | 0.931 | 0.288 | 20.00 |
| 128 | capacity-wb | 0 | 153.6 | no | 680.3 | 773.9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | kernel-wb | 76.80 | 76.80 | no | 1095 | 1225 | 100.4 | 643.8 | 781.5 | 240.8 | 927.3 | 0.633 | 373.7 | 0.471 | 20.20 |
| 128 | offset-ino-wb | 76.80 | 51.20 | **yes** | 2116 | 2354 | 44.85 | 198.4 | 251.5 | 85.77 | 265.2 | 0.376 | 0.853 | 0.614 | 23.50 |
| 128 | offset-qd1-wb | 76.80 | 44.80 | **yes** | 2238 | 2925 | 41.61 | 148.2 | 235.3 | 79.83 | 265.4 | 0.372 | 0.877 | 0.635 | 23.65 |
| 128 | arrival-qd1-wb | 76.80 | 57.60 | **yes** | 1985 | 2411 | 56.43 | 269.2 | 340.5 | 100.9 | 332.3 | 0.353 | 0.838 | 0.511 | 20.80 |
| 128 | idle-wb | 0 | 0 | no | 0 | 0 | 13.98 | 35.22 | 140.3 | 14.71 | 35.75 | 0.725 | 0.918 | 0.284 | 20.05 |

### X7 · A foreground under a deep scrub's budget, round 4 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 14.10 | 23.77 | 25.57 | 12.63 | 24.61 | 0.244 | 20.00 |
| 10 | 10.01 | 16.25 | 67.94 | 90.43 | 14.69 | 46.19 | 0.369 | 20.25 |
| 20 | 20.02 | 17.36 | 95.49 | 109.1 | 15.28 | 59.08 | 0.448 | 20.15 |
| 40 | 40.04 | 37.08 | 213.2 | 325.7 | 18.40 | 116.7 | 0.499 | 20.25 |
| 60 | 59.66 | 78.56 | 377.1 | 442.6 | 42.51 | 175.5 | 0.603 | 19.65 |
| unbounded | 74.07 | 163.7 | 367.3 | 433.8 | 65.80 | 185.9 | 0.711 | 19.70 |

