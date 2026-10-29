### Probes

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 334.6 µs |
| fdatasync of a clean file | p50 51.02 µs, p99 57.39 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 2.00 flushes a rename | p50 565.0 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 553.5 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 13.31 µs, p99 15.46 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · nvme1n1 INTEL SSDPED1D280GA (solid state, fw E2010480, cache write through, 512/512 B blocks) · xfs at /optane on /dev/nvme1n1p1 = nvme1n1p1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 3 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wb | 0 | 121.6 | no | 249.5 | 290.8 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | idle-wb | 0 | 0 | no | 0 | 0 | 14.53 | 59.08 | 127.5 | 15.15 | 159.1 | 0.728 | 0.890 | 0.290 | 20.05 |
| 32 | arrival-qd1-wb | 60.80 | 59.20 | no | 349.6 | 829.7 | 45.17 | 272.5 | 289.5 | 86.57 | 319.0 | 0.568 | 0.862 | 0.371 | 18.75 |
| 32 | offset-qd1-wb | 60.80 | 60.80 | no | 339.8 | 665.5 | 34.52 | 261.2 | 288.1 | 70.90 | 298.1 | 0.552 | 0.876 | 0.353 | 18.85 |
| 32 | offset-ino-wb | 60.80 | 59.20 | no | 351.9 | 698.9 | 36.09 | 271.0 | 295.5 | 75.61 | 313.6 | 0.554 | 0.891 | 0.386 | 19.10 |
| 32 | kernel-wb | 60.80 | 60.80 | no | 326.1 | 398.2 | 35.59 | 267.9 | 335.5 | 67.62 | 342.6 | 0.576 | 0.921 | 0.295 | 17.75 |
| 128 | capacity-wb | 0 | 153.6 | no | 680.8 | 694.0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | idle-wb | 0 | 0 | no | 0 | 0 | 15.05 | 57.08 | 112.1 | 15.42 | 64.71 | 0.748 | 1.00 | 0.310 | 20.15 |
| 128 | arrival-qd1-wb | 76.80 | 57.60 | **yes** | 1910 | 2335 | 56.35 | 271.6 | 363.3 | 105.5 | 350.5 | 0.355 | 0.736 | 0.512 | 21.75 |
| 128 | offset-qd1-wb | 76.80 | 44.80 | **yes** | 2128 | 2811 | 46.87 | 221.5 | 301.8 | 86.22 | 277.3 | 0.345 | 0.876 | 0.622 | 22.25 |
| 128 | offset-ino-wb | 76.80 | 44.80 | **yes** | 2425 | 2804 | 45.09 | 185.0 | 228.1 | 86.79 | 217.3 | 0.361 | 0.835 | 0.659 | 23.30 |
| 128 | kernel-wb | 76.80 | 76.80 | no | 1073 | 1233 | 104.3 | 615.7 | 718.4 | 223.4 | 923.4 | 0.347 | 376.3 | 0.464 | 18.80 |

### X7 · A foreground under a deep scrub's budget, round 3 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 91.6% of the device, 5120 GiB apart (p5 to p95))

europa · AMD Ryzen 9 7945HX with Radeon Graphics · governor performance · kernel 7.0.0-34-generic · sda WDC WD6001FZWX-00A2VA0 (7200 rpm, fw 1A01, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg europa hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 15.57 | 49.11 | 247.2 | 14.32 | 61.18 | 0.284 | 20.15 |
| 10 | 10.01 | 16.57 | 84.97 | 100.0 | 14.66 | 57.10 | 0.375 | 20.25 |
| 20 | 20.02 | 18.11 | 90.19 | 92.03 | 15.29 | 55.95 | 0.452 | 20.15 |
| 40 | 40.04 | 39.19 | 168.5 | 199.6 | 18.86 | 108.1 | 0.491 | 20.00 |
| 60 | 59.86 | 93.93 | 388.0 | 456.3 | 45.25 | 179.5 | 0.624 | 20.00 |
| unbounded | 72.27 | 179.0 | 386.9 | 439.4 | 68.61 | 184.8 | 0.742 | 19.60 |

