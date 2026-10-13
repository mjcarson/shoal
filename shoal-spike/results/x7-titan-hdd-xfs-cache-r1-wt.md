### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.82 µs |
| fdatasync of a clean file | p50 23.98 µs, p99 46.40 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8244 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8257 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.45 µs, p99 33.63 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 1 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wt | 0 | 83.20 | no | 348.2 | 440.6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | idle-wt | 0 | 0 | no | 0 | 0 | 22.11 | 60.36 | 190.5 | 14.99 | 47.17 | 1.03 | 3.81 | 0.538 | 0 |
| 32 | arrival-qd1-wt | 41.60 | 27.20 | **yes** | 1106 | 1242 | 40.52 | 203.7 | 230.2 | 33.43 | 98.02 | 1.04 | 3.88 | 0.859 | 0 |
| 32 | offset-qd1-wt | 41.60 | 41.60 | no | 586.5 | 870.6 | 55.66 | 431.0 | 493.1 | 45.25 | 250.2 | 1.03 | 3.84 | 0.828 | 0 |
| 32 | offset-ino-wt | 41.60 | 41.60 | no | 545.1 | 651.1 | 60.55 | 456.3 | 578.0 | 44.25 | 244.9 | 1.02 | 3.88 | 0.832 | 0 |
| 32 | kernel-wt | 41.60 | 41.60 | no | 366.8 | 447.6 | 35.79 | 277.4 | 302.9 | 31.51 | 241.8 | 1.03 | 1.36 | 0.748 | 0 |
| 128 | capacity-wt | 0 | 76.80 | no | 1249 | 1297 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | idle-wt | 0 | 0 | no | 0 | 0 | 21.65 | 59.50 | 218.0 | 15.44 | 41.23 | 1.04 | 1.23 | 0.536 | 0 |
| 128 | arrival-qd1-wt | 38.40 | 25.60 | **yes** | 4347 | 4775 | 42.73 | 236.3 | 334.2 | 33.40 | 100.7 | 1.04 | 2.44 | 0.863 | 0 |
| 128 | offset-qd1-wt | 38.40 | 38.40 | no | 2213 | 2339 | 43.12 | 814.9 | 1042 | 36.26 | 412.0 | 1.03 | 1.29 | 0.804 | 0 |
| 128 | offset-ino-wt | 38.40 | 38.40 | no | 2236 | 2399 | 44.88 | 885.4 | 1447 | 37.20 | 479.2 | 1.04 | 3.98 | 0.798 | 0 |
| 128 | kernel-wt | 38.40 | 38.40 | no | 1427 | 1469 | 36.23 | 916.3 | 1059 | 33.60 | 1066 | 1.07 | 1017 | 0.753 | 0 |

### X7 · A foreground under a deep scrub's budget, round 1 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 46.74 | 153.6 | 322.5 | 24.47 | 77.25 | 0.639 | 0 |
| 10 | 10.01 | 51.75 | 112.6 | 136.4 | 26.44 | 98.94 | 0.651 | 0 |
| 20 | 20.02 | 60.61 | 114.7 | 141.2 | 31.37 | 124.7 | 0.701 | 0 |
| 40 | 39.84 | 92.56 | 255.7 | 418.1 | 41.83 | 214.3 | 0.772 | 0 |
| 60 | 60.06 | 116.8 | 482.2 | 497.3 | 62.64 | 353.4 | 0.818 | 0 |
| unbounded | 71.67 | 177.0 | 619.7 | 825.8 | 89.98 | 549.9 | 0.856 | 0 |

