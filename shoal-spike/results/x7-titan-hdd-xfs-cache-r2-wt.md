### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.07 µs |
| fdatasync of a clean file | p50 23.52 µs, p99 37.40 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8244 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8253 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.98 µs, p99 33.37 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 2 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wt | 0 | 83.20 | no | 349.9 | 406.0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | kernel-wt | 41.60 | 41.60 | no | 372.7 | 487.3 | 39.32 | 317.7 | 375.6 | 35.27 | 292.1 | 1.03 | 4.10 | 0.753 | 0 |
| 32 | offset-ino-wt | 41.60 | 41.60 | no | 564.7 | 907.4 | 63.11 | 428.6 | 495.3 | 62.63 | 412.2 | 1.03 | 3.89 | 0.852 | 0 |
| 32 | offset-qd1-wt | 41.60 | 41.60 | no | 622.6 | 877.9 | 51.71 | 462.1 | 520.6 | 62.58 | 423.4 | 1.04 | 3.89 | 0.843 | 0 |
| 32 | arrival-qd1-wt | 41.60 | 25.60 | **yes** | 1125 | 1491 | 41.29 | 238.9 | 308.3 | 40.22 | 220.2 | 1.04 | 3.89 | 0.832 | 0 |
| 32 | idle-wt | 0 | 0 | no | 0 | 0 | 22.74 | 46.81 | 182.8 | 14.42 | 65.71 | 1.03 | 3.85 | 0.576 | 0 |
| 128 | capacity-wt | 0 | 76.80 | no | 1347 | 1394 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | kernel-wt | 38.40 | 38.40 | no | 1396 | 1417 | 32.41 | 1042 | 1370 | 37.39 | 1117 | 1.06 | 991.9 | 0.752 | 0 |
| 128 | offset-ino-wt | 38.40 | 38.40 | no | 2276 | 2466 | 43.15 | 911.7 | 1353 | 44.14 | 678.9 | 1.05 | 3.90 | 0.815 | 0 |
| 128 | offset-qd1-wt | 38.40 | 38.40 | no | 2288 | 2807 | 52.45 | 809.9 | 1185 | 56.57 | 824.3 | 1.06 | 1.36 | 0.800 | 0 |
| 128 | arrival-qd1-wt | 38.40 | 19.20 | **yes** | 4861 | 4932 | 41.09 | 229.3 | 294.1 | 43.54 | 176.1 | 1.03 | 1.27 | 0.842 | 0 |
| 128 | idle-wt | 0 | 0 | no | 0 | 0 | 22.53 | 92.47 | 222.1 | 14.85 | 70.15 | 1.03 | 1.30 | 0.567 | 0 |

### X7 · A foreground under a deep scrub's budget, round 2 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 49.39 | 97.73 | 105.2 | 11.42 | 59.04 | 0.547 | 0 |
| 10 | 10.01 | 52.93 | 117.6 | 142.1 | 12.55 | 97.17 | 0.592 | 0 |
| 20 | 20.02 | 59.61 | 118.3 | 181.0 | 14.41 | 104.6 | 0.631 | 0 |
| 40 | 40.04 | 98.27 | 256.4 | 434.3 | 22.54 | 180.1 | 0.730 | 0 |
| 60 | 59.86 | 163.1 | 507.6 | 536.8 | 60.87 | 416.8 | 0.804 | 0 |
| unbounded | 73.27 | 306.2 | 848.6 | 918.3 | 98.18 | 518.3 | 0.850 | 0 |

