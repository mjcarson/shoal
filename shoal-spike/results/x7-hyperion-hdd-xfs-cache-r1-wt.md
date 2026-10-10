### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 16.16 KiB a sync · sync p50 28.78 µs |
| fdatasync of a clean file | p50 25.07 µs, p99 49.36 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8245 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8262 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.19 µs, p99 32.96 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 1 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wt | 0 | 89.60 | no | 344.8 | 374.2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | idle-wt | 0 | 0 | no | 0 | 0 | 21.89 | 46.34 | 65.57 | 14.05 | 39.78 | 1.05 | 1.25 | 0.515 | 0 |
| 32 | arrival-qd1-wt | 44.80 | 27.20 | **yes** | 1111 | 1255 | 41.00 | 192.7 | 306.8 | 32.60 | 101.5 | 1.05 | 3.91 | 0.852 | 0 |
| 32 | offset-qd1-wt | 44.80 | 43.20 | no | 544.6 | 703.5 | 61.79 | 442.3 | 537.6 | 50.05 | 247.6 | 1.06 | 1.32 | 0.839 | 0 |
| 32 | offset-ino-wt | 44.80 | 43.20 | no | 539.7 | 723.2 | 61.08 | 423.6 | 624.6 | 48.08 | 255.0 | 1.04 | 3.92 | 0.834 | 0 |
| 32 | kernel-wt | 44.80 | 43.20 | no | 355.2 | 457.3 | 43.02 | 320.3 | 332.4 | 37.09 | 250.2 | 1.03 | 4.27 | 0.779 | 0 |
| 128 | capacity-wt | 0 | 102.4 | no | 1242 | 1245 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | idle-wt | 0 | 0 | no | 0 | 0 | 22.07 | 56.09 | 165.6 | 15.10 | 46.53 | 1.05 | 3.88 | 0.546 | 0 |
| 128 | arrival-qd1-wt | 51.20 | 25.60 | **yes** | 4365 | 4399 | 39.75 | 234.4 | 295.9 | 33.87 | 92.94 | 1.06 | 1.26 | 0.865 | 0 |
| 128 | offset-qd1-wt | 51.20 | 44.80 | **yes** | 2257 | 2385 | 75.86 | 972.2 | 1277 | 59.39 | 449.6 | 1.03 | 1.22 | 0.845 | 0 |
| 128 | offset-ino-wt | 51.20 | 44.80 | **yes** | 2429 | 2540 | 79.96 | 878.7 | 1156 | 64.30 | 433.9 | 1.05 | 3.92 | 0.882 | 0 |
| 128 | kernel-wt | 51.20 | 44.80 | **yes** | 1355 | 1461 | 57.22 | 948.7 | 1150 | 59.01 | 1039 | 1.13 | 968.7 | 0.802 | 0 |

### X7 · A foreground under a deep scrub's budget, round 1 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 44.95 | 72.72 | 77.43 | 24.26 | 64.87 | 0.636 | 0 |
| 10 | 10.01 | 50.08 | 110.6 | 132.4 | 25.76 | 104.4 | 0.665 | 0 |
| 20 | 20.02 | 52.17 | 125.1 | 154.4 | 31.23 | 127.1 | 0.714 | 0 |
| 40 | 39.84 | 83.93 | 244.0 | 306.4 | 38.75 | 149.4 | 0.747 | 0 |
| 60 | 59.86 | 113.4 | 516.8 | 589.9 | 57.61 | 354.7 | 0.826 | 0 |
| unbounded | 72.67 | 193.9 | 689.1 | 773.1 | 94.86 | 485.6 | 0.866 | 0 |

