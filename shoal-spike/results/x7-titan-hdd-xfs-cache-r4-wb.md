### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 100 · fs device 0 | 4.00 KiB a sync · sync p50 8066 µs |
| fdatasync of a clean file | p50 492.1 µs, p99 523.4 µs | 100 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 33 KiB and 2.00 flushes a rename | p50 8229 µs |
| rename, then the directory's Fsync | 4 KiB and 2.00 flushes a rename | p50 8244 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.18 µs, p99 33.10 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · Reads and stages while applies run, round 4 (applies of 64 KiB offered at half the capacity row; reads of 64 KiB and 4 KiB stages at 20/s and 20/s, open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| batch | side | offered/s | applies/s | behind | batch p50 ms | batch p99 ms | read p50 ms | read p99 ms | read max ms | disk stage p50 ms | disk stage p99 ms | SSD stage p50 ms | SSD stage p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | capacity-wb | 0 | 96.00 | no | 323.1 | 428.1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | kernel-wb | 48.00 | 35.20 | **yes** | 900.5 | 1100 | 141.3 | 483.0 | 528.4 | 465.5 | 831.1 | 1.04 | 252.2 | 0.397 | 7.70 |
| 32 | offset-ino-wb | 48.00 | 8.00 | **yes** | 3485 | 3538 | 56.65 | 123.4 | 156.6 | 253.2 | 354.9 | 1.03 | 87.42 | 0.300 | 10.25 |
| 32 | offset-qd1-wb | 48.00 | 6.40 | **yes** | 3502 | 3569 | 56.91 | 132.6 | 251.6 | 271.0 | 399.8 | 1.03 | 51.55 | 0.327 | 10.50 |
| 32 | arrival-qd1-wb | 48.00 | 8.00 | **yes** | 3402 | 3461 | 58.16 | 118.4 | 161.4 | 241.2 | 358.7 | 1.04 | 78.24 | 0.321 | 10.30 |
| 32 | idle-wb | 0 | 0 | no | 0 | 0 | 54.42 | 113.2 | 311.1 | 152.1 | 347.3 | 1.01 | 1.35 | 0.380 | 10.00 |
| 128 | capacity-wb | 0 | 128.0 | no | 839.8 | 931.8 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 128 | kernel-wb | 64.00 | 57.60 | **yes** | 1693 | 2655 | 196.8 | 965.5 | 1157 | 849.3 | 2006 | 1.18 | 666.3 | 0.529 | 8.85 |
| 128 | offset-ino-wb | 64.00 | 0 | **yes** | 0 | 0 | 67.53 | 178.2 | 225.7 | 244.2 | 420.1 | 1.05 | 3.95 | 0.302 | 8.55 |
| 128 | offset-qd1-wb | 64.00 | 0 | **yes** | 0 | 0 | 99.44 | 223.8 | 244.8 | 308.9 | 535.9 | 1.02 | 107.0 | 0.358 | 7.25 |
| 128 | arrival-qd1-wb | 64.00 | 0 | **yes** | 0 | 0 | 84.34 | 229.9 | 324.9 | 271.2 | 513.8 | 1.02 | 94.11 | 0.317 | 7.40 |
| 128 | idle-wb | 0 | 0 | no | 0 | 0 | 94.90 | 186.7 | 285.8 | 223.6 | 291.8 | 1.02 | 3.90 | 0.404 | 7.45 |

### X7 · A foreground under a deep scrub's budget, round 4 (4 KiB writes, staged then applied, at 10/s; 64 KiB reads at 20/s; open loop; population from 0.0% to 94.2% of the device, 12288 GiB apart (p5 to p95))

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write back, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs cache

| budget MiB/s | scrub MiB/s | write p50 ms | write p99 ms | write max ms | read p50 ms | read p99 ms | disk busy | flushes/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0 | 419.0 | 576.9 | 582.8 | 106.3 | 228.2 | 0.367 | 12.10 |
| 10 | 10.01 | 463.3 | 687.8 | 787.8 | 120.6 | 258.6 | 0.400 | 10.80 |
| 20 | 18.22 | 554.2 | 758.3 | 784.7 | 161.4 | 312.3 | 0.466 | 9.25 |
| 40 | 18.22 | 534.2 | 713.5 | 777.5 | 146.4 | 315.4 | 0.465 | 9.20 |
| 60 | 18.22 | 529.4 | 766.0 | 966.0 | 154.3 | 317.4 | 0.457 | 9.25 |
| unbounded | 18.42 | 550.5 | 758.6 | 778.9 | 159.3 | 326.0 | 0.458 | 9.20 |

