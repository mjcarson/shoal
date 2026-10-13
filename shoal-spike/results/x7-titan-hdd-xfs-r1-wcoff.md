### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.28 µs |
| fdatasync of a clean file | p50 23.84 µs, p99 43.26 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8242 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8256 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 23.17 µs, p99 67.18 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 1 (write cache write through)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | overwrite-wt | 119.6 | 8357 | 8398 | 9092 | 0 | 4.01 | 0.989 |
| 2 | overwrite-wt | 209.4 | 8358 | 33341 | 33390 | 0 | 4.00 | 0.982 |
| 4 | overwrite-wt | 112.8 | 33342 | 50057 | 784118 | 0 | 4.03 | 0.989 |
| 6 | overwrite-wt | 137.8 | 16684 | 108368 | 1925028 | 0 | 4.10 | 0.983 |

### 2. The journal-wt, round 1 (a 4 KiB header block on every record; write cache write through)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16K | 1 | written-ahead/each-wt | 119.2 | 119.5 | 1.00 | 8405 | 9075 | 10308 | 20.01 | 0 | 109.7 |
| 16K | 6 | written-ahead/each-wt | 10071 | 10065 | 1.00 | 413.1 | 9443 | 23017 | 20.00 | 0 | 59.49 |
| 4K | 1 | written-ahead/each-wt | 119.5 | 119.9 | 1.00 | 8361 | 8399 | 9110 | 8.01 | 0 | 106.5 |
| 4K | 6 | written-ahead/each-wt | 705.1 | 433.1 | 1.63 | 8502 | 9165 | 17523 | 8.00 | 0 | 42.77 |

### 3. A partial write-wt, round 1 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write through)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J-wt | 8893 | 31044 | 14900 | 33599 | 41.15 | 51.58 | 0 | 0 | 0 | 24619 | 51112 | 16.50 | 4.12 | 0 | 261.4 |
| 4K | 1 | J-ssd-wt | 936.3 | 3808 | 8285 | 28142 | 41.79 | 53.81 | 0 | 0 | 0 | 9259 | 29328 | 8.02 | 2.00 | 0 | 290.4 |
| 4K | 6 | J-wt | 25079 | 63560 | 32408 | 70507 | 41.43 | 87.01 | 0 | 0 | 0 | 59285 | 118097 | 16.16 | 4.04 | 0 | 205.5 |
| 4K | 6 | J-ssd-wt | 967.6 | 3873 | 24044 | 132719 | 35.15 | 86.48 | 0 | 0 | 0 | 25080 | 133692 | 8.02 | 2.00 | 0 | 221.5 |
| 16K | 1 | J-wt | 9132 | 32214 | 14828 | 30583 | 42.09 | 66.09 | 0 | 0 | 0 | 24632 | 49873 | 40.11 | 2.51 | 0 | 291.6 |
| 16K | 1 | J-ssd-wt | 1021 | 3833 | 8869 | 29408 | 41.90 | 63.59 | 0 | 0 | 0 | 9966 | 30439 | 20.02 | 1.25 | 0 | 293.9 |
| 16K | 6 | J-wt | 32136 | 70061 | 34021 | 84564 | 41.21 | 62.72 | 0 | 0 | 0 | 66078 | 129719 | 40.02 | 2.50 | 0 | 218.9 |
| 16K | 6 | J-ssd-wt | 1029 | 3845 | 22798 | 224516 | 34.63 | 73.31 | 0 | 0 | 0 | 24063 | 225526 | 20.17 | 1.26 | 0 | 247.4 |

