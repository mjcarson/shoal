### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 29.01 µs |
| fdatasync of a clean file | p50 24.43 µs, p99 38.15 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8243 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8256 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.21 µs, p99 33.43 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 3 (write cache write through)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | overwrite-wt | 102.0 | 8361 | 16659 | 541585 | 0 | 6.93 | 0.988 |
| 2 | overwrite-wt | 112.6 | 16691 | 33377 | 34967 | 0 | 4.01 | 0.989 |
| 4 | overwrite-wt | 112.8 | 33340 | 50069 | 784191 | 0 | 4.03 | 0.989 |
| 6 | overwrite-wt | 140.6 | 16715 | 74978 | 1533392 | 0 | 4.07 | 0.982 |

### 2. The journal-wt, round 3 (a 4 KiB header block on every record; write cache write through)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16K | 1 | written-ahead/each-wt | 119.0 | 119.4 | 1.00 | 8404 | 8456 | 10341 | 20.01 | 0 | 106.2 |
| 16K | 6 | written-ahead/each-wt | 10377 | 10371 | 1.00 | 413.1 | 9448 | 9508 | 20.00 | 0 | 59.50 |
| 4K | 1 | written-ahead/each-wt | 119.7 | 120.1 | 1.00 | 8362 | 8388 | 9112 | 8.01 | 0 | 107.6 |
| 4K | 6 | written-ahead/each-wt | 706.4 | 482.4 | 1.47 | 8501 | 9216 | 17494 | 8.00 | 0 | 44.17 |

### 3. A partial write-wt, round 3 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write through)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J-wt | 9087 | 31672 | 14736 | 22264 | 41.24 | 64.21 | 0 | 0 | 0 | 24620 | 49731 | 16.47 | 4.12 | 0 | 267.1 |
| 4K | 1 | J-ssd-wt | 937.2 | 1175 | 7997 | 26425 | 41.82 | 52.17 | 0 | 0 | 0 | 8907 | 27435 | 8.02 | 2.00 | 0 | 285.8 |
| 4K | 6 | J-wt | 25317 | 69518 | 31962 | 75343 | 30.63 | 78.05 | 0 | 0 | 0 | 59006 | 121016 | 16.02 | 4.00 | 0 | 190.4 |
| 4K | 6 | J-ssd-wt | 916.9 | 3794 | 22202 | 142352 | 42.17 | 71.13 | 0 | 0 | 0 | 23147 | 143340 | 8.18 | 2.04 | 0 | 254.4 |
| 16K | 1 | J-wt | 9460 | 31683 | 14053 | 31452 | 41.73 | 55.57 | 0 | 0 | 0 | 24640 | 50018 | 40.03 | 2.50 | 0 | 264.7 |
| 16K | 1 | J-ssd-wt | 1024 | 3832 | 7920 | 29325 | 41.93 | 54.28 | 0 | 0 | 0 | 8911 | 30382 | 20.13 | 1.26 | 0 | 289.5 |
| 16K | 6 | J-wt | 26684 | 75972 | 35747 | 80619 | 29.86 | 67.86 | 0 | 0 | 0 | 63770 | 130456 | 40.02 | 2.50 | 0 | 215.0 |
| 16K | 6 | J-ssd-wt | 1029 | 5782 | 23024 | 166127 | 30.48 | 68.93 | 0 | 0 | 0 | 24058 | 167131 | 20.02 | 1.25 | 0 | 251.2 |

