### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.52 µs |
| fdatasync of a clean file | p50 23.93 µs, p99 45.60 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8243 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8261 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.10 µs, p99 32.97 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 4 (write cache write through)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 6 | overwrite-wt | 369.4 | 8368 | 41695 | 775047 | 0 | 4.01 | 0.973 |
| 4 | overwrite-wt | 227.6 | 8357 | 50005 | 1175047 | 0 | 4.01 | 0.982 |
| 2 | overwrite-wt | 227.2 | 8349 | 25019 | 41689 | 0 | 4.19 | 0.979 |
| 1 | overwrite-wt | 119.6 | 8349 | 8385 | 9117 | 0 | 4.01 | 0.989 |

### 2. The journal-wt, round 4 (a 4 KiB header block on every record; write cache write through)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16K | 1 | written-ahead/each-wt | 114.0 | 114.3 | 1.00 | 8414 | 17611 | 53132 | 20.32 | 0 | 101.4 |
| 16K | 6 | written-ahead/each-wt | 9471 | 9468 | 1.00 | 472.2 | 9164 | 9221 | 20.00 | 0 | 57.12 |
| 4K | 1 | written-ahead/each-wt | 119.5 | 119.9 | 1.00 | 8365 | 8396 | 9370 | 8.01 | 0 | 100.5 |
| 4K | 6 | written-ahead/each-wt | 689.2 | 553.0 | 1.25 | 8523 | 17235 | 18006 | 8.00 | 0 | 45.53 |

### 3. A partial write-wt, round 4 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write through)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J-ssd-wt | 945.2 | 3803 | 7594 | 17145 | 42.12 | 66.64 | 0 | 0 | 0 | 8626 | 18291 | 8.29 | 2.07 | 0 | 298.4 |
| 4K | 1 | J-wt | 9465 | 13954 | 13854 | 22230 | 40.98 | 53.55 | 0 | 0 | 0 | 25009 | 41541 | 16.02 | 4.01 | 0 | 256.7 |
| 4K | 6 | J-ssd-wt | 971.3 | 2699 | 22095 | 207931 | 41.79 | 93.59 | 0 | 0 | 0 | 23279 | 208905 | 8.19 | 2.05 | 0 | 238.9 |
| 4K | 6 | J-wt | 17317 | 39154 | 22680 | 48016 | 39.73 | 82.28 | 0 | 0 | 0 | 41767 | 75173 | 16.02 | 4.00 | 0 | 166.5 |
| 16K | 1 | J-ssd-wt | 1034 | 3849 | 8386 | 17864 | 41.92 | 58.54 | 0 | 0 | 0 | 9491 | 18953 | 20.02 | 1.25 | 0 | 288.4 |
| 16K | 1 | J-wt | 8921 | 30613 | 15565 | 33440 | 41.67 | 55.15 | 0 | 0 | 0 | 25013 | 56785 | 40.12 | 2.51 | 0 | 264.5 |
| 16K | 6 | J-ssd-wt | 1058 | 4277 | 21039 | 157288 | 32.95 | 89.02 | 0 | 0 | 0 | 23061 | 158250 | 20.02 | 1.25 | 0 | 239.1 |
| 16K | 6 | J-wt | 16511 | 38408 | 24245 | 52534 | 41.12 | 89.01 | 0 | 0 | 0 | 41797 | 79987 | 40.02 | 2.50 | 0 | 190.6 |

