### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.39 µs |
| fdatasync of a clean file | p50 23.86 µs, p99 36.32 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8243 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8261 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.13 µs, p99 32.93 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 2 (write cache write through)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 6 | overwrite-wt | 325.2 | 16660 | 41695 | 1208413 | 0 | 4.01 | 0.972 |
| 4 | overwrite-wt | 226.0 | 8358 | 50006 | 1150020 | 0 | 4.01 | 0.981 |
| 2 | overwrite-wt | 205.0 | 8350 | 25025 | 441658 | 0 | 5.67 | 0.980 |
| 1 | overwrite-wt | 118.8 | 8348 | 8388 | 33347 | 0 | 4.01 | 0.988 |

### 2. The journal-wt, round 2 (a 4 KiB header block on every record; write cache write through)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16K | 1 | written-ahead/each-wt | 117.5 | 117.8 | 1.00 | 8415 | 12873 | 17077 | 20.01 | 0 | 105.6 |
| 16K | 6 | written-ahead/each-wt | 1380 | 1383 | 1.00 | 516.2 | 9191 | 42135 | 20.03 | 0 | 67.28 |
| 4K | 1 | written-ahead/each-wt | 119.5 | 119.9 | 1.00 | 8365 | 8388 | 8441 | 8.01 | 0 | 101.2 |
| 4K | 6 | written-ahead/each-wt | 694.7 | 519.7 | 1.34 | 8523 | 16995 | 17240 | 8.00 | 0 | 45.21 |

### 3. A partial write-wt, round 2 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write through)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J-ssd-wt | 911.7 | 3791 | 7826 | 17008 | 41.68 | 59.83 | 0 | 0 | 0 | 8917 | 17898 | 8.29 | 2.07 | 0 | 284.6 |
| 4K | 1 | J-wt | 9942 | 13926 | 15660 | 22221 | 40.78 | 51.26 | 0 | 0 | 0 | 25033 | 41640 | 16.03 | 4.01 | 0 | 253.4 |
| 4K | 6 | J-ssd-wt | 969.2 | 1847 | 18041 | 234046 | 41.45 | 85.78 | 0 | 0 | 0 | 19121 | 235048 | 8.18 | 2.04 | 0 | 240.9 |
| 4K | 6 | J-wt | 17444 | 42159 | 22864 | 52101 | 40.96 | 85.88 | 0 | 0 | 0 | 41773 | 76298 | 16.02 | 4.00 | 0 | 167.5 |
| 16K | 1 | J-ssd-wt | 1031 | 3817 | 8297 | 17866 | 41.64 | 54.87 | 0 | 0 | 0 | 9428 | 18854 | 20.02 | 1.25 | 0 | 287.5 |
| 16K | 1 | J-wt | 9261 | 28410 | 15857 | 22431 | 41.32 | 65.43 | 0 | 0 | 0 | 25020 | 43868 | 40.12 | 2.51 | 0 | 271.5 |
| 16K | 6 | J-ssd-wt | 1048 | 3897 | 17447 | 199878 | 30.22 | 86.35 | 0 | 0 | 0 | 18635 | 200982 | 20.02 | 1.25 | 0 | 242.8 |
| 16K | 6 | J-wt | 15623 | 40362 | 24208 | 48605 | 40.77 | 84.39 | 0 | 0 | 0 | 41783 | 76201 | 40.02 | 2.50 | 0 | 188.6 |

