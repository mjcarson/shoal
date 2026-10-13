### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.39 µs |
| fdatasync of a clean file | p50 23.91 µs, p99 37.98 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8241 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8257 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 26.18 µs, p99 33.34 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 4 (write cache write through)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 6 | overwrite-wt | 232.8 | 25016 | 50044 | 575831 | 0 | 4.02 | 0.972 |
| 4 | overwrite-wt | 110.8 | 33360 | 50123 | 99996 | 0 | 4.41 | 0.988 |
| 2 | overwrite-wt | 112.2 | 16694 | 33373 | 267496 | 0 | 4.01 | 0.990 |
| 1 | overwrite-wt | 119.8 | 8348 | 8384 | 9016 | 0 | 4.01 | 0.988 |

### 2. The journal-wt, round 4 (a 4 KiB header block on every record; write cache write through)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16K | 1 | written-ahead/each-wt | 118.3 | 118.6 | 1.00 | 8403 | 9083 | 17459 | 20.01 | 0 | 109.2 |
| 16K | 6 | written-ahead/each-wt | 10274 | 10270 | 1.00 | 412.5 | 9442 | 9490 | 20.00 | 0 | 58.86 |
| 4K | 1 | written-ahead/each-wt | 112.3 | 112.7 | 1.00 | 8362 | 25030 | 25066 | 8.38 | 0 | 102.9 |
| 4K | 6 | written-ahead/each-wt | 706.4 | 458.0 | 1.55 | 8501 | 9186 | 17576 | 8.00 | 0 | 43.47 |

### 3. A partial write-wt, round 4 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write through)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J-ssd-wt | 914.3 | 3794 | 8005 | 33699 | 41.78 | 52.78 | 0 | 0 | 0 | 8908 | 34675 | 8.02 | 2.00 | 0 | 287.9 |
| 4K | 1 | J-wt | 9013 | 31649 | 14382 | 21907 | 40.83 | 56.04 | 0 | 0 | 0 | 24745 | 49882 | 16.20 | 4.05 | 0 | 257.9 |
| 4K | 6 | J-ssd-wt | 944.2 | 3824 | 19617 | 116138 | 32.16 | 88.84 | 0 | 0 | 0 | 20538 | 117369 | 8.02 | 2.00 | 0 | 265.4 |
| 4K | 6 | J-wt | 28566 | 75474 | 33885 | 88257 | 31.90 | 67.60 | 0 | 0 | 0 | 66927 | 134858 | 16.02 | 4.00 | 0 | 194.7 |
| 16K | 1 | J-ssd-wt | 1020 | 3831 | 7924 | 32849 | 41.70 | 54.87 | 0 | 0 | 0 | 8910 | 33860 | 20.13 | 1.26 | 0 | 290.4 |
| 16K | 1 | J-wt | 9352 | 30269 | 13796 | 26624 | 41.49 | 51.63 | 0 | 0 | 0 | 24672 | 43574 | 40.02 | 2.50 | 0 | 274.8 |
| 16K | 6 | J-ssd-wt | 1023 | 3869 | 19233 | 117294 | 41.38 | 71.78 | 0 | 0 | 0 | 20701 | 118269 | 20.18 | 1.26 | 0 | 264.2 |
| 16K | 6 | J-wt | 27861 | 77174 | 37493 | 88326 | 41.09 | 69.60 | 0 | 0 | 0 | 67501 | 145626 | 40.02 | 2.50 | 0 | 216.6 |

