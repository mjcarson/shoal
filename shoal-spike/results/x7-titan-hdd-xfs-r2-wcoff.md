### Probes

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.04 µs |
| fdatasync of a clean file | p50 23.55 µs, p99 36.04 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8244 µs |
| rename, then the directory's Fsync | 4 KiB and 0 flushes a rename | p50 8260 µs |
| directory sync the measurements use | Fdatasync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.83 µs, p99 32.95 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 2 (write cache write through)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 6 | overwrite-wt | 338.2 | 16670 | 50019 | 1183441 | 0 | 4.01 | 0.972 |
| 4 | overwrite-wt | 113.6 | 33358 | 50048 | 50056 | 0 | 4.03 | 0.990 |
| 2 | overwrite-wt | 112.4 | 16694 | 33373 | 58307 | 0 | 4.01 | 0.990 |
| 1 | overwrite-wt | 119.8 | 8348 | 8391 | 8903 | 0 | 4.01 | 0.988 |

### 2. The journal-wt, round 2 (a 4 KiB header block on every record; write cache write through)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16K | 1 | written-ahead/each-wt | 118.8 | 119.1 | 1.00 | 8403 | 8435 | 9130 | 20.01 | 0 | 105.2 |
| 16K | 6 | written-ahead/each-wt | 10300 | 10285 | 1.00 | 412.2 | 9437 | 9488 | 20.00 | 0 | 58.42 |
| 4K | 1 | written-ahead/each-wt | 97.34 | 97.72 | 1.00 | 8363 | 25036 | 175091 | 8.69 | 0 | 107.3 |
| 4K | 6 | written-ahead/each-wt | 705.7 | 463.7 | 1.53 | 8501 | 9193 | 17465 | 8.00 | 0 | 43.34 |

### 3. A partial write-wt, round 2 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write through)

titan · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg titan hdd xfs

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J-ssd-wt | 918.8 | 1231 | 9118 | 30567 | 41.61 | 51.33 | 0 | 0 | 0 | 10063 | 31557 | 8.02 | 2.00 | 0 | 286.4 |
| 4K | 1 | J-wt | 9370 | 31009 | 15500 | 22114 | 40.77 | 70.97 | 0 | 0 | 0 | 25010 | 50041 | 16.26 | 4.06 | 0 | 268.0 |
| 4K | 6 | J-ssd-wt | 964.2 | 3811 | 24052 | 153214 | 35.40 | 87.56 | 0 | 0 | 0 | 25077 | 154109 | 8.02 | 2.00 | 0 | 221.4 |
| 4K | 6 | J-wt | 17574 | 44785 | 24647 | 57069 | 41.77 | 82.86 | 0 | 0 | 0 | 42094 | 78777 | 16.02 | 4.00 | 0 | 160.0 |
| 16K | 1 | J-ssd-wt | 1017 | 3813 | 8898 | 27809 | 41.48 | 53.60 | 0 | 0 | 0 | 9940 | 28791 | 20.13 | 1.26 | 0 | 288.8 |
| 16K | 1 | J-wt | 9128 | 31295 | 14970 | 31550 | 41.32 | 52.88 | 0 | 0 | 0 | 25007 | 50006 | 40.03 | 2.50 | 0 | 265.7 |
| 16K | 6 | J-ssd-wt | 1037 | 3985 | 24041 | 108313 | 41.35 | 79.74 | 0 | 0 | 0 | 25099 | 109440 | 20.18 | 1.26 | 0 | 224.6 |
| 16K | 6 | J-wt | 17505 | 42258 | 25221 | 60360 | 41.31 | 83.44 | 0 | 0 | 0 | 43239 | 88060 | 40.02 | 2.50 | 0 | 178.9 |

