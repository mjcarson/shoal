### Probes

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| probe | result | detail |
| --- | --- | --- |
| direct I/O reaches the device unsynced | true | 1024 KiB written for a 1024 KiB write |
| flushes for 100 overwrites, each synced | disk 0 · fs device 0 | 4.00 KiB a sync · sync p50 28.91 µs |
| fdatasync of a clean file | p50 24.07 µs, p99 38.41 µs | 0 flushes and 0 KiB for 100 |
| rename, then the directory's Fdatasync | 4 KiB and 0 flushes a rename | p50 8244 µs |
| rename, then the directory's Fsync | 39 KiB and 0 flushes a rename | p50 8262 µs |
| directory sync the measurements use | Fsync | the fdatasync unless it wrote less than half of what the fsync did |
| FICLONERANGE of one block | supported | FIEMAP on the target: Ok(Extents { total: 2, shared: 1, unwritten: 0 }) |
| an empty spawn_blocking | p50 25.97 µs, p99 32.57 µs | the hop every rename, unlink, mkdir and clone pays |
| alignment | 4096 B | glommio's file alignment 512 B |

SSD beside it: hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · nvme0n1 Samsung SSD 970 EVO 500GB (solid state, fw 1B2QEXE7, cache write back, 512/512 B blocks) · xfs at /xfs on /dev/mapper/ubuntu--vg-xfs = dm-1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B

### X7 · A 4 KiB overwrite and its fdatasync, each writer its own file, round 1 (write cache write through)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| writers | side | syncs/s | p50 µs | p99 µs | max µs | flushes/sync | dev KiB/sync | disk busy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | overwrite-wt | 119.6 | 8348 | 8378 | 8792 | 0 | 4.01 | 0.988 |
| 2 | overwrite-wt | 143.6 | 16676 | 33345 | 200469 | 0 | 4.01 | 0.987 |
| 4 | overwrite-wt | 261.8 | 8352 | 50004 | 875059 | 0 | 4.01 | 0.978 |
| 6 | overwrite-wt | 206.8 | 25003 | 66693 | 1674992 | 0 | 4.05 | 0.981 |

### 2. The journal-wt, round 1 (a 4 KiB header block on every record; write cache write through)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| record | stagers | file/writer | records/s | syncs/s | records/sync | p50 µs | p99 µs | p99.9 µs | dev KiB/record | flushes/record | cpu µs/record |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 16K | 1 | written-ahead/each-wt | 117.2 | 117.5 | 1.00 | 8415 | 8767 | 33391 | 20.01 | 0 | 101.3 |
| 16K | 6 | written-ahead/each-wt | 9793 | 9795 | 1.00 | 471.8 | 9170 | 9215 | 20.00 | 0 | 56.63 |
| 4K | 1 | written-ahead/each-wt | 119.4 | 119.8 | 1.00 | 8365 | 8386 | 8716 | 8.01 | 0 | 100.6 |
| 4K | 6 | written-ahead/each-wt | 699.4 | 522.2 | 1.34 | 8522 | 8953 | 17246 | 8.00 | 0 | 45.08 |

### 3. A partial write-wt, round 1 (latencies µs; whole units, no read-modify-write; the clone sides are not run on a disk: X6 rejected the clone; J-ssd journals on the host's SSD; write cache write through)

hyperion · AMD Ryzen Embedded V1756B with Radeon Vega Gfx · governor performance · kernel 7.0.0-34-generic · sda WDC WD140EDFZ-11A0VA0 (5400 rpm, fw 0A81, cache write through, 512/4096 B blocks, mq-deadline, fua no, 64 queued, NCQ 32) · xfs at /hdd on /dev/sda1 = sda1 (rw,relatime,rw,inode64,logbufs=8,logbsize=32k,noquota) · fs block 4096 B · leg hyperion hdd xfs

| size | in flight | side | stage p50 | stage p99 | apply p50 | apply p99 | apply's fdatasync p50 | p99 | clone p50 | clone wait p50 | release p50 | total p50 | total p99 | dev KiB/write | ÷ payload | flushes/write | cpu µs/write |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4K | 1 | J-wt | 8931 | 16295 | 16313 | 21920 | 41.23 | 51.88 | 0 | 0 | 0 | 25019 | 41658 | 16.37 | 4.09 | 0 | 255.0 |
| 4K | 1 | J-ssd-wt | 954.7 | 1274 | 7800 | 17490 | 41.82 | 58.63 | 0 | 0 | 0 | 8776 | 18629 | 8.02 | 2.00 | 0 | 283.2 |
| 4K | 6 | J-wt | 16368 | 40725 | 23983 | 54787 | 41.23 | 82.56 | 0 | 0 | 0 | 41751 | 75982 | 16.02 | 4.00 | 0 | 166.5 |
| 4K | 6 | J-ssd-wt | 969.9 | 3852 | 21378 | 173918 | 41.66 | 101.7 | 0 | 0 | 0 | 23157 | 175184 | 8.18 | 2.04 | 0 | 226.6 |
| 16K | 1 | J-wt | 9385 | 21158 | 15434 | 22021 | 41.69 | 52.11 | 0 | 0 | 0 | 25012 | 42990 | 40.03 | 2.50 | 0 | 261.0 |
| 16K | 1 | J-ssd-wt | 1042 | 3936 | 8419 | 22666 | 42.13 | 68.82 | 0 | 0 | 0 | 9586 | 23698 | 20.11 | 1.26 | 0 | 307.9 |
| 16K | 6 | J-wt | 18604 | 39257 | 23032 | 49342 | 39.81 | 84.70 | 0 | 0 | 0 | 41791 | 75252 | 40.02 | 2.50 | 0 | 191.5 |
| 16K | 6 | J-ssd-wt | 1062 | 4241 | 20634 | 179227 | 32.12 | 104.4 | 0 | 0 | 0 | 21714 | 180346 | 20.02 | 1.25 | 0 | 236.9 |

